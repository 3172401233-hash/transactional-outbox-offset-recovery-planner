"""Deterministic recovery planning for the transactional outbox.

Two invariants drive every rule below:

1. An outbox row marked SENT is *not* proof of consumption, so it never
   justifies advancing the consumer offset or skipping a message.
2. A missing broker receipt is *not* proof of non-delivery, so a message
   is only republished when the consumer-side dedup window still covers
   its idempotency key.

Rules are evaluated in a fixed order, so the same facts always produce
the same :class:`Decision`.
"""

from __future__ import annotations

from typing import Iterable, Optional, Tuple

from .models import (
    Action,
    ConsumptionState,
    Decision,
    MessageFacts,
    OutboxStatus,
    RecoveryContext,
    RecoveryPlan,
    ReplayKind,
)

_UNKNOWN_CONSUMPTION = ("consumption",)
_UNKNOWN_CONSUMPTION_AND_DEDUP = ("consumption", "dedup_window")


def _known_offset(facts: MessageFacts) -> Optional[int]:
    if facts.broker_receipt is not None:
        return facts.broker_receipt.offset
    return facts.expected_offset


def _consumer_rolled_back(facts: MessageFacts) -> bool:
    """A rollback entry, or a commit lower than an earlier commit."""
    commits = [
        entry.offset
        for entry in facts.replay_log
        if entry.kind == ReplayKind.OFFSET_COMMIT and entry.offset is not None
    ]
    if any(entry.kind == ReplayKind.OFFSET_ROLLBACK for entry in facts.replay_log):
        return True
    return any(later < earlier for earlier, later in zip(commits, commits[1:]))


def _proven_consumed(facts: MessageFacts) -> bool:
    """Offset proof only counts when the commit log is monotone."""
    offset = _known_offset(facts)
    if facts.consumer_offset is None or offset is None:
        return False
    if _consumer_rolled_back(facts):
        return False
    return facts.consumer_offset > offset


def _within_dedup_window(facts: MessageFacts, ctx: RecoveryContext) -> Optional[bool]:
    """True/False when decidable, None when the inputs cannot decide it."""
    offset = _known_offset(facts)
    if facts.consumer_offset is None or offset is None:
        return None
    return facts.consumer_offset - offset <= ctx.dedup_window


def plan_message(facts: MessageFacts, ctx: RecoveryContext) -> Decision:
    """Decide the recovery action for a single message."""
    message_id = facts.message_id
    receipt = facts.broker_receipt

    # Rule 1: no committed business transaction means the message must
    # never exist downstream. An outbox row in this state is corruption.
    if not facts.tx_committed:
        return Decision(
            message_id,
            Action.MANUAL_REVIEW,
            ConsumptionState.UNKNOWN,
            "outbox row exists but the business transaction never committed",
            _UNKNOWN_CONSUMPTION,
        )

    # Rule 2: epoch fencing. A receipt from a *newer* epoch means this
    # planner is a zombie producer and must not touch the topic.
    if receipt is not None and receipt.producer_epoch > facts.producer_epoch:
        return Decision(
            message_id,
            Action.MANUAL_REVIEW,
            ConsumptionState.UNKNOWN,
            "broker receipt carries newer producer epoch ({}>{}); "
            "this producer is fenced".format(
                receipt.producer_epoch, facts.producer_epoch
            ),
            _UNKNOWN_CONSUMPTION,
        )

    # Rule 3: provable consumption. Requires a monotone offset log; a
    # rolled-back commit proves nothing.
    if _proven_consumed(facts):
        return Decision(
            message_id,
            Action.SKIP,
            ConsumptionState.PROVEN_CONSUMED,
            "consumer offset {} is past message offset {}; mark outbox "
            "row acked".format(facts.consumer_offset, _known_offset(facts)),
        )

    # Rule 4: the broker has the message (receipt from our epoch or an
    # older one, e.g. after a producer restart). Republishing would only
    # add a duplicate the consumer must dedup; wait for delivery instead.
    if receipt is not None:
        rolled_back = _consumer_rolled_back(facts)
        reason = "broker receipt exists at offset {}".format(receipt.offset)
        if rolled_back:
            reason += "; consumer offset rolled back, re-delivery expected"
        return Decision(
            message_id,
            Action.WAIT,
            ConsumptionState.UNKNOWN,
            reason,
            _UNKNOWN_CONSUMPTION,
        )

    # Rule 5: never sent (or failed locally) and no receipt. Nothing
    # reached the broker, so republish cannot duplicate.
    if facts.outbox_status in (OutboxStatus.PENDING, OutboxStatus.FAILED):
        return Decision(
            message_id,
            Action.REPUBLISH,
            ConsumptionState.PROVEN_NOT_CONSUMED,
            "outbox status {} with no broker receipt: send was never "
            "accepted".format(facts.outbox_status.value),
        )

    # Rule 6: SENT but no receipt -- the ambiguous case. Republish only
    # when the idempotency key is still inside the consumer dedup window.
    within_window = _within_dedup_window(facts, ctx)
    if within_window is True:
        return Decision(
            message_id,
            Action.REPUBLISH,
            ConsumptionState.UNKNOWN,
            "ack lost but idempotency key {} is inside the dedup window; "
            "republish is deduplicated".format(message_id),
            _UNKNOWN_CONSUMPTION,
        )
    if within_window is False:
        return Decision(
            message_id,
            Action.MANUAL_REVIEW,
            ConsumptionState.UNKNOWN,
            "ack lost and dedup window expired; republish could duplicate",
            _UNKNOWN_CONSUMPTION,
        )
    return Decision(
        message_id,
        Action.MANUAL_REVIEW,
        ConsumptionState.UNKNOWN,
        "ack lost and dedup coverage undecidable (missing consumer offset "
        "or message offset)",
        _UNKNOWN_CONSUMPTION_AND_DEDUP,
    )


def plan_recovery(
    messages: Iterable[MessageFacts], ctx: RecoveryContext
) -> RecoveryPlan:
    """Plan every message; input order is preserved, verdicts are pure."""
    return RecoveryPlan(tuple(plan_message(facts, ctx) for facts in messages))
