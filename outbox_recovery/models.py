"""Data model for the outbox recovery planner.

Everything is immutable and hashable so that a plan is a pure function of
its inputs: the same facts always yield the same decision.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Tuple


class OutboxStatus(Enum):
    """State of the outbox row as recorded in the business database."""

    PENDING = "PENDING"  # row written, send never attempted
    SENT = "SENT"  # send attempted, broker ack lost or never arrived
    FAILED = "FAILED"  # local send attempt failed before hitting the broker


class Action(Enum):
    """Deterministic recovery action for one message."""

    REPUBLISH = "REPUBLISH"  # safe to re-send with the same idempotency key
    WAIT = "WAIT"  # evidence is consistent; let normal flow catch up
    SKIP = "SKIP"  # provably consumed; mark outbox row acked, do not resend
    MANUAL_REVIEW = "MANUAL_REVIEW"  # automatable proof is impossible


class ConsumptionState(Enum):
    """What can be *proven* about consumer-side processing."""

    PROVEN_CONSUMED = "PROVEN_CONSUMED"
    PROVEN_NOT_CONSUMED = "PROVEN_NOT_CONSUMED"
    UNKNOWN = "UNKNOWN"


class ReplayKind(Enum):
    PUBLISH = "PUBLISH"  # a publish attempt left the producer
    DELIVER = "DELIVER"  # the consumer saw the message (pre-commit)
    OFFSET_COMMIT = "OFFSET_COMMIT"  # consumer committed an offset
    OFFSET_ROLLBACK = "OFFSET_ROLLBACK"  # consumer offset moved backwards


@dataclass(frozen=True)
class BrokerReceipt:
    """A broker-side record that the message was accepted."""

    message_id: str
    offset: int
    producer_epoch: int


@dataclass(frozen=True)
class ReplayEntry:
    """One entry of the recovery replay log (producer + consumer side)."""

    kind: ReplayKind
    message_id: Optional[str] = None
    offset: Optional[int] = None
    producer_epoch: Optional[int] = None


@dataclass(frozen=True)
class MessageFacts:
    """All observable facts about one outbox message."""

    message_id: str
    tx_committed: bool
    outbox_status: OutboxStatus
    producer_epoch: int = 0
    expected_offset: Optional[int] = None  # offset the message would/did get
    broker_receipt: Optional[BrokerReceipt] = None
    consumer_offset: Optional[int] = None  # last committed consumer offset
    replay_log: Tuple[ReplayEntry, ...] = ()


@dataclass(frozen=True)
class RecoveryContext:
    """Cluster-wide recovery parameters."""

    # The consumer guarantees idempotency-key deduplication for messages
    # whose offset is at most ``dedup_window`` behind its committed offset.
    dedup_window: int


@dataclass(frozen=True)
class Decision:
    """The deterministic verdict for one message."""

    message_id: str
    action: Action
    consumption: ConsumptionState
    reason: str
    undecidable: Tuple[str, ...] = ()


@dataclass(frozen=True)
class RecoveryPlan:
    decisions: Tuple[Decision, ...] = field(default_factory=tuple)

    @property
    def undecidable(self) -> Tuple[Decision, ...]:
        return tuple(d for d in self.decisions if d.undecidable)

    def for_message(self, message_id: str) -> Decision:
        for decision in self.decisions:
            if decision.message_id == message_id:
                return decision
        raise KeyError(message_id)
