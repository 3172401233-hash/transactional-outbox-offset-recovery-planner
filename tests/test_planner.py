import unittest

from outbox_recovery import (
    Action,
    BrokerReceipt,
    ConsumptionState,
    MessageFacts,
    OutboxStatus,
    RecoveryContext,
    ReplayEntry,
    ReplayKind,
    plan_message,
    plan_recovery,
)

CTX = RecoveryContext(dedup_window=10)


def facts(**overrides):
    base = dict(
        message_id="tx-1:order-7",
        tx_committed=True,
        outbox_status=OutboxStatus.SENT,
        producer_epoch=4,
        expected_offset=40,
        broker_receipt=None,
        consumer_offset=38,
        replay_log=(),
    )
    base.update(overrides)
    return MessageFacts(**base)


ROLLBACK_LOG = (
    ReplayEntry(ReplayKind.OFFSET_COMMIT, offset=100),
    ReplayEntry(ReplayKind.OFFSET_ROLLBACK, offset=30),
)


class AckLostTests(unittest.TestCase):
    def test_ack_lost_within_window_republishes(self):
        decision = plan_message(facts(), CTX)
        self.assertEqual(decision.action, Action.REPUBLISH)
        self.assertEqual(decision.consumption, ConsumptionState.UNKNOWN)
        self.assertIn("consumption", decision.undecidable)

    def test_ack_lost_window_expired_goes_manual(self):
        # Offset proof is voided by the rollback, and the key has aged
        # out of the dedup window: neither republish nor skip is safe.
        decision = plan_message(
            facts(consumer_offset=100, replay_log=ROLLBACK_LOG), CTX
        )
        self.assertEqual(decision.action, Action.MANUAL_REVIEW)
        self.assertEqual(decision.consumption, ConsumptionState.UNKNOWN)

    def test_ack_lost_unknown_consumer_offset_goes_manual(self):
        decision = plan_message(facts(consumer_offset=None), CTX)
        self.assertEqual(decision.action, Action.MANUAL_REVIEW)
        self.assertIn("dedup_window", decision.undecidable)

    def test_sent_status_alone_never_skips(self):
        # SENT outbox row, no receipt, no offset proof: must not SKIP.
        decision = plan_message(
            facts(consumer_offset=None, expected_offset=None), CTX
        )
        self.assertNotEqual(decision.action, Action.SKIP)


class DuplicateDeliveryTests(unittest.TestCase):
    def test_receipt_with_duplicate_deliveries_waits(self):
        log = (
            ReplayEntry(ReplayKind.PUBLISH, message_id="tx-1:order-7"),
            ReplayEntry(ReplayKind.DELIVER, message_id="tx-1:order-7"),
            ReplayEntry(ReplayKind.DELIVER, message_id="tx-1:order-7"),
        )
        decision = plan_message(
            facts(
                broker_receipt=BrokerReceipt("tx-1:order-7", 40, 4),
                replay_log=log,
            ),
            CTX,
        )
        self.assertEqual(decision.action, Action.WAIT)
        self.assertEqual(decision.consumption, ConsumptionState.UNKNOWN)

    def test_missing_receipt_alone_does_not_republish_without_dedup(self):
        # No receipt, window undecidable -> manual, not a blind republish.
        decision = plan_message(facts(consumer_offset=None), CTX)
        self.assertEqual(decision.action, Action.MANUAL_REVIEW)


class ConsumerRollbackTests(unittest.TestCase):
    def test_rollback_unproves_consumption(self):
        log = (
            ReplayEntry(ReplayKind.OFFSET_COMMIT, offset=50),
            ReplayEntry(ReplayKind.OFFSET_ROLLBACK, offset=30),
        )
        decision = plan_message(
            facts(
                broker_receipt=BrokerReceipt("tx-1:order-7", 40, 4),
                consumer_offset=50,
                replay_log=log,
            ),
            CTX,
        )
        self.assertEqual(decision.action, Action.WAIT)
        self.assertNotEqual(decision.consumption, ConsumptionState.PROVEN_CONSUMED)

    def test_non_monotone_commits_detected_without_explicit_rollback(self):
        log = (
            ReplayEntry(ReplayKind.OFFSET_COMMIT, offset=50),
            ReplayEntry(ReplayKind.OFFSET_COMMIT, offset=30),
        )
        decision = plan_message(
            facts(
                broker_receipt=BrokerReceipt("tx-1:order-7", 40, 4),
                consumer_offset=50,
                replay_log=log,
            ),
            CTX,
        )
        self.assertEqual(decision.action, Action.WAIT)

    def test_monotone_offset_past_message_skips(self):
        log = (
            ReplayEntry(ReplayKind.OFFSET_COMMIT, offset=41),
            ReplayEntry(ReplayKind.OFFSET_COMMIT, offset=50),
        )
        decision = plan_message(
            facts(
                broker_receipt=BrokerReceipt("tx-1:order-7", 40, 4),
                consumer_offset=50,
                replay_log=log,
            ),
            CTX,
        )
        self.assertEqual(decision.action, Action.SKIP)
        self.assertEqual(decision.consumption, ConsumptionState.PROVEN_CONSUMED)


class ProducerRestartTests(unittest.TestCase):
    def test_stale_epoch_receipt_still_waits(self):
        # Producer restarted (epoch 4 -> 5); the old receipt remains valid.
        decision = plan_message(
            facts(
                producer_epoch=5,
                broker_receipt=BrokerReceipt("tx-1:order-7", 40, 4),
            ),
            CTX,
        )
        self.assertEqual(decision.action, Action.WAIT)

    def test_newer_epoch_receipt_fences_zombie(self):
        decision = plan_message(
            facts(
                producer_epoch=4,
                broker_receipt=BrokerReceipt("tx-1:order-7", 40, 5),
            ),
            CTX,
        )
        self.assertEqual(decision.action, Action.MANUAL_REVIEW)

    def test_pending_after_restart_republishes(self):
        decision = plan_message(
            facts(producer_epoch=5, outbox_status=OutboxStatus.PENDING), CTX
        )
        self.assertEqual(decision.action, Action.REPUBLISH)
        self.assertEqual(decision.consumption, ConsumptionState.PROVEN_NOT_CONSUMED)


class WindowExpiryTests(unittest.TestCase):
    def test_boundary_offset_is_inside_window(self):
        # consumer_offset - offset == window is still covered.
        decision = plan_message(
            facts(consumer_offset=50, replay_log=ROLLBACK_LOG), CTX
        )
        self.assertEqual(decision.action, Action.REPUBLISH)

    def test_one_past_boundary_expires(self):
        decision = plan_message(
            facts(consumer_offset=51, replay_log=ROLLBACK_LOG), CTX
        )
        self.assertEqual(decision.action, Action.MANUAL_REVIEW)


class ConsistencyTests(unittest.TestCase):
    def test_uncommitted_tx_with_outbox_row_is_manual(self):
        decision = plan_message(facts(tx_committed=False), CTX)
        self.assertEqual(decision.action, Action.MANUAL_REVIEW)

    def test_failed_send_republishes(self):
        decision = plan_message(
            facts(outbox_status=OutboxStatus.FAILED), CTX
        )
        self.assertEqual(decision.action, Action.REPUBLISH)
        self.assertEqual(decision.consumption, ConsumptionState.PROVEN_NOT_CONSUMED)

    def test_plan_is_deterministic(self):
        batch = [
            facts(),
            facts(
                message_id="tx-2:order-9",
                consumer_offset=100,
                replay_log=ROLLBACK_LOG,
            ),
            facts(
                message_id="tx-3:order-2",
                broker_receipt=BrokerReceipt("tx-3:order-2", 40, 4),
            ),
        ]
        first = plan_recovery(batch, CTX)
        second = plan_recovery(batch, CTX)
        self.assertEqual(first, second)
        self.assertEqual(
            [d.action for d in first.decisions],
            [Action.REPUBLISH, Action.MANUAL_REVIEW, Action.WAIT],
        )
        self.assertEqual(
            {d.message_id for d in first.undecidable},
            {"tx-1:order-7", "tx-2:order-9", "tx-3:order-2"},
        )


if __name__ == "__main__":
    unittest.main()
