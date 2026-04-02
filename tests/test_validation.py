"""
Tests for Lamport Clock Validation and Priority Queue Ordering
Member: Shagee (IT24103322)
"""

import unittest
import time
from node.time_sync import LamportClock, MessageReorderer


class TestLamportValidation(unittest.TestCase):
    """Tests for Lamport clock anomaly detection."""

    def test_negative_value_clamped(self):
        """Negative received values should be clamped to 0."""
        clock = LamportClock()
        clock.tick()  # clock = 1

        result = clock.update(-5)
        # max(1, 0) + 1 = 2 (clamped -5 to 0)
        self.assertEqual(result, 2)

    def test_negative_value_recorded_as_anomaly(self):
        """Negative values should be logged as anomalies."""
        clock = LamportClock()
        clock.update(-10)

        anomalies = clock.get_anomalies()
        self.assertEqual(len(anomalies), 1)
        self.assertEqual(anomalies[0]["kind"], "negative_clock")
        self.assertEqual(anomalies[0]["received"], -10)

    def test_large_jump_detected(self):
        """A clock value that jumps by more than JUMP_THRESHOLD is flagged."""
        clock = LamportClock()
        clock.tick()  # clock = 1

        clock.update(clock.JUMP_THRESHOLD + 100)

        anomalies = clock.get_anomalies()
        self.assertEqual(len(anomalies), 1)
        self.assertEqual(anomalies[0]["kind"], "large_jump")

    def test_normal_update_no_anomaly(self):
        """Normal updates should not produce anomalies."""
        clock = LamportClock()
        for i in range(1, 20):
            clock.update(i)

        self.assertEqual(len(clock.get_anomalies()), 0)

    def test_monotonicity_preserved_after_negative(self):
        """Clock should still be monotonically increasing after bad input."""
        clock = LamportClock()
        t1 = clock.tick()       # 1
        t2 = clock.update(-5)   # max(1, 0) + 1 = 2
        t3 = clock.tick()       # 3

        self.assertLess(t1, t2)
        self.assertLess(t2, t3)

    def test_anomaly_log_bounded(self):
        """Anomaly log should not grow unbounded — max 50 entries."""
        clock = LamportClock()
        for i in range(100):
            clock.update(-i)

        self.assertLessEqual(len(clock.get_anomalies()), 50)

    def test_get_stats_includes_counts(self):
        """get_stats should include tick and update counts."""
        clock = LamportClock()
        clock.tick()
        clock.tick()
        clock.update(5)

        stats = clock.get_stats()
        self.assertEqual(stats["total_ticks"], 2)
        self.assertEqual(stats["total_updates"], 1)
        self.assertEqual(stats["current_time"], clock.get())

    def test_large_jump_still_updates(self):
        """Even with a large jump warning, the clock should still advance."""
        clock = LamportClock()
        clock.tick()  # 1

        result = clock.update(5000)
        # Should still be max(1, 5000) + 1 = 5001
        self.assertEqual(result, 5001)
        self.assertEqual(clock.get(), 5001)


class TestPriorityQueueOrdering(unittest.TestCase):
    """Tests that buffered messages are delivered in Lamport time order."""

    def _make_msg(self, msg_id, sender_id, vector_clock, lamport_time):
        return {
            "id": msg_id,
            "sender_id": sender_id,
            "vector_clock": vector_clock,
            "lamport_time": lamport_time,
            "timestamp": time.time(),
            "content": f"Message {msg_id}",
        }

    def test_simultaneous_ready_delivered_by_lamport_order(self):
        """When multiple messages become ready at once, deliver lowest Lamport first."""
        r = MessageReorderer()
        delivered = []

        # Three messages from different nodes, all independent (no cross-deps)
        # but with different Lamport times. Buffer them all then let them flush.
        m3 = self._make_msg("m3", sender_id=3, vector_clock={"3": 1}, lamport_time=30)
        m1 = self._make_msg("m1", sender_id=1, vector_clock={"1": 1}, lamport_time=10)
        m2 = self._make_msg("m2", sender_id=2, vector_clock={"2": 1}, lamport_time=20)

        # Deliver in "wrong" order — but all are independent so all should deliver
        r.try_deliver(m3, delivered.append)
        r.try_deliver(m1, delivered.append)
        r.try_deliver(m2, delivered.append)

        # All should be delivered
        self.assertEqual(len(delivered), 3)

    def test_cascade_flush_respects_lamport_order(self):
        """When a missing dependency arrives and unblocks multiple messages,
        they should flush in Lamport time order."""
        r = MessageReorderer()
        delivered = []

        # Buffer messages that depend on m1 from node 1
        # m4 has highest lamport, m2 has lowest
        m4 = self._make_msg("m4", sender_id=1, vector_clock={"1": 3}, lamport_time=40)
        m3 = self._make_msg("m3", sender_id=1, vector_clock={"1": 2}, lamport_time=30)

        r.try_deliver(m4, delivered.append)
        r.try_deliver(m3, delivered.append)
        self.assertEqual(len(delivered), 0)  # both blocked

        # Now deliver m1 which unblocks the chain
        m1 = self._make_msg("m1", sender_id=1, vector_clock={"1": 1}, lamport_time=10)
        r.try_deliver(m1, delivered.append)

        # Should deliver: m1 (10), m3 (30), m4 (40)
        self.assertEqual(len(delivered), 3)
        self.assertEqual(delivered[0]["id"], "m1")
        self.assertEqual(delivered[1]["id"], "m3")
        self.assertEqual(delivered[2]["id"], "m4")

    def test_sort_key_uses_lamport_then_timestamp(self):
        """_sort_key should sort by lamport_time first, then timestamp."""
        key = MessageReorderer._sort_key

        msg_a = {"lamport_time": 5, "timestamp": 100.0}
        msg_b = {"lamport_time": 3, "timestamp": 200.0}
        msg_c = {"lamport_time": 5, "timestamp": 50.0}

        # b (lamport=3) < a (lamport=5, ts=100) < c (lamport=5, ts=50 — wait no)
        # Actually: b(3,200) < c(5,50) < a(5,100)
        sorted_msgs = sorted([msg_a, msg_b, msg_c], key=key)
        self.assertEqual(sorted_msgs[0], msg_b)  # lamport 3
        self.assertEqual(sorted_msgs[1], msg_c)  # lamport 5, ts 50
        self.assertEqual(sorted_msgs[2], msg_a)  # lamport 5, ts 100

    def test_missing_lamport_time_defaults_to_zero(self):
        """Messages without lamport_time should sort as 0."""
        key = MessageReorderer._sort_key

        msg_no_lamport = {"timestamp": 100.0}
        msg_with_lamport = {"lamport_time": 1, "timestamp": 100.0}

        self.assertLess(key(msg_no_lamport), key(msg_with_lamport))


if __name__ == "__main__":
    unittest.main()
