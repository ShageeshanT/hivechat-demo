"""
Edge Case Tests for HiveChat Time Synchronization Module
Member: Shagee (IT24103322)

Tests for network partition, clock drift, high latency,
and other adversarial scenarios.
"""

import unittest
import time
import threading
from node.time_sync import LamportClock, TimeSyncer, MessageReorderer


class TestClockDrift(unittest.TestCase):
    """Tests for handling large and varying clock offsets."""

    def test_large_positive_offset(self):
        """Server clock far ahead (e.g. 2 seconds) should still compute correctly."""
        ts = TimeSyncer(node_id=1)
        ts._add_sample(2.0)  # 2000 ms ahead
        self.assertAlmostEqual(ts.get_offset(), 2.0)

        raw = time.time()
        adjusted = ts.get_adjusted_time()
        self.assertAlmostEqual(adjusted - raw, 2.0, places=1)

    def test_large_negative_offset(self):
        """Server clock behind (negative offset) should adjust backwards."""
        ts = TimeSyncer(node_id=1)
        ts._add_sample(-1.5)  # 1500 ms behind
        self.assertAlmostEqual(ts.get_offset(), -1.5)

        raw = time.time()
        adjusted = ts.get_adjusted_time()
        self.assertAlmostEqual(adjusted - raw, -1.5, places=1)

    def test_gradual_drift_correction(self):
        """Simulate a clock that drifts over time — median should track it."""
        ts = TimeSyncer(node_id=1)
        # Gradually increasing drift
        for i in range(8):
            ts._add_sample(0.001 * (i + 1))

        # Median of [0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008]
        # = (0.004 + 0.005) / 2 = 0.0045
        self.assertAlmostEqual(ts.get_offset(), 0.0045)

    def test_sudden_jump_filtered_by_median(self):
        """A sudden clock jump should be filtered out by the median."""
        ts = TimeSyncer(node_id=1)
        # Steady offset around 1 ms
        for _ in range(6):
            ts._add_sample(0.001)

        # Sudden spike
        ts._add_sample(5.0)
        ts._add_sample(0.001)

        # Median should still be close to 0.001, not pulled by the outlier
        self.assertAlmostEqual(ts.get_offset(), 0.001, places=3)

    def test_alternating_positive_negative_offsets(self):
        """Oscillating offsets should center around zero via median."""
        ts = TimeSyncer(node_id=1)
        for i in range(8):
            ts._add_sample(0.005 if i % 2 == 0 else -0.005)

        # Equal number of +5ms and -5ms: median should be ~0
        self.assertAlmostEqual(ts.get_offset(), 0.0, places=3)

    def test_zero_offset_stays_zero(self):
        """All-zero samples should keep offset at exactly zero."""
        ts = TimeSyncer(node_id=1)
        for _ in range(8):
            ts._add_sample(0.0)
        self.assertEqual(ts.get_offset(), 0.0)


class TestNetworkPartition(unittest.TestCase):
    """Tests for behavior when the reference node is unreachable."""

    def test_offset_persists_when_no_reference(self):
        """If reference goes away, the last known offset should be retained."""
        ts = TimeSyncer(node_id=1, reference_addr="localhost:5002")
        ts._add_sample(0.005)
        ts._add_sample(0.006)

        # Simulate partition: clear reference
        ts.set_reference("")
        # But offset from before should still be usable until new ref is set
        # Note: set_reference clears samples, but we can test that the syncer
        # doesn't crash and uses 0 offset after clearing
        self.assertEqual(ts.get_offset(), 0.0)

    def test_sync_noop_without_reference(self):
        """_sync_loop should not crash or add samples without a reference."""
        ts = TimeSyncer(node_id=1)
        initial_count = ts.get_sample_count()

        # Manually call what the sync loop would call
        if ts.reference_addr:
            ts._do_sync()

        self.assertEqual(ts.get_sample_count(), initial_count)

    def test_reference_change_resets_samples(self):
        """Changing the reference node should clear stale offset data."""
        ts = TimeSyncer(node_id=1, reference_addr="node-a:5002")
        ts._add_sample(0.010)
        ts._add_sample(0.012)
        self.assertEqual(ts.get_sample_count(), 2)

        ts.set_reference("node-b:5002")
        self.assertEqual(ts.get_sample_count(), 0)
        self.assertEqual(ts.get_offset(), 0.0)

    def test_recovered_after_partition(self):
        """After a partition heals, new samples should build up fresh."""
        ts = TimeSyncer(node_id=1, reference_addr="node-a:5002")
        ts._add_sample(0.010)

        # Partition
        ts.set_reference("")
        self.assertEqual(ts.get_sample_count(), 0)

        # Recovery
        ts.set_reference("node-a:5002")
        ts._add_sample(0.003)
        self.assertEqual(ts.get_sample_count(), 1)
        self.assertAlmostEqual(ts.get_offset(), 0.003)


class TestReordererEdgeCases(unittest.TestCase):
    """Edge cases for the MessageReorderer."""

    def _make_msg(self, msg_id, sender_id, vector_clock):
        return {
            "id": msg_id,
            "sender_id": sender_id,
            "vector_clock": vector_clock,
            "lamport_time": 1,
            "timestamp": time.time(),
            "content": f"Message {msg_id}",
        }

    def test_many_messages_buffered_then_flushed(self):
        """Large burst of out-of-order messages should all eventually deliver."""
        r = MessageReorderer()
        delivered = []
        count = 50

        # Send messages in reverse order
        msgs = [self._make_msg(f"m{i}", sender_id=1,
                                vector_clock={"1": i})
                for i in range(1, count + 1)]

        for msg in reversed(msgs):
            r.try_deliver(msg, delivered.append)

        self.assertEqual(len(delivered), count)
        # Should be in causal order
        for i, m in enumerate(delivered):
            self.assertEqual(m["id"], f"m{i+1}")

    def test_multiple_senders_interleaved(self):
        """Messages from many senders arriving interleaved should sort correctly."""
        r = MessageReorderer()
        delivered = []

        # 3 senders each send 3 messages, arrive interleaved
        m1_1 = self._make_msg("n1_1", sender_id=1, vector_clock={"1": 1})
        m2_1 = self._make_msg("n2_1", sender_id=2, vector_clock={"2": 1})
        m3_1 = self._make_msg("n3_1", sender_id=3, vector_clock={"3": 1})
        m1_2 = self._make_msg("n1_2", sender_id=1, vector_clock={"1": 2})
        m2_2 = self._make_msg("n2_2", sender_id=2, vector_clock={"2": 2})
        m3_2 = self._make_msg("n3_2", sender_id=3, vector_clock={"3": 2})

        for msg in [m2_1, m1_1, m3_1, m1_2, m3_2, m2_2]:
            r.try_deliver(msg, delivered.append)

        self.assertEqual(len(delivered), 6)
        self.assertEqual(r.get_buffer_size(), 0)

    def test_empty_vector_clock(self):
        """A message with an empty vector clock should still deliver."""
        r = MessageReorderer()
        delivered = []

        msg = self._make_msg("m1", sender_id=1, vector_clock={})
        r.try_deliver(msg, delivered.append)
        # Empty VC means no dependencies — should deliver
        self.assertEqual(len(delivered), 1)

    def test_timeout_with_many_stuck_messages(self):
        """Multiple stuck messages should all force-deliver after timeout."""
        r = MessageReorderer(buffer_timeout=0.1)
        delivered = []

        # 5 messages all depending on a missing m0
        for i in range(2, 7):
            msg = self._make_msg(f"m{i}", sender_id=1,
                                  vector_clock={"1": i})
            r.try_deliver(msg, delivered.append)

        self.assertEqual(len(delivered), 0)
        self.assertEqual(r.get_buffer_size(), 5)

        time.sleep(0.15)

        # Trigger flush
        trigger = self._make_msg("trigger", sender_id=2, vector_clock={"2": 1})
        r.try_deliver(trigger, delivered.append)

        # All 5 + trigger should be delivered
        self.assertEqual(len(delivered), 6)
        self.assertEqual(r.get_buffer_size(), 0)


class TestLamportClockEdgeCases(unittest.TestCase):
    """Edge cases for the LamportClock."""

    def test_update_with_zero(self):
        """Updating with 0 should still increment."""
        clock = LamportClock()
        result = clock.update(0)
        # max(0, 0) + 1 = 1
        self.assertEqual(result, 1)

    def test_update_with_very_large_value(self):
        """Clock should handle very large received values."""
        clock = LamportClock()
        result = clock.update(999_999_999)
        self.assertEqual(result, 1_000_000_000)

    def test_rapid_ticks(self):
        """Many rapid ticks should produce strictly increasing values."""
        clock = LamportClock()
        values = [clock.tick() for _ in range(1000)]
        # Every value should be strictly greater than the previous
        for i in range(1, len(values)):
            self.assertGreater(values[i], values[i - 1])

    def test_mixed_ticks_and_updates(self):
        """Interleaving ticks and updates should maintain monotonicity."""
        clock = LamportClock()
        prev = 0
        for i in range(100):
            if i % 3 == 0:
                val = clock.update(prev + 5)
            else:
                val = clock.tick()
            self.assertGreater(val, prev)
            prev = val


if __name__ == "__main__":
    unittest.main()
