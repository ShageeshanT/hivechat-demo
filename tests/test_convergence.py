"""
Tests for Time Sync Convergence and Multi-Node Simulation
Member: Shagee (IT24103322)

Tests that verify:
  - Offset convergence speed after a reference change
  - Adaptive interval reacts to drift
  - RTT outlier rejection filters bad samples
  - Multi-node scenarios where nodes sync against each other
"""

import unittest
import time
import statistics
from node.time_sync import TimeSyncer, MessageReorderer, LamportClock


class TestOffsetConvergence(unittest.TestCase):
    """Test that the offset converges to the correct value."""

    def test_converges_to_stable_offset(self):
        """Feeding consistent samples should converge quickly."""
        ts = TimeSyncer(node_id=1)
        target_offset = 0.025  # 25 ms

        for _ in range(ts.SAMPLE_COUNT):
            ts._add_sample(target_offset, rtt=0.002)

        self.assertAlmostEqual(ts.get_offset(), target_offset, places=4)

    def test_converges_despite_noisy_samples(self):
        """Median filter should converge even with noisy input."""
        ts = TimeSyncer(node_id=1)
        # Mix of good samples around 10ms with a few outliers
        samples = [0.010, 0.011, 0.009, 0.050, 0.010, 0.012, 0.009, 0.011]

        for s in samples:
            ts._add_sample(s, rtt=0.002)

        # Median should be close to 0.010-0.011, not pulled by the outlier
        self.assertAlmostEqual(ts.get_offset(), statistics.median(samples), places=4)
        self.assertLess(abs(ts.get_offset() - 0.010), 0.005)

    def test_reference_change_resets_convergence(self):
        """After set_reference(), offset should reset and reconverge."""
        ts = TimeSyncer(node_id=1, reference_addr="localhost:5001")

        # Converge to 20ms
        for _ in range(8):
            ts._add_sample(0.020, rtt=0.001)
        self.assertAlmostEqual(ts.get_offset(), 0.020, places=3)

        # Change reference — should reset
        ts.set_reference("localhost:5002")
        self.assertEqual(ts.get_offset(), 0.0)
        self.assertEqual(ts.get_sample_count(), 0)

        # Reconverge to -10ms
        for _ in range(8):
            ts._add_sample(-0.010, rtt=0.001)
        self.assertAlmostEqual(ts.get_offset(), -0.010, places=3)


class TestAdaptiveInterval(unittest.TestCase):
    """Test that the sync interval adapts to drift conditions."""

    def test_interval_decreases_on_large_drift(self):
        """When offset changes significantly, interval should shrink."""
        ts = TimeSyncer(node_id=1)
        initial = ts.get_current_interval()

        # Simulate large offset jump
        ts._add_sample(0.100, rtt=0.001)  # 100ms offset
        ts._adapt_interval()

        self.assertLess(ts.get_current_interval(), initial)

    def test_interval_increases_when_stable(self):
        """When offset is stable, interval should grow."""
        ts = TimeSyncer(node_id=1)

        # Fill with stable samples to set prev_offset
        for _ in range(3):
            ts._add_sample(0.005, rtt=0.001)
            ts._adapt_interval()

        interval_before = ts.get_current_interval()

        # One more stable sample
        ts._add_sample(0.005, rtt=0.001)
        ts._adapt_interval()

        self.assertGreaterEqual(ts.get_current_interval(), interval_before)

    def test_interval_never_below_minimum(self):
        """Even with massive drift, interval should not go below MIN."""
        ts = TimeSyncer(node_id=1)

        for i in range(20):
            ts._add_sample(i * 0.1, rtt=0.001)  # increasing offset each time
            ts._adapt_interval()

        self.assertGreaterEqual(ts.get_current_interval(), ts.MIN_SYNC_INTERVAL)

    def test_interval_never_above_maximum(self):
        """Interval should not exceed MAX even after long stability."""
        ts = TimeSyncer(node_id=1)

        for _ in range(50):
            ts._add_sample(0.001, rtt=0.001)
            ts._adapt_interval()

        self.assertLessEqual(ts.get_current_interval(), ts.MAX_SYNC_INTERVAL)


class TestRttOutlierRejection(unittest.TestCase):
    """Test that RTT-based outlier rejection works correctly."""

    def test_normal_samples_accepted(self):
        """Samples with normal RTT should all be accepted."""
        ts = TimeSyncer(node_id=1)

        for i in range(5):
            ts._add_sample(0.010, rtt=0.002)

        self.assertEqual(ts.get_sample_count(), 5)
        self.assertEqual(ts._rejected_count, 0)

    def test_high_rtt_sample_rejected(self):
        """A sample with very high RTT should be rejected."""
        ts = TimeSyncer(node_id=1)

        # Build up normal RTT history
        for _ in range(5):
            ts._add_sample(0.010, rtt=0.002)

        count_before = ts.get_sample_count()

        # Now add a sample with 10x normal RTT
        ts._add_sample(0.050, rtt=0.060)

        # Should be rejected — count unchanged
        self.assertEqual(ts.get_sample_count(), count_before)
        self.assertEqual(ts._rejected_count, 1)

    def test_offset_not_corrupted_by_outlier_rtt(self):
        """Offset should remain stable when high-RTT samples are rejected."""
        ts = TimeSyncer(node_id=1)

        # Converge to 10ms
        for _ in range(8):
            ts._add_sample(0.010, rtt=0.002)
        offset_before = ts.get_offset()

        # Try to corrupt with high-RTT outlier
        ts._add_sample(0.500, rtt=0.200)

        # Offset should be unchanged
        self.assertAlmostEqual(ts.get_offset(), offset_before, places=4)

    def test_no_rejection_with_insufficient_history(self):
        """With < 3 RTT samples, nothing should be rejected."""
        ts = TimeSyncer(node_id=1)

        ts._add_sample(0.010, rtt=0.002)
        ts._add_sample(0.010, rtt=0.100)  # high RTT but only 2 samples

        self.assertEqual(ts.get_sample_count(), 2)
        self.assertEqual(ts._rejected_count, 0)

    def test_reference_change_clears_rtt_history(self):
        """set_reference should clear RTT samples too."""
        ts = TimeSyncer(node_id=1, reference_addr="localhost:5001")

        for _ in range(5):
            ts._add_sample(0.010, rtt=0.002)
        self.assertEqual(len(ts._rtt_samples), 5)

        ts.set_reference("localhost:5002")
        self.assertEqual(len(ts._rtt_samples), 0)


class TestMultiNodeSimulation(unittest.TestCase):
    """Simulate multiple nodes syncing and exchanging messages."""

    def test_three_node_offset_independence(self):
        """Each node should maintain its own independent offset estimate."""
        ts1 = TimeSyncer(node_id=1)
        ts2 = TimeSyncer(node_id=2)
        ts3 = TimeSyncer(node_id=3)

        # Each node has a different offset
        for _ in range(8):
            ts1._add_sample(0.010, rtt=0.001)
            ts2._add_sample(-0.005, rtt=0.001)
            ts3._add_sample(0.025, rtt=0.001)

        self.assertAlmostEqual(ts1.get_offset(), 0.010, places=3)
        self.assertAlmostEqual(ts2.get_offset(), -0.005, places=3)
        self.assertAlmostEqual(ts3.get_offset(), 0.025, places=3)

    def test_lamport_ordering_across_nodes(self):
        """Messages flowing through 3 nodes should maintain Lamport ordering."""
        ts1 = TimeSyncer(node_id=1)
        ts2 = TimeSyncer(node_id=2)
        ts3 = TimeSyncer(node_id=3)

        # N1 sends → t1
        t1 = ts1.lamport.tick()

        # N2 receives from N1, then sends → t2, t3
        t2 = ts2.lamport.update(t1)
        t3 = ts2.lamport.tick()

        # N3 receives from N2 → t4
        t4 = ts3.lamport.update(t3)

        # N1 receives from N3 → t5
        t5 = ts1.lamport.update(t4)

        # Full causal chain
        self.assertLess(t1, t2)
        self.assertLess(t2, t3)
        self.assertLess(t3, t4)
        self.assertLess(t4, t5)

    def test_adjusted_timestamps_comparable_across_nodes(self):
        """Offset-corrected timestamps from different nodes should be comparable."""
        ts1 = TimeSyncer(node_id=1)
        ts2 = TimeSyncer(node_id=2)

        # Both nodes have a small positive offset
        for _ in range(8):
            ts1._add_sample(0.005, rtt=0.001)
            ts2._add_sample(0.003, rtt=0.001)

        t1 = ts1.get_adjusted_time()
        time.sleep(0.1)  # 100ms gap — larger than the offset difference
        t2 = ts2.get_adjusted_time()

        # t2 was created 100ms later, so should be larger
        self.assertGreater(t2, t1)

    def test_stats_report_includes_all_fields(self):
        """get_stats should include adaptive interval and rejection info."""
        ts = TimeSyncer(node_id=1)
        ts._add_sample(0.010, rtt=0.002)

        stats = ts.get_stats()

        self.assertIn("current_interval", stats)
        self.assertIn("rejected_samples", stats)
        self.assertIn("rtt_samples_ms", stats)
        self.assertIn("offset_ms", stats)
        self.assertIn("node_id", stats)


if __name__ == "__main__":
    unittest.main()
