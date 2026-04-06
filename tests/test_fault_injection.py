"""
Tests for Time Sync Fault Injection
Member: Shagee (IT24103322)
"""

import unittest
import time
from node.time_sync import TimeSyncer
from node.fault_injection import FaultInjector


class TestFaultInjector(unittest.TestCase):
    """Tests for the FaultInjector testing utility."""

    def test_clock_jump_positive(self):
        """Injecting a positive clock jump should shift adjusted time forward."""
        ts = TimeSyncer(node_id=1)
        fi = FaultInjector(ts)
        fi.enable()

        fi.inject_clock_jump(0.500)  # 500ms ahead

        raw = time.time()
        adjusted = ts.get_adjusted_time()

        # Should be ~500ms ahead of raw time
        self.assertAlmostEqual(adjusted - raw, 0.500, places=1)

        fi.disable()

    def test_clock_jump_negative(self):
        """Injecting a negative clock jump should shift adjusted time backward."""
        ts = TimeSyncer(node_id=1)
        fi = FaultInjector(ts)
        fi.enable()

        fi.inject_clock_jump(-0.300)

        raw = time.time()
        adjusted = ts.get_adjusted_time()

        self.assertAlmostEqual(adjusted - raw, -0.300, places=1)

        fi.disable()

    def test_partition_blocks_sync(self):
        """Simulated partition should prevent _do_sync from adding samples."""
        ts = TimeSyncer(node_id=1, reference_addr="localhost:5001")
        fi = FaultInjector(ts)
        fi.enable()

        fi.simulate_partition()
        count_before = ts.get_sample_count()
        ts._do_sync()

        # No new sample should be added during partition
        self.assertEqual(ts.get_sample_count(), count_before)

        fi.disable()

    def test_heal_partition_restores_sync(self):
        """After healing, sync should work again."""
        ts = TimeSyncer(node_id=1, reference_addr="localhost:5001")
        fi = FaultInjector(ts)
        fi.enable()

        fi.simulate_partition()
        ts._do_sync()
        count_during = ts.get_sample_count()

        fi.heal_partition()
        ts._do_sync()

        # Should have a new sample after healing
        self.assertEqual(ts.get_sample_count(), count_during + 1)

        fi.disable()

    def test_corrupt_sample_injected(self):
        """corrupt_sample should add a sample directly to the syncer."""
        ts = TimeSyncer(node_id=1)
        fi = FaultInjector(ts)
        fi.enable()

        fi.corrupt_sample(1.0)  # inject 1 second offset

        self.assertEqual(ts.get_sample_count(), 1)
        self.assertAlmostEqual(ts.get_offset(), 1.0, places=2)

        fi.disable()

    def test_disable_restores_original(self):
        """After disable(), TimeSyncer should behave normally."""
        ts = TimeSyncer(node_id=1)
        fi = FaultInjector(ts)
        fi.enable()

        fi.inject_clock_jump(5.0)
        adjusted_with_fault = ts.get_adjusted_time()

        fi.disable()
        adjusted_after = ts.get_adjusted_time()

        # After disable, the 5 second jump should be gone
        self.assertAlmostEqual(adjusted_after, time.time(), places=0)
        self.assertGreater(adjusted_with_fault, adjusted_after + 4)

    def test_clear_all_resets_state(self):
        """clear_all should reset all injection parameters."""
        ts = TimeSyncer(node_id=1)
        fi = FaultInjector(ts)
        fi.enable()

        fi.inject_clock_jump(1.0)
        fi.simulate_network_delay(0.5)
        fi.simulate_partition()

        fi.clear_all()

        state = fi.get_state()
        self.assertEqual(state["clock_jump_s"], 0.0)
        self.assertEqual(state["network_delay_s"], 0.0)
        self.assertFalse(state["partition"])

        fi.disable()

    def test_get_state(self):
        """get_state should reflect current injection config."""
        ts = TimeSyncer(node_id=1)
        fi = FaultInjector(ts)

        state = fi.get_state()
        self.assertFalse(state["active"])

        fi.enable()
        fi.inject_clock_jump(0.1)
        fi.simulate_network_delay(0.05)

        state = fi.get_state()
        self.assertTrue(state["active"])
        self.assertAlmostEqual(state["clock_jump_s"], 0.1)
        self.assertAlmostEqual(state["network_delay_s"], 0.05)

        fi.disable()


if __name__ == "__main__":
    unittest.main()
