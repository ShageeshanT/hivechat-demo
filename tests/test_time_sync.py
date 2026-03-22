"""
Tests for HiveChat Time Synchronization Module
Member: Shagee (IT24103322)
"""

import unittest
import time
import threading
from node.time_sync import LamportClock, TimeSyncer


class TestLamportClock(unittest.TestCase):
    """Tests for the LamportClock class."""

    def test_initial_value_is_zero(self):
        clock = LamportClock()
        self.assertEqual(clock.get(), 0)

    def test_tick_increments_by_one(self):
        clock = LamportClock()
        self.assertEqual(clock.tick(), 1)
        self.assertEqual(clock.tick(), 2)
        self.assertEqual(clock.tick(), 3)

    def test_update_takes_max_plus_one(self):
        clock = LamportClock()
        clock.tick()  # clock = 1

        # Receive a message with clock value 5
        result = clock.update(5)
        # max(1, 5) + 1 = 6
        self.assertEqual(result, 6)

    def test_update_when_local_is_higher(self):
        clock = LamportClock()
        for _ in range(10):
            clock.tick()  # clock = 10

        # Receive a message with lower clock value
        result = clock.update(3)
        # max(10, 3) + 1 = 11
        self.assertEqual(result, 11)

    def test_causal_ordering_two_nodes(self):
        """Simulate two nodes exchanging messages and verify causal order."""
        node1 = LamportClock()
        node2 = LamportClock()

        # Node 1 sends message
        t1 = node1.tick()  # 1

        # Node 2 receives and replies
        t2 = node2.update(t1)  # max(0,1)+1 = 2
        t3 = node2.tick()      # 3

        # Node 1 receives reply
        t4 = node1.update(t3)  # max(1,3)+1 = 4

        # Causal chain: t1 < t2 < t3 < t4
        self.assertLess(t1, t2)
        self.assertLess(t2, t3)
        self.assertLess(t3, t4)

    def test_causal_ordering_three_nodes(self):
        """Simulate three nodes to verify ordering across multiple hops."""
        n1 = LamportClock()
        n2 = LamportClock()
        n3 = LamportClock()

        # N1 sends to N2
        t1 = n1.tick()
        t2 = n2.update(t1)

        # N2 sends to N3
        t3 = n2.tick()
        t4 = n3.update(t3)

        # N3 sends to N1
        t5 = n3.tick()
        t6 = n1.update(t5)

        # Full causal chain must be strictly increasing
        self.assertLess(t1, t2)
        self.assertLess(t2, t3)
        self.assertLess(t3, t4)
        self.assertLess(t4, t5)
        self.assertLess(t5, t6)

    def test_concurrent_events_get_different_values(self):
        """Two nodes ticking independently should have independent clocks."""
        n1 = LamportClock()
        n2 = LamportClock()

        a = n1.tick()  # 1
        b = n2.tick()  # 1

        # Concurrent events CAN have the same Lamport value
        # This is a known limitation — Lamport clocks don't detect concurrency
        self.assertEqual(a, 1)
        self.assertEqual(b, 1)

    def test_thread_safety(self):
        """Multiple threads ticking the same clock should not lose increments."""
        clock = LamportClock()
        num_threads = 10
        ticks_per_thread = 100

        def tick_many():
            for _ in range(ticks_per_thread):
                clock.tick()

        threads = [threading.Thread(target=tick_many) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All increments should be accounted for
        self.assertEqual(clock.get(), num_threads * ticks_per_thread)


class TestTimeSyncer(unittest.TestCase):
    """Tests for the TimeSyncer class."""

    def test_initial_state(self):
        ts = TimeSyncer(node_id=1)
        self.assertEqual(ts.get_offset(), 0.0)
        self.assertEqual(ts.get_sample_count(), 0)

    def test_add_sample_updates_offset(self):
        ts = TimeSyncer(node_id=1)
        ts._add_sample(0.005)
        self.assertAlmostEqual(ts.get_offset(), 0.005)

    def test_median_filtering(self):
        """Median should filter out outlier samples."""
        ts = TimeSyncer(node_id=1)
        # Add mostly small offsets with one big outlier
        for val in [0.001, 0.002, 0.001, 0.050, 0.002]:
            ts._add_sample(val)
        # Median of [0.001, 0.002, 0.001, 0.050, 0.002] = 0.002
        self.assertAlmostEqual(ts.get_offset(), 0.002)

    def test_sample_window_limit(self):
        """Only the last SAMPLE_COUNT samples should be kept."""
        ts = TimeSyncer(node_id=1)
        # Fill beyond the sample window
        for i in range(ts.SAMPLE_COUNT + 5):
            ts._add_sample(0.001 * i)
        self.assertEqual(ts.get_sample_count(), ts.SAMPLE_COUNT)

    def test_get_adjusted_time_applies_offset(self):
        ts = TimeSyncer(node_id=1)
        ts._add_sample(0.010)  # 10 ms offset

        raw = time.time()
        adjusted = ts.get_adjusted_time()

        # adjusted should be ~10 ms ahead of raw
        diff = adjusted - raw
        self.assertAlmostEqual(diff, 0.010, places=2)

    def test_correct_timestamp_with_no_remote_offset(self):
        ts = TimeSyncer(node_id=1)
        ts._add_sample(0.005)  # our offset is 5 ms

        remote_ts = 1000.0
        corrected = ts.correct_timestamp(remote_ts)
        # corrected = remote_ts - 0 + 0.005 = 1000.005
        self.assertAlmostEqual(corrected, 1000.005)

    def test_correct_timestamp_with_remote_offset(self):
        ts = TimeSyncer(node_id=1)
        ts._add_sample(0.005)  # our offset is 5 ms

        remote_ts = 1000.0
        corrected = ts.correct_timestamp(remote_ts, remote_offset=0.003)
        # corrected = 1000.0 - 0.003 + 0.005 = 1000.002
        self.assertAlmostEqual(corrected, 1000.002)

    def test_set_reference_clears_samples(self):
        ts = TimeSyncer(node_id=1, reference_addr="localhost:5002")
        ts._add_sample(0.010)
        ts._add_sample(0.012)
        self.assertEqual(ts.get_sample_count(), 2)

        ts.set_reference("localhost:5003")
        self.assertEqual(ts.get_sample_count(), 0)
        self.assertEqual(ts.get_offset(), 0.0)

    def test_do_sync_adds_sample(self):
        """_do_sync (stub) should produce an offset sample."""
        ts = TimeSyncer(node_id=1, reference_addr="localhost:5002")
        self.assertEqual(ts.get_sample_count(), 0)

        ts._do_sync()
        self.assertEqual(ts.get_sample_count(), 1)
        # Stub simulates 1 ms ahead, so offset should be roughly positive
        self.assertGreater(ts.get_offset(), 0)

    def test_lamport_clock_embedded(self):
        """TimeSyncer should carry a LamportClock instance."""
        ts = TimeSyncer(node_id=1)
        t1 = ts.lamport.tick()
        t2 = ts.lamport.tick()
        self.assertEqual(t1, 1)
        self.assertEqual(t2, 2)

    def test_background_thread_starts_and_stops(self):
        """start() should launch a daemon thread, stop() should signal it."""
        ts = TimeSyncer(node_id=1, reference_addr="localhost:5002")
        ts.start()
        time.sleep(0.1)  # let thread spin up
        self.assertTrue(ts._running)

        ts.stop()
        time.sleep(0.1)
        self.assertFalse(ts._running)


if __name__ == "__main__":
    unittest.main()
