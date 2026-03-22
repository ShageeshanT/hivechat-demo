"""
Tests for HiveChat Time Synchronization Module
Member: Shagee (IT24103322)
"""

import unittest
import threading
from node.time_sync import LamportClock


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


if __name__ == "__main__":
    unittest.main()
