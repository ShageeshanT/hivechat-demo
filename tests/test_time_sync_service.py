"""
Tests for HiveChat Time Sync gRPC Service
Member: Shagee (IT24103322)
"""

import unittest
import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'proto'))

from node.time_sync_service import TimeSyncServicer, start_sync_server, sync_once


class TestTimeSyncServicer(unittest.TestCase):
    """Tests for the gRPC TimeSyncServicer handler."""

    def test_get_time_returns_server_time(self):
        """GetTime should return a server_time close to now."""
        servicer = TimeSyncServicer(node_id=1)

        # Create a mock request
        class MockRequest:
            client_send_time = time.time()

        class MockContext:
            pass

        before = time.time()
        response = servicer.GetTime(MockRequest(), MockContext())
        after = time.time()

        self.assertGreaterEqual(response.server_time, before)
        self.assertLessEqual(response.server_time, after)

    def test_get_time_echoes_client_send_time(self):
        """GetTime should echo back the client's send time."""
        servicer = TimeSyncServicer(node_id=1)

        class MockRequest:
            client_send_time = 12345.678

        class MockContext:
            pass

        response = servicer.GetTime(MockRequest(), MockContext())
        self.assertEqual(response.client_send_time, 12345.678)

    def test_get_time_returns_server_node_id(self):
        """GetTime should include the server's node ID."""
        servicer = TimeSyncServicer(node_id=42)

        class MockRequest:
            client_send_time = time.time()

        class MockContext:
            pass

        response = servicer.GetTime(MockRequest(), MockContext())
        self.assertEqual(response.server_node_id, 42)


class TestGrpcRoundTrip(unittest.TestCase):
    """Integration test: start a real gRPC server and sync against it."""

    @classmethod
    def setUpClass(cls):
        cls.server, cls.port = start_sync_server(node_id=1, port=0)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop(grace=1)

    def test_sync_once_returns_offset(self):
        """sync_once should return a small offset and RTT."""
        result = sync_once(f"localhost:{self.port}", node_id=2)

        self.assertNotIn('error', result)
        self.assertIn('offset', result)
        self.assertIn('rtt', result)
        self.assertEqual(result['server_node_id'], 1)
        # Offset should be small since both are on same machine
        self.assertLess(abs(result['offset']), 0.5)
        # RTT should be under 2 seconds for localhost
        self.assertLess(result['rtt'], 2.0)

    def test_sync_multiple_rounds(self):
        """Multiple syncs should all succeed and produce consistent offsets."""
        offsets = []
        for _ in range(5):
            result = sync_once(f"localhost:{self.port}", node_id=2)
            self.assertNotIn('error', result)
            offsets.append(result['offset'])

        # All offsets should be close to each other (same machine)
        spread = max(offsets) - min(offsets)
        self.assertLess(spread, 0.05)

    def test_sync_once_bad_address_returns_error(self):
        """sync_once to a dead address should return an error dict."""
        result = sync_once("localhost:59999", node_id=2)
        self.assertIn('error', result)


if __name__ == "__main__":
    unittest.main()
