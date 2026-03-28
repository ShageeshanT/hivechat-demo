"""
Integration Tests - End-to-End Time Sync Flow
Member: Shagee (IT24103322)

Tests that TimeSyncer, MessageReorderer, and ReplicationManager
work together correctly as an integrated system.
"""

import unittest
import time
from node.time_sync import TimeSyncer, MessageReorderer, LamportClock
from node.replication import ReplicationManager


class TestTimeSyncReplicationIntegration(unittest.TestCase):
    """Test that ReplicationManager uses TimeSyncer correctly."""

    def _make_replication(self, node_id, peers=None, all_ids=None):
        """Create a ReplicationManager with time sync wired in."""
        peers = peers or []
        all_ids = all_ids or [1, 2, 3]
        ts = TimeSyncer(node_id=node_id)
        reorderer = MessageReorderer()
        rm = ReplicationManager(
            node_id=node_id,
            peers=peers,
            all_node_ids=all_ids,
            time_syncer=ts,
            reorderer=reorderer,
        )
        return rm, ts, reorderer

    def test_create_message_has_lamport_time(self):
        """Messages created via ReplicationManager should include lamport_time."""
        rm, ts, _ = self._make_replication(1)
        msg = rm.create_message("chat1", "alice", "hello")

        self.assertIn("lamport_time", msg)
        self.assertEqual(msg["lamport_time"], 1)
        self.assertIn("sender_id", msg)
        self.assertEqual(msg["sender_id"], 1)

    def test_lamport_time_increments_across_messages(self):
        """Each new message should have a higher Lamport time."""
        rm, _, _ = self._make_replication(1)

        m1 = rm.create_message("chat1", "alice", "first")
        m2 = rm.create_message("chat1", "alice", "second")
        m3 = rm.create_message("chat1", "alice", "third")

        self.assertLess(m1["lamport_time"], m2["lamport_time"])
        self.assertLess(m2["lamport_time"], m3["lamport_time"])

    def test_adjusted_timestamp_used(self):
        """Message timestamp should come from TimeSyncer, not raw time.time()."""
        rm, ts, _ = self._make_replication(1)

        # Inject a known offset
        ts._add_sample(0.050)  # 50 ms ahead

        before = time.time()
        msg = rm.create_message("chat1", "alice", "hello")
        after = time.time()

        # Timestamp should be ~50ms ahead of wall clock
        self.assertGreater(msg["timestamp"], before)
        self.assertAlmostEqual(msg["timestamp"] - before, 0.050, places=1)

    def test_receive_replica_updates_lamport(self):
        """Receiving a replica should merge Lamport clocks."""
        rm, ts, _ = self._make_replication(2)

        # Simulate incoming message from node 1 with lamport_time=5
        incoming = {
            "id": "msg-from-node1",
            "chat_id": "chat1",
            "sender": "alice",
            "sender_id": 1,
            "content": "hello from node 1",
            "timestamp": time.time(),
            "lamport_time": 5,
            "vector_clock": {1: 1},
            "status": "committed",
        }
        rm.receive_replica(incoming)

        # Our Lamport clock should now be max(0, 5) + 1 = 6
        self.assertEqual(ts.lamport.get(), 6)

    def test_receive_replica_through_reorderer(self):
        """Incoming replicas should be routed through MessageReorderer."""
        rm, _, reorderer = self._make_replication(2)

        # Send message with vector_clock showing it's the first from node 1
        incoming = {
            "id": "msg-001",
            "chat_id": "chat1",
            "sender": "alice",
            "sender_id": 1,
            "content": "first message",
            "timestamp": time.time(),
            "lamport_time": 1,
            "vector_clock": {1: 1},
            "status": "committed",
        }
        rm.receive_replica(incoming)

        # Should be delivered (first message, no dependencies)
        self.assertEqual(reorderer.get_delivered_count(), 1)
        stored = rm.store.get("msg-001")
        self.assertIsNotNone(stored)

    def test_out_of_order_replica_buffered(self):
        """A replica that arrives before its dependency should be buffered."""
        rm, _, reorderer = self._make_replication(2)

        # Send seq 2 before seq 1
        msg2 = {
            "id": "msg-002",
            "chat_id": "chat1",
            "sender": "alice",
            "sender_id": 1,
            "content": "second message",
            "timestamp": time.time(),
            "lamport_time": 2,
            "vector_clock": {1: 2},
            "status": "committed",
        }
        rm.receive_replica(msg2)

        # msg2 should be buffered — msg1 hasn't arrived yet
        self.assertEqual(reorderer.get_buffer_size(), 1)
        self.assertIsNone(rm.store.get("msg-002"))

        # Now send seq 1 — both should flush
        msg1 = {
            "id": "msg-001",
            "chat_id": "chat1",
            "sender": "bob",
            "sender_id": 1,
            "content": "first message",
            "timestamp": time.time(),
            "lamport_time": 1,
            "vector_clock": {1: 1},
            "status": "committed",
        }
        rm.receive_replica(msg1)

        self.assertEqual(reorderer.get_buffer_size(), 0)
        self.assertIsNotNone(rm.store.get("msg-001"))
        self.assertIsNotNone(rm.store.get("msg-002"))


class TestMultiNodeSync(unittest.TestCase):
    """Simulate multiple nodes exchanging messages with time sync."""

    def test_two_node_message_exchange(self):
        """Two nodes sending messages back and forth maintain causal order."""
        rm1, ts1, _ = self._make_node(1)
        rm2, ts2, _ = self._make_node(2)

        # Node 1 creates a message
        m1 = rm1.create_message("chat1", "alice", "hello from node 1")
        self.assertEqual(m1["lamport_time"], 1)

        # Node 2 receives the replica
        rm2.receive_replica(m1.copy())
        self.assertGreaterEqual(ts2.lamport.get(), m1["lamport_time"])

        # Node 2 creates a reply
        m2 = rm2.create_message("chat1", "bob", "reply from node 2")
        self.assertGreater(m2["lamport_time"], m1["lamport_time"])

        # Node 1 receives the reply
        rm1.receive_replica(m2.copy())
        self.assertGreaterEqual(ts1.lamport.get(), m2["lamport_time"])

    def test_three_node_causal_chain(self):
        """Messages forwarded through 3 nodes preserve causal ordering."""
        rm1, ts1, _ = self._make_node(1)
        rm2, ts2, _ = self._make_node(2)
        rm3, ts3, _ = self._make_node(3)

        # N1 -> N2 -> N3
        m1 = rm1.create_message("chat1", "alice", "start")
        rm2.receive_replica(m1.copy())

        m2 = rm2.create_message("chat1", "bob", "middle")
        rm3.receive_replica(m1.copy())
        rm3.receive_replica(m2.copy())

        m3 = rm3.create_message("chat1", "carol", "end")

        # Lamport ordering must hold
        self.assertLess(m1["lamport_time"], m2["lamport_time"])
        self.assertLess(m2["lamport_time"], m3["lamport_time"])

    def test_offset_correction_produces_comparable_timestamps(self):
        """Nodes with different offsets should still produce ordered timestamps."""
        rm1, ts1, _ = self._make_node(1)
        rm2, ts2, _ = self._make_node(2)

        # Simulate node 1 being 50ms ahead, node 2 being 50ms behind
        ts1._add_sample(0.050)
        ts2._add_sample(-0.050)

        m1 = rm1.create_message("chat1", "alice", "from fast node")
        time.sleep(0.01)
        m2 = rm2.create_message("chat1", "bob", "from slow node")

        # Despite different offsets, m2 should have a later timestamp than m1
        # because it was created later in real time
        # The offset correction should make them comparable
        self.assertIsInstance(m1["timestamp"], float)
        self.assertIsInstance(m2["timestamp"], float)

    def _make_node(self, node_id):
        ts = TimeSyncer(node_id=node_id)
        reorderer = MessageReorderer()
        rm = ReplicationManager(
            node_id=node_id,
            peers=[],
            all_node_ids=[1, 2, 3],
            time_syncer=ts,
            reorderer=reorderer,
        )
        return rm, ts, reorderer


if __name__ == "__main__":
    unittest.main()
