"""
HiveChat - Data Replication Test Suite
Member: Maheesha (IT24103477)

Tests for:
  - MessageStore        (save, get, get_by_chat, deduplication, mark_committed)
  - VectorClock         (tick, update, happened_before, concurrent)
  - ReplicationManager  (create_message, write, receive_replica, sync, apply_committed_entry)
  - Integration         (with TimeSyncer + MessageReorderer)
"""

import unittest
import time
import uuid

from node.replication import ReplicationManager, MessageStore, VectorClock
from node.time_sync import TimeSyncer, MessageReorderer


# ─────────────────────────────────────────────────────────────────────────────
# Section 1: MessageStore unit tests
# ─────────────────────────────────────────────────────────────────────────────

class TestMessageStore(unittest.TestCase):
    """Unit tests for the in-memory MessageStore."""

    def setUp(self):
        self.store = MessageStore()

    def _make_msg(self, status="committed"):
        return {
            "id": str(uuid.uuid4()),
            "chat_id": "room1",
            "sender": "alice",
            "content": "hello",
            "timestamp": time.time(),
            "vector_clock": {1: 1},
            "status": status,
        }

    def test_save_and_get(self):
        """Saved message should be retrievable by ID."""
        msg = self._make_msg()
        self.store.save(msg)
        retrieved = self.store.get(msg["id"])
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved["content"], "hello")

    def test_get_missing_returns_none(self):
        """get() on an unknown ID should return None."""
        self.assertIsNone(self.store.get("does-not-exist"))

    def test_get_by_chat_only_committed(self):
        """get_by_chat should only return committed messages."""
        committed = self._make_msg(status="committed")
        pending   = self._make_msg(status="pending")
        self.store.save(committed)
        self.store.save(pending)

        results = self.store.get_by_chat("room1")
        ids = [m["id"] for m in results]
        self.assertIn(committed["id"], ids)
        self.assertNotIn(pending["id"], ids)

    def test_get_by_chat_sorted_by_timestamp(self):
        """Messages in get_by_chat should be sorted ascending by timestamp."""
        m1 = self._make_msg(); m1["timestamp"] = 1000.0
        m2 = self._make_msg(); m2["timestamp"] = 2000.0
        m3 = self._make_msg(); m3["timestamp"] = 1500.0
        for m in [m2, m3, m1]:
            self.store.save(m)

        results = self.store.get_by_chat("room1")
        timestamps = [m["timestamp"] for m in results]
        self.assertEqual(timestamps, sorted(timestamps))

    def test_overwrite_with_save(self):
        """Saving a message with the same ID should overwrite it."""
        msg = self._make_msg(status="pending")
        self.store.save(msg)

        msg["status"] = "committed"
        self.store.save(msg)

        retrieved = self.store.get(msg["id"])
        self.assertEqual(retrieved["status"], "committed")

    def test_mark_committed(self):
        """mark_committed should change a pending message to committed."""
        msg = self._make_msg(status="pending")
        self.store.save(msg)
        self.store.mark_committed(msg["id"])
        self.assertEqual(self.store.get(msg["id"])["status"], "committed")

    def test_get_ids(self):
        """get_ids should return all stored IDs."""
        m1 = self._make_msg()
        m2 = self._make_msg()
        self.store.save(m1)
        self.store.save(m2)
        ids = self.store.get_ids()
        self.assertIn(m1["id"], ids)
        self.assertIn(m2["id"], ids)

    def test_get_all(self):
        """get_all should return every stored message."""
        m1 = self._make_msg()
        m2 = self._make_msg()
        self.store.save(m1)
        self.store.save(m2)
        all_msgs = self.store.get_all()
        self.assertEqual(len(all_msgs), 2)


# ─────────────────────────────────────────────────────────────────────────────
# Section 2: VectorClock unit tests
# ─────────────────────────────────────────────────────────────────────────────

class TestVectorClock(unittest.TestCase):
    """Unit tests for the VectorClock causal ordering tool."""

    def _make_clock(self, node_id, all_ids=None):
        return VectorClock(node_id, all_ids or [1, 2, 3])

    def test_tick_increments_own_counter(self):
        """tick() should increment only the owning node's counter."""
        vc = self._make_clock(1)
        snapshot = vc.tick()
        self.assertEqual(snapshot[1], 1)
        self.assertEqual(snapshot[2], 0)
        self.assertEqual(snapshot[3], 0)

    def test_tick_returns_snapshot(self):
        """tick() should return a copy, not the live clock."""
        vc = self._make_clock(1)
        snap1 = vc.tick()
        snap2 = vc.tick()
        self.assertEqual(snap1[1], 1)
        self.assertEqual(snap2[1], 2)

    def test_update_merges_and_increments(self):
        """update() should take element-wise max then increment own counter."""
        vc = self._make_clock(2)
        incoming = {1: 5, 2: 0, 3: 2}
        result = vc.update(incoming)
        # max(0, 5)=5 for node 1,  max(0,0)+1=1 for node 2, max(0,2)=2 for node 3
        self.assertEqual(result[1], 5)
        self.assertEqual(result[2], 1)
        self.assertEqual(result[3], 2)

    def test_happened_before(self):
        """A → B means A's clock is strictly dominated by B's."""
        a = {1: 1, 2: 0}
        b = {1: 1, 2: 1}
        self.assertTrue(VectorClock.happened_before(a, b))
        self.assertFalse(VectorClock.happened_before(b, a))

    def test_not_happened_before_equal(self):
        """Equal clocks do NOT satisfy happened-before."""
        a = {1: 1, 2: 1}
        self.assertFalse(VectorClock.happened_before(a, a))

    def test_concurrent(self):
        """Events on different partitions with no shared history are concurrent."""
        a = {1: 1, 2: 0}
        b = {1: 0, 2: 1}
        self.assertTrue(VectorClock.concurrent(a, b))

    def test_not_concurrent_when_causal(self):
        """Causal events should NOT be concurrent."""
        a = {1: 1, 2: 0}
        b = {1: 1, 2: 1}
        self.assertFalse(VectorClock.concurrent(a, b))


# ─────────────────────────────────────────────────────────────────────────────
# Section 3: ReplicationManager unit tests (no gRPC peers)
# ─────────────────────────────────────────────────────────────────────────────

class TestReplicationManager(unittest.TestCase):
    """Tests for ReplicationManager in standalone mode (no live peers)."""

    def _make_rm(self, node_id=1, peers=None):
        return ReplicationManager(
            node_id=node_id,
            peers=peers or [],
            all_node_ids=[1, 2, 3],
        )

    def test_create_message_fields(self):
        """create_message should produce all mandatory fields."""
        rm = self._make_rm()
        msg = rm.create_message("room1", "alice", "hi there")
        for field in ["id", "chat_id", "sender", "content",
                      "timestamp", "vector_clock", "status", "sender_id"]:
            self.assertIn(field, msg, f"Missing field: {field}")
        self.assertEqual(msg["chat_id"], "room1")
        self.assertEqual(msg["sender"], "alice")
        self.assertEqual(msg["content"], "hi there")
        self.assertEqual(msg["status"], "pending")

    def test_create_message_vector_clock_ticks(self):
        """Each new message should advance the vector clock for node 1."""
        rm = self._make_rm(node_id=1)
        m1 = rm.create_message("room1", "alice", "first")
        m2 = rm.create_message("room1", "alice", "second")
        self.assertGreater(m2["vector_clock"][1], m1["vector_clock"][1])

    def test_write_no_peers_commits_immediately(self):
        """With quorum_w=2 and no peers (only 1 node), write meets quorum when quorum_w=1."""
        rm = ReplicationManager(
            node_id=1, peers=[], all_node_ids=[1], quorum_w=1
        )
        msg = rm.create_message("room1", "alice", "hello")
        success = rm.write(msg)
        self.assertTrue(success)
        stored = rm.store.get(msg["id"])
        self.assertIsNotNone(stored)
        self.assertEqual(stored["status"], "committed")

    def test_write_duplicate_is_idempotent(self):
        """Writing the same message twice should succeed both times (dedup)."""
        rm = ReplicationManager(node_id=1, peers=[], all_node_ids=[1], quorum_w=1)
        msg = rm.create_message("room1", "alice", "hello")
        self.assertTrue(rm.write(msg))
        self.assertTrue(rm.write(msg))   # second write = duplicate → still True

    def test_receive_replica_stores_message(self):
        """receive_replica should save message to the local store."""
        rm = self._make_rm(node_id=2)
        incoming = {
            "id": str(uuid.uuid4()),
            "chat_id": "room1",
            "sender": "alice",
            "sender_id": 1,
            "content": "hello",
            "timestamp": time.time(),
            "vector_clock": {1: 1, 2: 0, 3: 0},
            "status": "committed",
        }
        rm.receive_replica(incoming)
        stored = rm.store.get(incoming["id"])
        self.assertIsNotNone(stored)

    def test_receive_replica_deduplication(self):
        """Receiving the same replica twice should not double-store it."""
        rm = self._make_rm(node_id=2)
        msg = {
            "id": str(uuid.uuid4()),
            "chat_id": "room1",
            "sender": "alice",
            "sender_id": 1,
            "content": "hi",
            "timestamp": time.time(),
            "vector_clock": {1: 1, 2: 0, 3: 0},
            "status": "committed",
        }
        rm.receive_replica(msg)
        rm.receive_replica(msg)
        all_msgs = rm.store.get_all()
        matching = [m for m in all_msgs if m["id"] == msg["id"]]
        self.assertEqual(len(matching), 1)

    def test_get_sync_state_returns_all(self):
        """get_sync_state should return every message (for anti-entropy)."""
        rm = ReplicationManager(node_id=1, peers=[], all_node_ids=[1], quorum_w=1)
        m1 = rm.create_message("room1", "alice", "a"); rm.write(m1)
        m2 = rm.create_message("room1", "alice", "b"); rm.write(m2)
        state = rm.get_sync_state()
        self.assertEqual(len(state), 2)

    def test_apply_sync_imports_new_messages(self):
        """apply_sync should import messages not already stored."""
        rm = self._make_rm(node_id=2)
        foreign_msg = {
            "id": str(uuid.uuid4()),
            "chat_id": "room1",
            "sender": "alice",
            "sender_id": 1,
            "content": "from node1",
            "timestamp": time.time(),
            "vector_clock": {1: 1, 2: 0, 3: 0},
            "status": "committed",
        }
        count = rm.apply_sync([foreign_msg])
        self.assertEqual(count, 1)
        self.assertIsNotNone(rm.store.get(foreign_msg["id"]))

    def test_apply_sync_skips_duplicates(self):
        """apply_sync should not re-import already-known messages."""
        rm = ReplicationManager(node_id=1, peers=[], all_node_ids=[1], quorum_w=1)
        msg = rm.create_message("room1", "alice", "hello")
        rm.write(msg)
        count = rm.apply_sync([msg])
        self.assertEqual(count, 0)   # already known

    def test_apply_committed_entry_stores_message(self):
        """apply_committed_entry (called by Raft) should store the entry."""
        rm = self._make_rm(node_id=1)
        entry_dict = {"term": 1, "message": "Alice: hello world"}
        rm.apply_committed_entry(entry_dict)
        all_msgs = rm.store.get_all()
        self.assertEqual(len(all_msgs), 1)
        self.assertIn("Alice: hello world", all_msgs[0]["content"])


# ─────────────────────────────────────────────────────────────────────────────
# Section 4: Integration with TimeSyncer + MessageReorderer
# ─────────────────────────────────────────────────────────────────────────────

class TestReplicationWithTimeSync(unittest.TestCase):
    """Tests that ReplicationManager wires correctly with TimeSyncer."""

    def _make_rm(self, node_id):
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

    def test_create_message_uses_lamport(self):
        """Messages should carry a lamport_time field when TimeSyncer is present."""
        rm, _, _ = self._make_rm(1)
        msg = rm.create_message("room1", "alice", "hi")
        self.assertIn("lamport_time", msg)
        self.assertGreater(msg["lamport_time"], 0)

    def test_lamport_increments_across_messages(self):
        """Successive messages must have strictly increasing Lamport times."""
        rm, _, _ = self._make_rm(1)
        times = [rm.create_message("room1", "alice", f"msg{i}")["lamport_time"]
                 for i in range(5)]
        for i in range(1, len(times)):
            self.assertGreater(times[i], times[i - 1])

    def test_receive_replica_updates_lamport(self):
        """Receiving a replica with a high lamport_time should advance local clock."""
        rm, ts, _ = self._make_rm(2)
        incoming = {
            "id": str(uuid.uuid4()),
            "chat_id": "room1",
            "sender": "alice",
            "sender_id": 1,
            "content": "high clock msg",
            "timestamp": time.time(),
            "lamport_time": 99,
            "vector_clock": {1: 1, 2: 0, 3: 0},
            "status": "committed",
        }
        rm.receive_replica(incoming)
        self.assertGreaterEqual(ts.lamport.get(), 99)

    def test_adjusted_timestamp_uses_offset(self):
        """Timestamps should reflect offset corrections via TimeSyncer."""
        rm, ts, _ = self._make_rm(1)
        ts._add_sample(0.100)   # pretend local clock is 100 ms behind reference
        before = time.time()
        msg = rm.create_message("room1", "alice", "offset test")
        # Adjusted timestamp should be ~100 ms ahead of raw wall clock
        self.assertAlmostEqual(msg["timestamp"] - before, 0.100, places=1)


# ─────────────────────────────────────────────────────────────────────────────
# Section 5: Consensus ↔ Replication interface contract
# ─────────────────────────────────────────────────────────────────────────────

class TestConsensusReplicationInterface(unittest.TestCase):
    """Verify that apply_committed_entry satisfies the Raft → Replication contract."""

    def _make_rm_with_consensus(self, node_id=1):
        from node.consensus import RaftNode
        rm = ReplicationManager(
            node_id=node_id,
            peers=[],
            all_node_ids=[1, 2, 3],
        )
        raft = RaftNode(node_id=node_id, peers=[], replication=rm)
        return rm, raft

    def test_raft_commit_applies_to_replication(self):
        """A message committed by Raft should appear in the replication store."""
        rm, raft = self._make_rm_with_consensus()
        raft.start_election()          # become leader (only node in cluster)
        self.assertTrue(raft.is_leader())
        committed = raft.receive_client_message("Alice: hello")
        self.assertTrue(committed)
        all_msgs = rm.store.get_all()
        self.assertEqual(len(all_msgs), 1)
        self.assertIn("Alice: hello", all_msgs[0]["content"])

    def test_multiple_raft_entries_all_applied(self):
        """All committed Raft log entries should end up in the replication store."""
        rm, raft = self._make_rm_with_consensus()
        raft.start_election()
        for i in range(5):
            raft.receive_client_message(f"Message {i}")
        self.assertEqual(len(rm.store.get_all()), 5)

    def test_raft_is_leader_check(self):
        """Replication can use consensus.is_leader() to gate writes."""
        rm, raft = self._make_rm_with_consensus()
        # Before election, no one is leader
        self.assertFalse(raft.is_leader())
        # After a solo election, this node becomes leader
        raft.start_election()
        self.assertTrue(raft.is_leader())


if __name__ == "__main__":
    unittest.main(verbosity=2)
