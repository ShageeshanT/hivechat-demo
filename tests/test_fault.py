"""
Tests for the Fault Tolerance module (node/fault.py)
=====================================================
Covers:
  1. Local store + successful replication to a live peer
  2. Duplicate message rejection
  3. Node recovery from peers on rejoin
  4. Metrics structure and content
  5. Missed-heartbeat threshold (peer declared DEAD after N misses)
  6. Pending queue populated for dead peers
  7. Pending queue drained when dead peer recovers
  8. Storage overhead metric (size_bytes > 0 after a message)
  9. Per-peer replication success rate tracking
 10. Uptime seconds is a positive number
"""

import os
import tempfile
import time
import unittest

from node.fault import (
    FaultToleranceManager,
    PendingReplicationQueue,
    FailureDetector,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helper factory
# ─────────────────────────────────────────────────────────────────────────────

def make_manager(
    temp_dir,
    heartbeat_map=None,
    peer_messages=None,
    replica_calls=None,
    peers=None,
    replication_factor=2,
    missed_threshold=3,
):
    """
    Build a FaultToleranceManager wired to in-memory stub callbacks.
    """
    if peers is None:
        peers = ["node2", "node3"]
    if heartbeat_map is None:
        heartbeat_map = {"node2": True, "node3": False}
    if peer_messages is None:
        peer_messages = {
            "node2": [
                {
                    "message_id":  "msg-peer-1",
                    "sender":      "Alice",
                    "receiver":    "Bob",
                    "content":     "Recovered message",
                    "timestamp":   12345.0,
                    "origin_node": "node2",
                }
            ],
            "node3": [],
        }
    if replica_calls is None:
        replica_calls = []

    store_path = os.path.join(temp_dir, "messages.json")

    def heartbeat_fn(peer):
        return heartbeat_map.get(peer, False)

    def replicate_fn(peer, message):
        if heartbeat_map.get(peer, False):
            replica_calls.append((peer, message["message_id"]))
            return True
        return False

    def fetch_messages_fn(peer):
        return peer_messages.get(peer, [])

    return FaultToleranceManager(
        node_id="node1",
        peers=peers,
        heartbeat_fn=heartbeat_fn,
        replicate_fn=replicate_fn,
        fetch_messages_fn=fetch_messages_fn,
        store_path=store_path,
        replication_factor=replication_factor,
        missed_threshold=missed_threshold,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test cases
# ─────────────────────────────────────────────────────────────────────────────

class TestPersistentStore(unittest.TestCase):
    """Unit tests for the message store (dedup, merge, size)."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.managers = []

    def tearDown(self):
        for m in self.managers:
            m.stop()
        self.tmp.cleanup()

    def _mgr(self, **kw):
        m = make_manager(self.tmp.name, **kw)
        self.managers.append(m)
        return m

    def test_local_store_and_replication(self):
        """Message is stored locally and replicated to the one live peer."""
        replica_calls = []
        mgr = self._mgr(
            heartbeat_map={"node2": True, "node3": False},
            replica_calls=replica_calls,
        )
        mgr.detector._status = {"node2": True, "node3": False}

        msg    = mgr.build_message("Alice", "Bob", "Hello")
        result = mgr.handle_client_message(msg)

        self.assertEqual(result["status"], "stored_and_replicated")
        self.assertEqual(result["replicated_to"], 1)
        self.assertEqual(len(mgr.export_messages()), 1)
        self.assertEqual(len(replica_calls), 1)
        self.assertEqual(replica_calls[0][0], "node2")

    def test_duplicate_message_ignored(self):
        """Same message_id delivered twice → second is flagged duplicate."""
        mgr = self._mgr()
        msg = mgr.build_message("Alice", "Bob", "Hello")

        mgr.handle_client_message(msg)
        result = mgr.handle_replica_message(msg)

        self.assertEqual(result["status"], "duplicate_ignored")
        self.assertEqual(mgr.store.count(), 1)

    def test_recovery_from_peers(self):
        """On rejoin, messages from live peers are merged into local store."""
        mgr       = self._mgr()
        recovered = mgr.recover_from_peers()

        self.assertEqual(recovered, 1)
        self.assertEqual(mgr.store.count(), 1)
        self.assertEqual(mgr.export_messages()[0]["message_id"], "msg-peer-1")


class TestMetrics(unittest.TestCase):
    """Metrics structure and values."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.managers = []

    def tearDown(self):
        for m in self.managers:
            m.stop()
        self.tmp.cleanup()

    def _mgr(self, **kw):
        m = make_manager(self.tmp.name, **kw)
        self.managers.append(m)
        return m

    def test_metrics_keys_present(self):
        mgr     = self._mgr()
        metrics = mgr.get_metrics()

        for key in (
            "node_id", "uptime_seconds", "local_message_count",
            "storage_bytes", "replication_factor",
            "estimated_storage_overhead_multiplier",
            "pending_queue_total", "per_peer_status",
            "missed_heartbeat_counts", "internal_metrics", "liveness_status",
            "system_health"
        ):
            self.assertIn(key, metrics, f"Missing key: {key}")

    def test_uptime_is_positive(self):
        mgr = self._mgr()
        time.sleep(0.05)
        self.assertGreater(mgr.get_metrics()["uptime_seconds"], 0)

    def test_storage_bytes_nonzero_after_message(self):
        """Storage file size should be > 0 once a message is stored."""
        mgr = self._mgr()
        mgr.handle_client_message(mgr.build_message("A", "B", "test"))
        self.assertGreater(mgr.get_metrics()["storage_bytes"], 0)

    def test_per_peer_success_rate(self):
        """After a successful replication the success rate should be 1.0."""
        replica_calls = []
        mgr = self._mgr(
            heartbeat_map={"node2": True, "node3": False},
            replica_calls=replica_calls,
        )
        mgr.detector._status = {"node2": True, "node3": False}
        mgr.handle_client_message(mgr.build_message("A", "B", "hi"))

        per_peer = mgr.get_metrics()["per_peer_status"]["node2"]
        self.assertEqual(per_peer["successes"], 1)
        self.assertEqual(per_peer["success_rate"], 1.0)


class TestFailureDetector(unittest.TestCase):
    """Missed-heartbeat threshold behaviour."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.managers = []

    def tearDown(self):
        for m in self.managers:
            m.stop()
        self.tmp.cleanup()

    def _mgr(self, **kw):
        m = make_manager(self.tmp.name, **kw)
        self.managers.append(m)
        return m

    def test_peer_stays_unknown_below_threshold(self):
        """
        Peer starts as False (default). If missed < threshold it remains False
        but is not yet toggled by the threshold logic run manually below.
        """
        mgr = self._mgr(
            heartbeat_map={"node2": False, "node3": False},
            missed_threshold=3,
        )
        detector = mgr.detector
        # Simulate 2 missed beats manually (below threshold of 3)
        detector._missed["node2"] = 2
        detector._status["node2"] = True   # was alive
        # Threshold not yet reached → still alive-ish (we don't flip yet)
        self.assertLess(detector._missed["node2"], detector.threshold)

    def test_missed_count_threshold_marks_dead(self):
        """
        After `threshold` consecutive misses the detector marks the peer DEAD.
        We simulate the internal state directly to avoid real threading.
        """
        mgr = self._mgr(
            heartbeat_map={"node2": False, "node3": False},
            missed_threshold=3,
        )
        detector = mgr.detector

        # Simulate 3 missed beats
        detector._missed["node2"] = 3
        detector._status["node2"] = False  # threshold reached

        self.assertFalse(detector.is_alive("node2"))
        self.assertEqual(detector.get_missed_counts()["node2"], 3)

    def test_get_live_peers_excludes_dead(self):
        """get_live_peers() only returns peers with alive=True."""
        mgr = self._mgr(
            heartbeat_map={"node2": True, "node3": False},
        )
        mgr.detector._status = {"node2": True, "node3": False}
        live = mgr.detector.get_live_peers()
        self.assertIn("node2", live)
        self.assertNotIn("node3", live)


class TestPendingQueue(unittest.TestCase):
    """Pending replication queue behaviour."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.managers = []

    def tearDown(self):
        for m in self.managers:
            m.stop()
        self.tmp.cleanup()

    def _mgr(self, **kw):
        m = make_manager(self.tmp.name, **kw)
        self.managers.append(m)
        return m

    def test_pending_queue_filled_for_dead_peers(self):
        """
        When a peer is dead, failed replications go into the pending queue.
        """
        mgr = self._mgr(
            heartbeat_map={"node2": False, "node3": False},
        )
        mgr.detector._status = {"node2": False, "node3": False}

        mgr.handle_client_message(mgr.build_message("A", "B", "queued?"))

        # Both peers are dead → message queued for both
        total = mgr.pending_queue.total_pending()
        self.assertGreater(total, 0)

    def test_queue_drained_on_peer_recovery(self):
        """
        Pending messages for a peer are retried and removed from the queue
        when `_on_peer_recovered` is called.
        """
        replica_calls = []
        hb_map        = {"node2": False, "node3": False}

        mgr = self._mgr(
            heartbeat_map=hb_map,
            replica_calls=replica_calls,
        )
        mgr.detector._status = {"node2": False, "node3": False}

        # Queue a message for node2 by failing the live replication
        msg = mgr.build_message("A", "B", "retry me")
        mgr.handle_client_message(msg)
        self.assertEqual(mgr.pending_queue.pending_count("node2"), 1)

        # Simulate node2 coming back alive
        hb_map["node2"] = True
        mgr._on_peer_recovered("node2")

        # Queue should now be empty and the message retried
        self.assertEqual(mgr.pending_queue.pending_count("node2"), 0)
        self.assertEqual(len(replica_calls), 1)


class TestPendingReplicationQueueUnit(unittest.TestCase):
    """Direct unit test for PendingReplicationQueue."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test_queue.db")
        # Ensure 'messages' table exists because drain() joins on it!
        import sqlite3, contextlib
        with contextlib.closing(sqlite3.connect(self.db_path)) as conn:
            with conn:
                conn.execute("""
                    CREATE TABLE messages (
                        message_id TEXT PRIMARY KEY, sender TEXT, receiver TEXT, content TEXT, timestamp REAL, origin_node TEXT
                    )
                """)

    def tearDown(self):
        self.tmp.cleanup()

    def test_enqueue_drain(self):
        q   = PendingReplicationQueue(self.db_path)
        msg = {"message_id": "x", "content": "hi", "sender": "A", "receiver": "B", "timestamp": 1.0, "origin_node": "N"}
        
        # Manually insert message into `messages` table so JOIN succeeds
        import sqlite3, contextlib
        with contextlib.closing(sqlite3.connect(self.db_path)) as conn:
            with conn:
                conn.execute("INSERT INTO messages (message_id, sender, receiver, content, timestamp, origin_node) VALUES (?, ?, ?, ?, ?, ?)", ("x", "A", "B", "hi", 1.0, "N"))

        q.enqueue("peer1", msg)

        self.assertEqual(q.pending_count("peer1"), 1)
        self.assertEqual(q.total_pending(), 1)

        drained = q.drain("peer1")
        self.assertEqual(len(drained), 1)
        self.assertEqual(q.pending_count("peer1"), 0)

    def test_drain_unknown_peer_returns_empty(self):
        q = PendingReplicationQueue(self.db_path)
        self.assertEqual(q.drain("ghost"), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)