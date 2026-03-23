import os
import tempfile
import unittest

from node.fault import FaultToleranceManager


class TestFaultToleranceManager(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store_path = os.path.join(self.temp_dir.name, "messages.json")

        self.replica_calls = []
        self.heartbeat_map = {
            "node2": True,
            "node3": False,
        }
        self.peer_messages = {
            "node2": [
                {
                    "message_id": "msg-peer-1",
                    "sender": "Alice",
                    "receiver": "Bob",
                    "content": "Recovered message",
                    "timestamp": 12345.0,
                    "origin_node": "node2"
                }
            ],
            "node3": []
        }

        def heartbeat_fn(peer):
            return self.heartbeat_map.get(peer, False)

        def replicate_fn(peer, message):
            if self.heartbeat_map.get(peer, False):
                self.replica_calls.append((peer, message["message_id"]))
                return True
            return False

        def fetch_messages_fn(peer):
            return self.peer_messages.get(peer, [])

        self.manager = FaultToleranceManager(
            node_id="node1",
            peers=["node2", "node3"],
            heartbeat_fn=heartbeat_fn,
            replicate_fn=replicate_fn,
            fetch_messages_fn=fetch_messages_fn,
            store_path=self.store_path,
            replication_factor=2
        )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_local_store_and_replication(self):
        self.manager.detector._status = {"node2": True, "node3": False}

        msg = self.manager.build_message("Alice", "Bob", "Hello")
        result = self.manager.handle_client_message(msg)

        self.assertEqual(result["status"], "stored")
        self.assertEqual(result["replicated_to"], 1)
        self.assertEqual(len(self.manager.export_messages()), 1)
        self.assertEqual(len(self.replica_calls), 1)

    def test_duplicate_message_ignored(self):
        msg = self.manager.build_message("Alice", "Bob", "Hello")

        self.manager.handle_client_message(msg)
        result = self.manager.handle_replica_message(msg)

        self.assertEqual(result["status"], "duplicate_ignored")
        self.assertEqual(self.manager.store.count(), 1)

    def test_recovery_from_peers(self):
        recovered = self.manager.recover_from_peers()

        self.assertEqual(recovered, 1)
        self.assertEqual(self.manager.store.count(), 1)

    def test_metrics_exist(self):
        metrics = self.manager.get_metrics()

        self.assertIn("node_id", metrics)
        self.assertIn("local_message_count", metrics)
        self.assertIn("metrics", metrics)
        self.assertIn("peer_status", metrics)


if __name__ == "__main__":
    unittest.main()