import json
import os
import threading
import time
import uuid
from typing import Callable, Dict, List, Optional


Message = Dict[str, object]
HeartbeatFunction = Callable[[str], bool]
ReplicateFunction = Callable[[str, Message], bool]
FetchMessagesFunction = Callable[[str], List[Message]]


class PersistentMessageStore:
    """
    Local persistent store for fault tolerance.
    Stores messages in JSON and prevents duplicates by message_id.
    """

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.lock = threading.Lock()

        directory = os.path.dirname(file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        if not os.path.exists(self.file_path):
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump([], f)

    def _read_all(self) -> List[Message]:
        with open(self.file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_all(self, messages: List[Message]) -> None:
        with open(self.file_path, "w", encoding="utf-8") as f:
            json.dump(messages, f, indent=2)

    def save_message(self, message: Message) -> bool:
        """
        Save a message only if its message_id is new.
        Returns True if inserted, False if duplicate.
        """
        with self.lock:
            messages = self._read_all()
            existing_ids = {m["message_id"] for m in messages}

            if message["message_id"] in existing_ids:
                return False

            messages.append(message)
            self._write_all(messages)
            return True

    def merge_messages(self, incoming_messages: List[Message]) -> int:
        """
        Merge peer messages into local store.
        Returns number of newly inserted messages.
        """
        with self.lock:
            messages = self._read_all()
            existing_ids = {m["message_id"] for m in messages}
            added = 0

            for msg in incoming_messages:
                if msg["message_id"] not in existing_ids:
                    messages.append(msg)
                    existing_ids.add(msg["message_id"])
                    added += 1

            self._write_all(messages)
            return added

    def get_all_messages(self) -> List[Message]:
        with self.lock:
            return self._read_all()

    def count(self) -> int:
        with self.lock:
            return len(self._read_all())


class FailureDetector:
    """
    Periodically checks whether peer nodes are alive using a heartbeat callback.
    """

    def __init__(
        self,
        peers: List[str],
        heartbeat_fn: HeartbeatFunction,
        interval_seconds: float = 3.0
    ):
        self.peers = peers
        self.heartbeat_fn = heartbeat_fn
        self.interval_seconds = interval_seconds

        self._status = {peer: False for peer in peers}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=1.0)

    def _monitor_loop(self) -> None:
        while self._running:
            for peer in self.peers:
                alive = False
                try:
                    alive = self.heartbeat_fn(peer)
                except Exception:
                    alive = False

                with self._lock:
                    self._status[peer] = alive

            time.sleep(self.interval_seconds)

    def get_status(self) -> Dict[str, bool]:
        with self._lock:
            return dict(self._status)

    def get_live_peers(self) -> List[str]:
        with self._lock:
            return [peer for peer, alive in self._status.items() if alive]


class FaultToleranceManager:
    """
    Main fault tolerance module for:
    - message redundancy
    - failure detection
    - automatic failover support
    - node recovery
    - redundancy overhead metrics
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        heartbeat_fn: HeartbeatFunction,
        replicate_fn: ReplicateFunction,
        fetch_messages_fn: FetchMessagesFunction,
        store_path: str = "node/data/messages.json",
        replication_factor: int = 2
    ):
        self.node_id = node_id
        self.peers = peers
        self.replication_factor = max(1, replication_factor)
        self.store = PersistentMessageStore(store_path)

        self.heartbeat_fn = heartbeat_fn
        self.replicate_fn = replicate_fn
        self.fetch_messages_fn = fetch_messages_fn

        self.detector = FailureDetector(peers, heartbeat_fn)

        self.metrics = {
            "messages_received_from_clients": 0,
            "messages_stored_locally": 0,
            "messages_replicated_to_peers": 0,
            "replication_failures": 0,
            "duplicates_ignored": 0,
            "recovered_messages": 0
        }

    def start(self) -> None:
        """
        Start monitoring peers and recover missing data on startup.
        """
        self.detector.start()

        # Small delay helps if peers are also starting up.
        time.sleep(1.0)
        recovered = self.recover_from_peers()
        self.metrics["recovered_messages"] += recovered

    def stop(self) -> None:
        self.detector.stop()

    def build_message(
        self,
        sender: str,
        receiver: str,
        content: str,
        timestamp: Optional[float] = None
    ) -> Message:
        return {
            "message_id": str(uuid.uuid4()),
            "sender": sender,
            "receiver": receiver,
            "content": content,
            "timestamp": timestamp if timestamp is not None else time.time(),
            "origin_node": self.node_id
        }

    def handle_client_message(self, message: Message) -> Dict[str, object]:
        """
        Called when this node receives a new client message.
        Save locally, then replicate to live peers.
        """
        self.metrics["messages_received_from_clients"] += 1

        inserted = self.store.save_message(message)
        if inserted:
            self.metrics["messages_stored_locally"] += 1
        else:
            self.metrics["duplicates_ignored"] += 1

        replicated_count = self._replicate_to_live_peers(message)

        return {
            "status": "stored",
            "node_id": self.node_id,
            "replicated_to": replicated_count,
            "message_id": message["message_id"]
        }

    def handle_replica_message(self, message: Message) -> Dict[str, object]:
        """
        Called when this node receives a replica from another node.
        """
        inserted = self.store.save_message(message)

        if inserted:
            self.metrics["messages_stored_locally"] += 1
            status = "replica_stored"
        else:
            self.metrics["duplicates_ignored"] += 1
            status = "duplicate_ignored"

        return {
            "status": status,
            "node_id": self.node_id,
            "message_id": message["message_id"]
        }

    def _replicate_to_live_peers(self, message: Message) -> int:
        live_peers = self.detector.get_live_peers()
        target_count = max(0, self.replication_factor - 1)

        successful_replications = 0

        for peer in live_peers:
            if successful_replications >= target_count:
                break

            try:
                ok = self.replicate_fn(peer, message)
                if ok:
                    successful_replications += 1
                    self.metrics["messages_replicated_to_peers"] += 1
                else:
                    self.metrics["replication_failures"] += 1
            except Exception:
                self.metrics["replication_failures"] += 1

        return successful_replications

    def recover_from_peers(self) -> int:
        """
        On restart, ask peers for all stored messages and merge missing ones.
        """
        recovered_count = 0

        for peer in self.peers:
            try:
                peer_messages = self.fetch_messages_fn(peer)
                if peer_messages:
                    recovered_count += self.store.merge_messages(peer_messages)
            except Exception:
                continue

        return recovered_count

    def export_messages(self) -> List[Message]:
        return self.store.get_all_messages()

    def get_peer_status(self) -> Dict[str, bool]:
        return self.detector.get_status()

    def get_metrics(self) -> Dict[str, object]:
        local_count = self.store.count()

        return {
            "node_id": self.node_id,
            "local_message_count": local_count,
            "replication_factor": self.replication_factor,
            "estimated_storage_overhead_multiplier": self.replication_factor,
            "metrics": dict(self.metrics),
            "peer_status": self.detector.get_status()
        }
