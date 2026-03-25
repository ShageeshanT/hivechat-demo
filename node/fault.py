"""
HiveChat – Fault Tolerance Module
==================================
Responsibilities (Member 2 – Sihan, IT24103532):
  1. Persistent message store  – SQLite-backed, dedup by message_id
  2. Failure detector          – Parallel heartbeat loop with latency tracking
  3. Pending replication queue – retry failed replications when peer recovers
  4. FaultToleranceManager     – orchestrates all three sub-systems and exposes
                                 detailed metrics (latency, storage, success rates)
"""

import json
import os
import sqlite3
import threading
import time
import uuid
import logging
import contextlib
from concurrent import futures
from typing import Callable, Dict, List, Optional


# ── logger setup ─────────────────────────────────────────────────────────────

logger = logging.getLogger("HiveChatFault")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("[%(levelname)s] [%(name)s] %(message)s"))
    logger.addHandler(ch)


# ── Type aliases ─────────────────────────────────────────────────────────────

Message             = Dict[str, object]
HeartbeatFunction   = Callable[[str], bool]
ReplicateFunction   = Callable[[str, Message], bool]
FetchMessagesFunction = Callable[[str], List[Message]]


# ─────────────────────────────────────────────────────────────────────────────
# 1. Persistent Message Store
# ─────────────────────────────────────────────────────────────────────────────

class PersistentMessageStore:
    """
    Local persistent store for fault tolerance.
    Uses SQLite for performance and reliability (atomic ACID).
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = threading.Lock()

        directory = os.path.dirname(db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)

        # ensure table exists
        with contextlib.closing(sqlite3.connect(self.db_path)) as conn:
            with conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        message_id TEXT PRIMARY KEY,
                        sender TEXT,
                        receiver TEXT,
                        content TEXT,
                        timestamp REAL,
                        origin_node TEXT
                    )
                """)

    def save_message(self, message: Message) -> bool:
        """
        Persist a message only if its message_id has not been seen before.
        Returns True if inserted, False if duplicate.
        """
        try:
            with self.lock, contextlib.closing(sqlite3.connect(self.db_path)) as conn:
                with conn:
                    conn.execute(
                        "INSERT INTO messages VALUES (?,?,?,?,?,?)",
                        (
                            message["message_id"],
                            message["sender"],
                            message["receiver"],
                            message["content"],
                            message["timestamp"],
                            message["origin_node"]
                        )
                    )
                return True
        except sqlite3.IntegrityError:
            return False

    def merge_messages(self, incoming: List[Message]) -> int:
        """
        Merge messages fetched from a peer into the local store.
        Returns the number of newly inserted messages.
        """
        added = 0
        with self.lock, contextlib.closing(sqlite3.connect(self.db_path)) as conn:
            with conn:
                for msg in incoming:
                    try:
                        conn.execute(
                            "INSERT INTO messages VALUES (?,?,?,?,?,?)",
                            (
                                msg["message_id"],
                                msg["sender"],
                                msg["receiver"],
                                msg["content"],
                                msg["timestamp"],
                                msg["origin_node"]
                            )
                        )
                        added += 1
                    except sqlite3.IntegrityError:
                        continue
        return added

    def get_all_messages(self) -> List[Message]:
        with self.lock, contextlib.closing(sqlite3.connect(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM messages")
            return [dict(row) for row in cursor.fetchall()]

    def count(self) -> int:
        with self.lock, contextlib.closing(sqlite3.connect(self.db_path)) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM messages")
            return cursor.fetchone()[0]

    def size_bytes(self) -> int:
        try:
            return os.path.getsize(self.db_path)
        except OSError:
            return 0


# ─────────────────────────────────────────────────────────────────────────────
# 2. Failure Detector  (missed-count threshold model)
# ─────────────────────────────────────────────────────────────────────────────

class FailureDetector:
    """
    Periodically pings every peer via heartbeat_fn.

    A peer is declared DEAD only after it misses `threshold` consecutive
    heartbeats (default = 3).
    """

    def __init__(
        self,
        peers: List[str],
        heartbeat_fn: HeartbeatFunction,
        interval_seconds: float = 3.0,
        threshold: int = 3,
        on_peer_recovered: Optional[Callable[[str], None]] = None,
    ):
        self.peers = peers
        self.heartbeat_fn = heartbeat_fn
        self.interval_seconds = interval_seconds
        self.threshold = threshold
        self.on_peer_recovered = on_peer_recovered

        self._missed: Dict[str, int] = {p: 0 for p in peers}
        self._status: Dict[str, bool] = {p: False for p in peers}
        self._latencies: Dict[str, float] = {p: 0.0 for p in peers}

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
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _ping_peer(self, peer: str) -> None:
        start_time = time.time()
        try:
            responded = self.heartbeat_fn(peer)
        except Exception:
            responded = False
        latency = time.time() - start_time

        with self._lock:
            was_alive = self._status[peer]
            if responded:
                self._missed[peer] = 0
                self._status[peer] = True
                self._latencies[peer] = round(latency, 4)
                if not was_alive and self.on_peer_recovered:
                    threading.Thread(
                        target=self.on_peer_recovered,
                        args=(peer,),
                        daemon=True
                    ).start()
            else:
                self._missed[peer] += 1
                if self._missed[peer] >= self.threshold:
                    self._status[peer] = False
                    self._latencies[peer] = 0.0

    def _monitor_loop(self) -> None:
        with futures.ThreadPoolExecutor(max_workers=len(self.peers) or 1) as executor:
            while self._running:
                list(executor.map(self._ping_peer, self.peers))
                time.sleep(self.interval_seconds)

    def get_status(self) -> Dict[str, bool]:
        with self._lock:
            return dict(self._status)

    def get_missed_counts(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._missed)

    def get_live_peers(self) -> List[str]:
        with self._lock:
            return [p for p, alive in self._status.items() if alive]

    def is_alive(self, peer: str) -> bool:
        with self._lock:
            return self._status.get(peer, False)

    def get_latencies(self) -> Dict[str, float]:
        with self._lock:
            return dict(self._latencies)


# ─────────────────────────────────────────────────────────────────────────────
# 3. Pending Replication Queue
# ─────────────────────────────────────────────────────────────────────────────

class PendingReplicationQueue:
    def __init__(self):
        self._queue: Dict[str, List[Message]] = {}
        self._lock = threading.Lock()

    def enqueue(self, peer: str, message: Message) -> None:
        with self._lock:
            self._queue.setdefault(peer, []).append(message)

    def drain(self, peer: str) -> List[Message]:
        with self._lock:
            return self._queue.pop(peer, [])

    def pending_count(self, peer: str) -> int:
        with self._lock:
            return len(self._queue.get(peer, []))

    def total_pending(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._queue.values())


# ─────────────────────────────────────────────────────────────────────────────
# 4. Fault Tolerance Manager
# ─────────────────────────────────────────────────────────────────────────────

class FaultToleranceManager:
    def __init__(
        self,
        node_id: str,
        peers: List[str],
        heartbeat_fn: HeartbeatFunction,
        replicate_fn: ReplicateFunction,
        fetch_messages_fn: FetchMessagesFunction,
        store_path: str = "node/data/messages.db",
        replication_factor: int = 2,
        heartbeat_interval: float = 3.0,
        missed_threshold: int = 3,
    ):
        self.node_id = node_id
        self.peers = peers
        self.replication_factor = max(1, replication_factor)
        
        # ensure .db extension if passed as .json
        if store_path.endswith(".json"):
            store_path = store_path.replace(".json", ".db")
            
        self.store = PersistentMessageStore(store_path)
        self._start_time = time.time()

        self.heartbeat_fn = heartbeat_fn
        self.replicate_fn = replicate_fn
        self.fetch_messages_fn = fetch_messages_fn

        self.pending_queue = PendingReplicationQueue()

        self.detector = FailureDetector(
            peers=peers,
            heartbeat_fn=heartbeat_fn,
            interval_seconds=heartbeat_interval,
            threshold=missed_threshold,
            on_peer_recovered=self._on_peer_recovered,
        )

        self._peer_successes: Dict[str, int] = {p: 0 for p in peers}
        self._peer_failures:  Dict[str, int] = {p: 0 for p in peers}
        self._stats_lock = threading.Lock()

        self.metrics = {
            "messages_received_from_clients": 0,
            "messages_stored_locally":        0,
            "messages_replicated_to_peers":   0,
            "replication_failures":           0,
            "duplicates_ignored":             0,
            "recovered_messages":             0,
            "pending_retried":                0,
        }

    def start(self) -> None:
        self.detector.start()
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
        timestamp: Optional[float] = None,
    ) -> Message:
        return {
            "message_id":   str(uuid.uuid4()),
            "sender":       sender,
            "receiver":     receiver,
            "content":      content,
            "timestamp":    timestamp if timestamp is not None else time.time(),
            "origin_node":  self.node_id,
        }

    def handle_client_message(self, message: Message) -> Dict[str, object]:
        self.metrics["messages_received_from_clients"] += 1
        inserted = self.store.save_message(message)
        if inserted:
            self.metrics["messages_stored_locally"] += 1
        else:
            self.metrics["duplicates_ignored"] += 1

        replicated_count = self._replicate_to_live_peers(message)
        return {
            "status":        "stored",
            "node_id":       self.node_id,
            "replicated_to": replicated_count,
            "message_id":    message["message_id"],
        }

    def handle_replica_message(self, message: Message) -> Dict[str, object]:
        inserted = self.store.save_message(message)
        if inserted:
            self.metrics["messages_stored_locally"] += 1
            status = "replica_stored"
        else:
            self.metrics["duplicates_ignored"] += 1
            status = "duplicate_ignored"

        return {
            "status":     status,
            "node_id":    self.node_id,
            "message_id": message["message_id"],
        }

    def _replicate_to_live_peers(self, message: Message) -> int:
        live_peers   = self.detector.get_live_peers()
        needed       = max(0, self.replication_factor - 1)
        successful   = 0

        for peer in live_peers:
            if successful >= needed:
                break
            ok = self._try_replicate(peer, message)
            if ok:
                successful += 1

        dead_peers = [p for p in self.peers if not self.detector.is_alive(p)]
        for peer in dead_peers:
            self.pending_queue.enqueue(peer, message)

        return successful

    def _try_replicate(self, peer: str, message: Message) -> bool:
        try:
            ok = self.replicate_fn(peer, message)
            with self._stats_lock:
                if ok:
                    self._peer_successes[peer] = self._peer_successes.get(peer, 0) + 1
                    self.metrics["messages_replicated_to_peers"] += 1
                else:
                    self._peer_failures[peer] = self._peer_failures.get(peer, 0) + 1
                    self.metrics["replication_failures"] += 1
            return ok
        except Exception:
            with self._stats_lock:
                self._peer_failures[peer] = self._peer_failures.get(peer, 0) + 1
                self.metrics["replication_failures"] += 1
            return False

    def recover_from_peers(self) -> int:
        recovered = 0
        for peer in self.peers:
            try:
                peer_messages = self.fetch_messages_fn(peer)
                if peer_messages:
                    recovered += self.store.merge_messages(peer_messages)
            except Exception:
                continue
        return recovered

    def _on_peer_recovered(self, peer: str) -> None:
        pending = self.pending_queue.drain(peer)
        retried = 0
        for message in pending:
            if self._try_replicate(peer, message):
                retried += 1
        if retried:
            self.metrics["pending_retried"] += retried
            logger.info(f"Peer {peer} recovered -> retried {retried} queued message(s).")

    def export_messages(self) -> List[Message]:
        return self.store.get_all_messages()

    def get_peer_status(self) -> Dict[str, bool]:
        return self.detector.get_status()

    def get_metrics(self) -> Dict[str, object]:
        local_count   = self.store.count()
        storage_bytes = self.store.size_bytes()
        uptime        = round(time.time() - self._start_time, 1)

        with self._stats_lock:
            per_peer = {}
            for peer in self.peers:
                s = self._peer_successes.get(peer, 0)
                f = self._peer_failures.get(peer, 0)
                total = s + f
                per_peer[peer] = {
                    "successes": s,
                    "failures":  f,
                    "success_rate": round(s / total, 3) if total else None,
                }

        return {
            "node_id":                        self.node_id,
            "uptime_seconds":                 uptime,
            "local_message_count":            local_count,
            "storage_bytes":                  storage_bytes,
            "replication_factor":             self.replication_factor,
            "estimated_storage_overhead_multiplier": self.replication_factor,
            "pending_queue_total":            self.pending_queue.total_pending(),
            "peer_latencies_seconds":         self.detector.get_latencies(),
            "per_peer_replication":           per_peer,
            "missed_heartbeat_counts":        self.detector.get_missed_counts(),
            "metrics":                        dict(self.metrics),
            "peer_status":                    self.detector.get_status(),
        }
