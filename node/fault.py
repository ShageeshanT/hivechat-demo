"""
HiveChat – Fault Tolerance Module
==================================
Responsibilities (Member 2 – Sihan, IT24103532):
  1. Persistent message store  – JSON-backed, dedup by message_id
  2. Failure detector          – threaded heartbeat loop, missed-count threshold
  3. Pending replication queue – retry failed replications when peer recovers
  4. FaultToleranceManager     – orchestrates all three sub-systems and exposes
                                 metrics for evaluation
"""

import json
import os
import threading
import time
import uuid
from typing import Callable, Dict, List, Optional


# ─────────────────────────────────────────────────────────────────────────────
# Type aliases
# ─────────────────────────────────────────────────────────────────────────────

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
    Stores messages in a JSON file; prevents duplicates via message_id.
    Thread-safe via a single lock.
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

    # ── internal helpers ──────────────────────────────────────────────────

    def _read_all(self) -> List[Message]:
        with open(self.file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_all(self, messages: List[Message]) -> None:
        with open(self.file_path, "w", encoding="utf-8") as f:
            json.dump(messages, f, indent=2)

    # ── public API ────────────────────────────────────────────────────────

    def save_message(self, message: Message) -> bool:
        """
        Persist a message only if its message_id has not been seen before.
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

    def merge_messages(self, incoming: List[Message]) -> int:
        """
        Merge messages fetched from a peer into the local store.
        Returns the number of newly inserted messages.
        """
        with self.lock:
            messages = self._read_all()
            existing_ids = {m["message_id"] for m in messages}
            added = 0
            for msg in incoming:
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

    def size_bytes(self) -> int:
        """Return raw file size in bytes (useful for storage-overhead metrics)."""
        try:
            return os.path.getsize(self.file_path)
        except OSError:
            return 0


# ─────────────────────────────────────────────────────────────────────────────
# 2. Failure Detector  (missed-count threshold model)
# ─────────────────────────────────────────────────────────────────────────────

class FailureDetector:
    """
    Periodically pings every peer via heartbeat_fn.

    A peer is declared DEAD only after it misses `threshold` consecutive
    heartbeats (default = 3). This prevents false positives from transient
    network hiccups and is the standard phi-accrual / threshold approach.

    When a previously dead peer becomes alive again the detector fires the
    optional `on_peer_recovered` callback so the manager can drain its
    pending replication queue for that peer.
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

        # track consecutive misses  {peer: missed_count}
        self._missed: Dict[str, int] = {p: 0 for p in peers}
        # alive status after applying threshold
        self._status: Dict[str, bool] = {p: False for p in peers}

        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None

    # ── lifecycle ─────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    # ── internal loop ─────────────────────────────────────────────────────

    def _monitor_loop(self) -> None:
        while self._running:
            for peer in self.peers:
                try:
                    responded = self.heartbeat_fn(peer)
                except Exception:
                    responded = False

                with self._lock:
                    was_alive = self._status[peer]

                    if responded:
                        self._missed[peer] = 0
                        self._status[peer] = True
                        # if peer just recovered → notify manager
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

            time.sleep(self.interval_seconds)

    # ── queries ───────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────────────────────
# 3. Pending Replication Queue
# ─────────────────────────────────────────────────────────────────────────────

class PendingReplicationQueue:
    """
    When a replication attempt to a peer fails, the message is queued here.
    When the peer is declared alive again the queue is drained automatically.

    Structure:  { peer_address: [message, ...] }
    """

    def __init__(self):
        self._queue: Dict[str, List[Message]] = {}
        self._lock = threading.Lock()

    def enqueue(self, peer: str, message: Message) -> None:
        with self._lock:
            self._queue.setdefault(peer, []).append(message)

    def drain(self, peer: str) -> List[Message]:
        """Return and remove all pending messages for a peer."""
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
    """
    Main fault-tolerance orchestrator.

    Covers all five requirements for Member 1:
      ✔ Message redundancy    – replication_factor copies stored across cluster
      ✔ Failure detection     – FailureDetector with missed-count threshold
      ✔ Automatic failover    – skip dead peers; client stays connected via list
      ✔ Node recovery         – recover_from_peers() on rejoin + queue drain
      ✔ Redundancy metrics    – storage_overhead_bytes, per-peer success rates
    """

    def __init__(
        self,
        node_id: str,
        peers: List[str],
        heartbeat_fn: HeartbeatFunction,
        replicate_fn: ReplicateFunction,
        fetch_messages_fn: FetchMessagesFunction,
        store_path: str = "node/data/messages.json",
        replication_factor: int = 2,
        heartbeat_interval: float = 3.0,
        missed_threshold: int = 3,
    ):
        self.node_id = node_id
        self.peers = peers
        self.replication_factor = max(1, replication_factor)
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

        # Per-peer replication success / failure counters
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

    # ── lifecycle ─────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start failure detector and attempt recovery from peers."""
        self.detector.start()
        # Small delay so that peers have a chance to start as well
        time.sleep(1.0)
        recovered = self.recover_from_peers()
        self.metrics["recovered_messages"] += recovered

    def stop(self) -> None:
        self.detector.stop()

    # ── message construction ──────────────────────────────────────────────

    def build_message(
        self,
        sender: str,
        receiver: str,
        content: str,
        timestamp: Optional[float] = None,
    ) -> Message:
        """Build a normalised message dict with a globally-unique ID."""
        return {
            "message_id":   str(uuid.uuid4()),
            "sender":       sender,
            "receiver":     receiver,
            "content":      content,
            "timestamp":    timestamp if timestamp is not None else time.time(),
            "origin_node":  self.node_id,
        }

    # ── inbound handlers ──────────────────────────────────────────────────

    def handle_client_message(self, message: Message) -> Dict[str, object]:
        """
        Called when a client submits a new message to this node.
        1. Store locally (dedup).
        2. Replicate to live peers up to (replication_factor - 1) copies.
        3. Queue failed replications for later retry.
        """
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
        """
        Called when another node pushes a replica to this node.
        Simply stores locally (dedup guard already in place).
        """
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

    # ── replication ───────────────────────────────────────────────────────

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

        # Queue for dead peers so they get it when they come back
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

    # ── recovery ──────────────────────────────────────────────────────────

    def recover_from_peers(self) -> int:
        """
        On node rejoin: pull messages from all reachable peers and merge
        any that are missing locally.
        """
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
        """
        Callback fired by FailureDetector when a dead peer comes back.
        Drains the pending queue for that peer.
        """
        pending = self.pending_queue.drain(peer)
        retried = 0
        for message in pending:
            if self._try_replicate(peer, message):
                retried += 1
        if retried:
            self.metrics["pending_retried"] += retried
            print(
                f"[FaultTolerance] Peer {peer} recovered -> "
                f"retried {retried} queued message(s)."
            )

    # ── queries / export ──────────────────────────────────────────────────

    def export_messages(self) -> List[Message]:
        return self.store.get_all_messages()

    def get_peer_status(self) -> Dict[str, bool]:
        return self.detector.get_status()

    def get_metrics(self) -> Dict[str, object]:
        local_count   = self.store.count()
        storage_bytes = self.store.size_bytes()
        uptime        = round(time.time() - self._start_time, 1)

        # Per-peer replication success rate
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
            # overhead: if RF=2, total bytes stored cluster-wide ≈ 2× single node
            "estimated_storage_overhead_multiplier": self.replication_factor,
            "pending_queue_total":            self.pending_queue.total_pending(),
            "per_peer_replication":           per_peer,
            "missed_heartbeat_counts":        self.detector.get_missed_counts(),
            "metrics":                        dict(self.metrics),
            "peer_status":                    self.detector.get_status(),
        }
