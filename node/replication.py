"""
HiveChat - Data Replication Module
Member: Maheesha (IT24103477)

Strategy: Quorum-Based Replication (N=3, W=2, R=2)
Consistency: Eventual Consistency with deduplication
"""

# pyright: basic     # Use Pylance/Pyright as the type checker (not Pyre2)

import uuid
import time
import threading
from typing import Optional

# SECTION 1: Message Store
# Stores every message this node holds, along with metadata.
class MessageStore:
    """In-memory store for messages on a single node.

    Each message entry looks like:
    {
        "id":        "uuid-string",          # Unique message ID (for deduplication)
        "chat_id":   "chat-room-identifier", # Which chat/channel this belongs to
        "sender":    "user-name",            # Who sent it
        "content":   "hello world",          # The actual text
        "timestamp": 1710000000.123,         # Wall-clock time (float, Unix seconds)
        "vector_clock": {1: 2, 2: 1, 3: 0}, # Logical clock per node
        "status":    "committed"             # "pending" | "committed"
    }
    """

    def __init__(self):
        # Main storage: msg_id → message dict
        self._store: dict[str, dict] = {}
        # Thread lock so multiple threads don't corrupt the store at the same time
        self._lock = threading.Lock()

    def save(self, message: dict) -> None:
        """Save (or overwrite) a message in the store."""
        with self._lock:
            self._store[message["id"]] = message

    def get(self, msg_id: str) -> Optional[dict]:
        """Retrieve a single message by its ID."""
        with self._lock:
            return self._store.get(msg_id)

    def get_by_chat(self, chat_id: str) -> list[dict]:
        """Return all committed messages for a specific chat, ordered by timestamp."""
        with self._lock:
            msgs = [
                m for m in self._store.values()
                if m["chat_id"] == chat_id and m["status"] == "committed"
            ]
        # Sort by wall-clock timestamp (Time Sync member can improve this later)
        return sorted(msgs, key=lambda m: m["timestamp"])

    def get_all(self) -> list[dict]:
        """Return every message this node holds (used for sync/anti-entropy)."""
        with self._lock:
            return list(self._store.values())

    def get_ids(self) -> set[str]:
        """Return the set of all message IDs this node has (for deduplication)."""
        with self._lock:
            return set(self._store.keys())

    def mark_committed(self, msg_id: str) -> None:
        """Mark a message as fully committed (quorum acknowledged)."""
        with self._lock:
            if msg_id in self._store:
                self._store[msg_id]["status"] = "committed"


# SECTION 2: Vector Clock
# Tracks causal ordering of events across nodes.
# Each node has its own counter. On each event, increment your own counter.
class VectorClock:
    """A vector clock for causal ordering of messages.

    Example with 3 nodes (IDs: 1, 2, 3):
      Node 1 sends msg  → clock becomes {1:1, 2:0, 3:0}
      Node 2 receives   → clock becomes {1:1, 2:1, 3:0}  (merge + increment own)
      Node 2 sends msg  → clock becomes {1:1, 2:2, 3:0}
    """

    def __init__(self, node_id: int, all_node_ids: list[int]):
        self.node_id = node_id
        # Initialize every node's counter to 0
        self.clock: dict[int, int] = {nid: 0 for nid in all_node_ids}

    def tick(self) -> dict[int, int]:
        """Increment this node's own counter (call before sending a message)."""
        self.clock[self.node_id] += 1
        return self.clock.copy()  # Return a snapshot copy

    def update(self, received_clock: dict[int, int]) -> dict[int, int]:
        """
        Merge an incoming clock with ours, then tick our own counter.
        Call this when we RECEIVE a message from another node.
        """
        for nid, count in received_clock.items():
            # Take the maximum of ours vs. the sender's knowledge
            self.clock[nid] = max(self.clock.get(nid, 0), count)
        # Also increment our own counter to record that we processed an event
        self.clock[self.node_id] += 1
        return self.clock.copy()

    def get(self) -> dict[int, int]:
        """Return a snapshot of the current clock (safe copy)."""
        return self.clock.copy()

    @staticmethod
    def happened_before(clock_a: dict, clock_b: dict) -> bool:
        """
        Returns True if event A causally happened before event B.
        A happened-before B means:
          - A's clock is <= B's clock in EVERY position, AND
          - strictly less in at least one position.
        """
        less_or_equal = all(clock_a.get(k, 0) <= clock_b.get(k, 0) for k in clock_b)
        strictly_less  = any(clock_a.get(k, 0) <  clock_b.get(k, 0) for k in clock_b)
        return less_or_equal and strictly_less

    @staticmethod
    def concurrent(clock_a: dict, clock_b: dict) -> bool:
        """
        Returns True if neither A happened-before B nor B happened-before A.
        These events are 'concurrent' — happened independently on different nodes.
        """
        return (
            not VectorClock.happened_before(clock_a, clock_b) and
            not VectorClock.happened_before(clock_b, clock_a)
        )


# SECTION 3: Replication Manager
# Ties everything together: deduplication, quorum writes, quorum reads, sync.
#
# CONSENSUS INTERFACE (how this connects to Gunitha's Raft module)
#
# The Raft (consensus) module is responsible for:
#   (a) Electing a LEADER node.
#   (b) Telling the replication layer WHO the leader is.
#
# This module (replication) is responsible for:
#   (a) USING the leader info to decide where to send writes.
#   (b) Actually storing and syncing the messages.
#
# The two modules talk through TWO simple methods that Gunitha must implement:
#
#   consensus.get_leader()  → int  (returns node_id of current leader, or -1)
#   consensus.is_leader()   → bool (True if THIS node is the leader right now)
#
# This replication module passes `consensus` in via the constructor and calls
# those two methods. That's the ENTIRE interface — nothing else couples them.
class ReplicationManager:
    """Manages quorum-based replication of messages across nodes.

    Parameters
    ----------
    node_id     : int    This node's unique ID (e.g. 1, 2, or 3)
    peers       : list   List of peer node addresses ["localhost:5002", ...]
    all_node_ids: list   All node IDs in the cluster [1, 2, 3]
    quorum_w    : int    Write quorum (default 2: message committed when 2 nodes ack)
    quorum_r    : int    Read quorum  (default 2: read from 2 nodes and merge)
    consensus   : object  Gunitha's consensus module (must have is_leader() method)
    """

    def __init__(
        self,
        node_id: int,
        peers: list[str],
        all_node_ids: list[int],
        quorum_w: int = 2,
        quorum_r: int = 2,
        consensus=None,
        time_syncer=None,
        reorderer=None,
    ):
        self.node_id    = node_id
        self.peers      = peers          # e.g. ["localhost:5002", "localhost:5003"]
        self.quorum_w   = quorum_w       # How many acks needed before write is "done"
        self.quorum_r   = quorum_r       # How many nodes to read from
        self.consensus  = consensus      # Gunitha's module (can be None for standalone)

        # Sub-components
        self.store       = MessageStore()
        self.vector_clock = VectorClock(node_id, all_node_ids)

        # Time synchronization (Shagee's module)
        self.time_syncer = time_syncer   # TimeSyncer instance for adjusted timestamps
        self.reorderer   = reorderer     # MessageReorderer for causal delivery

    #  3a. Deduplication Check 

    def _is_duplicate(self, msg_id: str) -> bool:
        """Return True if we've already seen this message ID."""
        return msg_id in self.store.get_ids()

    #  3b. Create a new message (entry point from the client) 

    def create_message(self, chat_id: str, sender: str, content: str) -> dict:
        """
        Build a new message dict with a fresh UUID, current timestamp,
        and the current vector clock snapshot.
        Called by the server when a client POSTs a new message.
        """
        clock_snapshot = self.vector_clock.tick()   # Advance our clock

        # Use TimeSyncer for offset-corrected timestamp if available
        if self.time_syncer:
            timestamp = self.time_syncer.get_adjusted_time()
            lamport_time = self.time_syncer.lamport.tick()
        else:
            timestamp = time.time()
            lamport_time = 0

        message = {
            "id":           str(uuid.uuid4()),       # Globally unique ID
            "chat_id":      chat_id,
            "sender":       sender,
            "content":      content,
            "timestamp":    timestamp,               # Offset-corrected time
            "lamport_time": lamport_time,            # Lamport logical clock value
            "vector_clock": clock_snapshot,          # Causal position of this message
            "sender_id":    self.node_id,            # Needed by MessageReorderer
            "status":       "pending",               # Will become "committed" after quorum
        }
        return message

    #  3c. Quorum Write 

    def write(self, message: dict) -> bool:
        """
        Perform a quorum write:
          1. Save to this node immediately.
          2. Forward to all peers and count acknowledgements.
          3. If acks + 1 (self) >= quorum_w, mark committed and return True.
          4. Otherwise return False (quorum not met).

        NOTE: In the full gRPC version, step 2 sends an RPC to each peer.
        For now, we simulate with a stub.
        """
        if self._is_duplicate(message["id"]):
            print(f"[Replication] Duplicate detected, ignoring msg {message['id']}")
            return True  # Already stored, treat as success

        # Step 1: Save locally as "pending"
        self.store.save(message)
        acks: int = 1  # Count ourselves

        # Step 2: Forward to peers (in the real version this is a gRPC call)
        for peer in self.peers:
            if self._forward_to_peer(peer, message):
                acks += 1  # type: ignore[operator]

        # Step 3: Check quorum
        if acks >= self.quorum_w:
            self.store.mark_committed(message["id"])
            print(f"[Replication] ✓ Quorum met ({acks}/{len(self.peers)+1}). "
                  f"Message {message['id']} committed.")
            return True
        else:
            print(f"[Replication] ✗ Quorum NOT met ({acks}/{len(self.peers)+1}). "
                  f"Message {message['id']} remains pending.")
            return False

    def _forward_to_peer(self, peer: str, message: dict) -> bool:
        """
        Send a message to a peer node and return True if it acknowledged.
        ── STUB ──  Replace with actual gRPC call in integration phase.

        gRPC call will look like:
            stub = ReplicationStub(channel)
            response = stub.ReplicateMessage(ReplicateRequest(message=message))
            return response.success
        """
        print(f"[Replication] Forwarding to {peer} ... (stub, always True for now)")
        return True  # Stub: assume peer always acks

    # ── 3d. Quorum Read ──────────────────────────────────────────────────────

    def read(self, chat_id: str) -> list[dict]:
        """
        Perform a quorum read:
          1. Read from this node.
          2. Ask quorum_r-1 peers for their copies.
          3. Merge and deduplicate all results.
          4. Sort by vector clock causal order (or timestamp as fallback).
        """
        # Start with local messages
        all_messages = {m["id"]: m for m in self.store.get_by_chat(chat_id)}

        # Gather from peers (stub: in real version, gRPC call to each peer)
        reads_done: int = 1
        for peer in self.peers:
            if reads_done >= self.quorum_r:
                break
            peer_msgs = self._read_from_peer(peer, chat_id)
            for m in peer_msgs:
                # Deduplication: keep the message that's more up-to-date (committed > pending)
                if m["id"] not in all_messages or m["status"] == "committed":
                    all_messages[m["id"]] = m
            reads_done += 1  # type: ignore[operator]  # Pyre2 false-positive on int +=

        # Sort: use timestamp as primary sort (Time Sync member can refine this)
        return sorted(all_messages.values(), key=lambda m: m["timestamp"])

    def _read_from_peer(self, peer: str, chat_id: str) -> list[dict]:
        """
        Read messages from a peer node.
        ── STUB ──  Replace with actual gRPC call in integration phase.
        """
        print(f"[Replication] Reading from {peer} ... (stub, returns empty for now)")
        return []  # Stub: peer returns nothing yet

    # ── 3e. Handle Incoming Replica (when a peer forwards a message to us) ───

    def receive_replica(self, message: dict) -> bool:
        """
        Called when another node sends us a ReplicateMessage RPC.
        This is the receiver side of _forward_to_peer.
        """
        if self._is_duplicate(message["id"]):
            return True  # Already have it, acknowledge anyway

        # Merge the incoming vector clock with ours
        updated_clock = self.vector_clock.update(message["vector_clock"])
        message["vector_clock"] = updated_clock
        message["status"] = "committed"  # We trust the sender already achieved quorum

        # Update Lamport clock from the incoming message if TimeSyncer is available
        if self.time_syncer and "lamport_time" in message:
            self.time_syncer.lamport.update(message["lamport_time"])

        # Route through MessageReorderer for causal delivery if available
        if self.reorderer:
            self.reorderer.try_deliver(message, self.store.save)
        else:
            self.store.save(message)

        print(f"[Replication] Received replica for msg {message['id']}")
        return True

    # ── 3f. Sync / Anti-Entropy (for nodes rejoining after a crash) ──────────

    def get_sync_state(self) -> list[dict]:
        """
        Return all messages this node has.
        Used by a recovering node to ask 'what did I miss?'
        Fault Tolerance member (Sihan) calls this when a node rejoins.
        """
        return self.store.get_all()

    def apply_sync(self, messages_from_peer: list[dict]) -> int:
        """
        Apply a batch of messages received from a peer during recovery sync.
        Returns the count of new messages learned.
        """
        new_count: int = 0
        for message in messages_from_peer:
            if not self._is_duplicate(message["id"]):
                self.store.save(message)
                new_count += 1  # type: ignore[operator]  # Pyre2 false-positive on int +=
        print(f"[Replication] Sync applied: {new_count} new messages learned.")
        return new_count
