"""
HiveChat - Data Replication Module
Member: Maheesha (IT24103477)

Strategy: Quorum-Based Replication (N=3, W=2, R=2)
Consistency: Eventual Consistency with deduplication
"""

from __future__ import annotations  # Allow modern type hints on older Python checkers
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
