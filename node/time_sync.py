"""
HiveChat - Time Synchronization Module
Member: Shagee (IT24103322)

Responsibilities:
  1. LamportClock     — logical clock for causal ordering
  2. TimeSyncer       — NTP-style offset correction for physical timestamps
  3. MessageReorderer — re-sort messages that arrive out of order
"""

import time
import threading
import statistics
from typing import Optional


# SECTION 1: Lamport Logical Clock
# A simple scalar clock that ensures causal ordering of events.
# Rule 1: Increment before every event you cause (sending a message).
# Rule 2: On receive, set clock = max(your_clock, received_clock) + 1.
# Guarantee: if event A happened-before event B, then A's clock < B's clock.
class LamportClock:
    """A simple Lamport logical clock for causal event ordering.

    Example with 2 nodes:
      Node 1 sends msg  → clock becomes 1
      Node 2 receives   → clock becomes max(0, 1) + 1 = 2
      Node 2 sends msg  → clock becomes 3
      Node 1 receives   → clock becomes max(1, 3) + 1 = 4

    Note: Maheesha's VectorClock (in replication.py) provides stronger
    guarantees — it can detect concurrent events. This LamportClock is
    used as a simpler secondary timestamp alongside the NTP-style
    physical time correction (TimeSyncer).
    """

    def __init__(self):
        self._time: int = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        """Increment the clock (call before sending an event).

        Returns the new clock value to embed in the outgoing message.
        """
        with self._lock:
            self._time += 1
            return self._time

    def update(self, received_time: int) -> int:
        """Merge a received clock value, then tick.

        Call this on RECEIVE of a message with an embedded Lamport time.
        Implements: clock = max(local, received) + 1

        Parameters
        ----------
        received_time : int
            The Lamport timestamp from the incoming message.

        Returns
        -------
        int
            The updated local clock value.
        """
        with self._lock:
            self._time = max(self._time, received_time) + 1
            return self._time

    def get(self) -> int:
        """Return the current clock value (thread-safe read)."""
        with self._lock:
            return self._time


# SECTION 2: NTP-Style Time Synchronization
# Periodically polls a reference node (typically the Raft leader) and
# computes the clock offset using Cristian's algorithm:
#     offset = server_time - (send_time + rtt / 2)
# Multiple samples are median-filtered to reduce network noise.
class TimeSyncer:
    """NTP-style clock offset correction for physical timestamps.

    How it works (Cristian's Algorithm):
      1. Send a time request to the reference node, record send_time.
      2. Reference replies with its current time (server_time).
      3. Record receive_time, compute RTT = receive_time - send_time.
      4. Estimate offset = server_time - (send_time + RTT/2).
      5. Keep the last SAMPLE_COUNT offsets, use the median to filter noise.
      6. All physical timestamps are adjusted by this offset.

    Integration:
      - ReplicationManager calls get_adjusted_time() when creating messages.
      - Incoming message timestamps can be corrected via correct_timestamp().
      - Call set_reference() when the Raft leader changes.
    """

    SYNC_INTERVAL: float = 5.0   # seconds between sync polls
    SAMPLE_COUNT:  int   = 8     # how many samples to median-filter
    MAX_OFFSET_MS: float = 500   # warn if offset exceeds this (milliseconds)

    def __init__(self, node_id: int, reference_addr: Optional[str] = None):
        self.node_id        = node_id
        self.reference_addr = reference_addr   # address of the node to sync against
        self.offset: float  = 0.0              # current best estimate of clock offset
        self._samples: list[float] = []
        self._lock    = threading.Lock()
        self._running = False
        self.lamport  = LamportClock()         # each node's Lamport clock lives here

    def start(self):
        """Start the background sync thread (daemon, won't block shutdown)."""
        self._running = True
        t = threading.Thread(target=self._sync_loop, daemon=True)
        t.start()
        print(f"[TimeSync] Node {self.node_id}: sync thread started "
              f"(interval={self.SYNC_INTERVAL}s, samples={self.SAMPLE_COUNT})")

    def stop(self):
        """Stop the background sync thread."""
        self._running = False

    def set_reference(self, addr: str):
        """Update which node we sync against (call when Raft leader changes).

        Clears old samples since they came from a different reference clock.
        """
        with self._lock:
            self.reference_addr = addr
            self._samples.clear()
            self.offset = 0.0
        print(f"[TimeSync] Node {self.node_id}: reference changed to {addr}")

    # ── Core Sync Logic ──────────────────────────────────────────────────

    def _sync_loop(self):
        """Background loop: periodically sync with the reference node."""
        while self._running:
            if self.reference_addr:
                self._do_sync()
            time.sleep(self.SYNC_INTERVAL)

    def _do_sync(self):
        """Perform one round-trip sync with the reference node.

        STUB — the gRPC call will be wired in during integration.
        For now, simulates a small drift for testing.
        """
        t_send = time.time()

        # ── STUB: replace with real gRPC call ────────────────────────
        # stub = TimeSyncServiceStub(channel)
        # resp = stub.GetTime(TimeRequest(client_send_time=t_send))
        # t_server = resp.server_time
        # t_recv   = time.time()
        # rtt      = t_recv - t_send
        # offset   = t_server - (t_send + rtt / 2)
        # ─────────────────────────────────────────────────────────────

        # Simulated response for testing without gRPC
        t_server = t_send + 0.001   # pretend server is 1 ms ahead
        t_recv   = time.time()
        rtt      = t_recv - t_send
        offset   = t_server - (t_send + rtt / 2)

        self._add_sample(offset)

    def _add_sample(self, offset: float):
        """Add a new offset sample and recompute the median."""
        with self._lock:
            self._samples.append(offset)
            if len(self._samples) > self.SAMPLE_COUNT:
                self._samples.pop(0)
            self.offset = statistics.median(self._samples)

        if abs(self.offset) * 1000 > self.MAX_OFFSET_MS:
            print(f"[TimeSync] WARNING: large clock offset: "
                  f"{self.offset * 1000:.1f} ms")

    # ── Public API ───────────────────────────────────────────────────────

    def get_adjusted_time(self) -> float:
        """Return the current time, adjusted for the measured clock offset.

        Use this instead of time.time() when creating message timestamps:
            message["timestamp"] = time_syncer.get_adjusted_time()
        """
        with self._lock:
            return time.time() + self.offset

    def correct_timestamp(self, remote_timestamp: float,
                          remote_offset: float = 0.0) -> float:
        """Correct an incoming message's timestamp.

        Adjusts the sender's timestamp by removing their known offset
        and applying ours, so all timestamps are in a common time frame.

        Parameters
        ----------
        remote_timestamp : float
            The timestamp from the incoming message.
        remote_offset : float
            The sender's clock offset (if known), default 0.

        Returns
        -------
        float
            The corrected timestamp.
        """
        with self._lock:
            return remote_timestamp - remote_offset + self.offset

    def get_offset(self) -> float:
        """Return the current estimated offset in seconds."""
        with self._lock:
            return self.offset

    def get_sample_count(self) -> int:
        """Return how many offset samples have been collected."""
        with self._lock:
            return len(self._samples)


# TODO: Implement MessageReorderer (causal delivery buffer)
# - Buffer messages that arrive out of causal order
# - Release them once all dependencies are satisfied
# - Use vector clocks to check causal readiness
