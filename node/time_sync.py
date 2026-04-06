"""
HiveChat - Time Synchronization Module
Member: Shagee (IT24103322)

Responsibilities:
  1. LamportClock     — logical clock for causal ordering
  2. TimeSyncer       — NTP-style offset correction for physical timestamps
  3. MessageReorderer — re-sort messages that arrive out of order

Architecture Overview:
  ┌─────────────┐      gRPC (TimeSyncService)      ┌─────────────┐
  │   Node A    │ ◄──────────────────────────────► │   Node B    │
  │ TimeSyncer  │   offset = server - (send+rtt/2) │ (reference) │
  │ LamportClock│                                   └─────────────┘
  └──────┬──────┘
         │  get_adjusted_time()
         ▼
  ┌──────────────────┐      on_deliver(msg)      ┌──────────────┐
  │ MessageReorderer │ ─────────────────────────► │  Chat Window │
  │ (causal buffer)  │                            │  / Client    │
  └──────────────────┘                            └──────────────┘

  Flow:
    1. TimeSyncer runs a background thread that periodically polls the
       reference node (Raft leader) to estimate the clock offset.
    2. When sending a message, the node stamps it with:
       - get_adjusted_time()  → physical timestamp (offset-corrected)
       - lamport.tick()       → logical timestamp
    3. When receiving a message, the node:
       - Calls lamport.update(msg.lamport_time) to merge clocks.
       - Passes the message to MessageReorderer.try_deliver().
       - The reorderer checks vector clock dependencies and either
         delivers immediately or buffers until dependencies are met.

  Configuration:
    All tuneable parameters (sync_interval, sample_count, buffer_timeout,
    etc.) can be overridden via config/time_sync.json. See sync_config.py.
"""

import time
import logging
import threading
import statistics
from typing import Optional
from node.sync_config import SyncConfig
from node.sync_logger import SyncLogger

logger = logging.getLogger("hivechat.time_sync")

__all__ = [
    "LamportClock",
    "TimeSyncer",
    "MessageReorderer",
    "get_sync_stats",
]


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

    # Anomaly detection: warn if a received clock value jumps by more than this
    JUMP_THRESHOLD: int = 1000

    def __init__(self):
        self._time: int = 0
        self._lock = threading.Lock()
        self._anomalies: list[dict] = []  # log of detected anomalies
        self._total_ticks: int = 0
        self._total_updates: int = 0

    def tick(self) -> int:
        """Increment the clock (call before sending an event).

        Returns the new clock value to embed in the outgoing message.
        """
        with self._lock:
            prev = self._time
            self._time += 1
            self._total_ticks += 1
            # Invariant: clock must always increase
            assert self._time > prev, "Lamport clock failed to increment"
            return self._time

    def update(self, received_time: int) -> int:
        """Merge a received clock value, then tick.

        Call this on RECEIVE of a message with an embedded Lamport time.
        Implements: clock = max(local, received) + 1

        Validates the incoming value and detects anomalies:
          - Negative clock values (protocol violation)
          - Large jumps that may indicate a corrupted or rogue node

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
            self._total_updates += 1

            # Validate: Lamport times must be non-negative
            if received_time < 0:
                self._record_anomaly("negative_clock", received_time)
                logger.warning("Lamport anomaly: negative value %d received", received_time)
                received_time = 0  # clamp to safe value

            # Detect large jumps that may indicate a misbehaving node
            jump = received_time - self._time
            if jump > self.JUMP_THRESHOLD:
                self._record_anomaly("large_jump", received_time, jump=jump)
                logger.warning("Lamport anomaly: jump of %d (local=%d, received=%d)",
                               jump, self._time, received_time)

            prev = self._time
            self._time = max(self._time, received_time) + 1
            # Invariant: must strictly increase after merge
            assert self._time > prev, "Lamport clock did not advance after update"
            return self._time

    def get(self) -> int:
        """Return the current clock value (thread-safe read)."""
        with self._lock:
            return self._time

    def _record_anomaly(self, kind: str, received: int, **extra):
        """Record a detected anomaly for later inspection."""
        entry = {
            "kind": kind,
            "received": received,
            "local_at_time": self._time,
            "ts": time.time(),
            **extra,
        }
        self._anomalies.append(entry)
        # Keep only last 50 anomalies to bound memory
        if len(self._anomalies) > 50:
            self._anomalies.pop(0)

    def get_anomalies(self) -> list[dict]:
        """Return a copy of all detected anomalies."""
        with self._lock:
            return list(self._anomalies)

    def get_stats(self) -> dict:
        """Return clock statistics for monitoring."""
        with self._lock:
            return {
                "current_time": self._time,
                "total_ticks": self._total_ticks,
                "total_updates": self._total_updates,
                "anomaly_count": len(self._anomalies),
            }


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

    # Adaptive sync: speed up when drift is detected, slow down when stable
    MIN_SYNC_INTERVAL: float = 1.0    # fastest sync rate (seconds)
    MAX_SYNC_INTERVAL: float = 30.0   # slowest sync rate when stable
    DRIFT_THRESHOLD_MS: float = 50.0  # offset change > this triggers faster sync

    # RTT-based outlier rejection: discard samples with abnormal round-trip time
    RTT_REJECT_MULTIPLIER: float = 3.0  # reject if rtt > median_rtt * this

    def __init__(self, node_id: int, reference_addr: Optional[str] = None,
                 config: Optional[SyncConfig] = None):
        self.node_id        = node_id
        self.reference_addr = reference_addr   # address of the node to sync against
        self.offset: float  = 0.0              # current best estimate of clock offset
        self._samples: list[float] = []
        self._lock    = threading.Lock()
        self._running = False
        self.lamport  = LamportClock()         # each node's Lamport clock lives here
        self._current_interval: float = self.SYNC_INTERVAL  # adaptive interval
        self._prev_offset: float = 0.0         # previous offset for drift detection
        self._rtt_samples: list[float] = []    # RTT history for outlier detection
        self._rejected_count: int = 0          # how many samples were rejected
        self.slog = SyncLogger(node_id)        # structured event logger

        # Apply config overrides if provided
        if config:
            self.SYNC_INTERVAL = config.get("sync_interval")
            self.SAMPLE_COUNT  = config.get("sample_count")
            self.MAX_OFFSET_MS = config.get("max_offset_ms")
            self._current_interval = self.SYNC_INTERVAL

    def start(self):
        """Start the background sync thread (daemon, won't block shutdown)."""
        self._running = True
        t = threading.Thread(target=self._sync_loop, daemon=True)
        t.start()
        logger.info("Node %d: sync thread started (interval=%.1fs, samples=%d)",
                    self.node_id, self.SYNC_INTERVAL, self.SAMPLE_COUNT)
        self.slog.sync_started()

    def stop(self):
        """Stop the background sync thread."""
        self._running = False
        self.slog.sync_stopped()

    def set_reference(self, addr: str):
        """Update which node we sync against (call when Raft leader changes).

        Clears old samples since they came from a different reference clock.
        """
        with self._lock:
            old_addr = self.reference_addr
            self.reference_addr = addr
            self._samples.clear()
            self._rtt_samples.clear()
            self.offset = 0.0
        logger.info("Node %d: reference changed to %s", self.node_id, addr)
        self.slog.reference_changed(old_addr, addr)

    # ── Core Sync Logic ──────────────────────────────────────────────────

    def _sync_loop(self):
        """Background loop: periodically sync with the reference node.

        Uses adaptive interval — syncs faster when drift is detected,
        slows down when the offset is stable.
        """
        while self._running:
            if self.reference_addr:
                self._do_sync()
                self._adapt_interval()
            time.sleep(self._current_interval)

    def _adapt_interval(self):
        """Adjust the sync interval based on how much the offset is changing.

        If the offset changed significantly since the last sync, we speed up
        to converge faster. If the offset is stable, we slow down to reduce
        network overhead.
        """
        with self._lock:
            drift_ms = abs(self.offset - self._prev_offset) * 1000
            self._prev_offset = self.offset

        if drift_ms > self.DRIFT_THRESHOLD_MS:
            # Large drift detected — sync faster
            self._current_interval = max(
                self.MIN_SYNC_INTERVAL,
                self._current_interval * 0.5
            )
            logger.info("Node %d: drift %.1f ms detected, interval -> %.1fs",
                        self.node_id, drift_ms, self._current_interval)
            self.slog.interval_adapted(self._current_interval, drift_ms, "faster")
        else:
            # Stable — gradually slow down toward the default
            self._current_interval = min(
                self.MAX_SYNC_INTERVAL,
                self._current_interval * 1.2
            )
            # Don't exceed the configured default
            self._current_interval = min(self._current_interval, self.MAX_SYNC_INTERVAL)

    def _do_sync(self):
        """Perform one round-trip sync with the reference node.

        Tries the real gRPC call first (via time_sync_service.sync_once).
        Falls back to a simulated offset if gRPC is unavailable.
        Samples with abnormally high RTT are rejected as outliers.
        """
        try:
            from node.time_sync_service import sync_once
            result = sync_once(self.reference_addr, self.node_id)
            if 'error' not in result:
                self._add_sample(result['offset'], result.get('rtt', 0))
                return
            # gRPC call failed — fall through to stub
        except ImportError:
            pass

        # Fallback: simulated response for testing without gRPC
        t_send   = time.time()
        t_server = t_send + 0.001   # pretend server is 1 ms ahead
        t_recv   = time.time()
        rtt      = t_recv - t_send
        offset   = t_server - (t_send + rtt / 2)
        self._add_sample(offset, rtt)

    def _is_rtt_outlier(self, rtt: float) -> bool:
        """Check if a round-trip time is an outlier compared to recent history.

        Uses the median RTT as baseline. A sample is rejected if its RTT
        exceeds RTT_REJECT_MULTIPLIER times the median. This filters out
        samples taken during network congestion or GC pauses, which would
        produce inaccurate offset estimates.
        """
        if rtt <= 0 or len(self._rtt_samples) < 3:
            return False  # not enough history to judge
        median_rtt = statistics.median(self._rtt_samples)
        if median_rtt <= 0:
            return False
        return rtt > median_rtt * self.RTT_REJECT_MULTIPLIER

    def _add_sample(self, offset: float, rtt: float = 0.0):
        """Add a new offset sample and recompute the median.

        If the RTT for this sample is abnormally high (outlier), the offset
        sample is discarded to avoid corrupting the estimate with data from
        congested network conditions.
        """
        # Check for RTT outlier before accepting the sample
        if rtt > 0 and self._is_rtt_outlier(rtt):
            self._rejected_count += 1
            threshold = statistics.median(self._rtt_samples) * 1000 * self.RTT_REJECT_MULTIPLIER
            logger.info("Node %d: rejected sample (rtt=%.1f ms, threshold=%.1f ms)",
                        self.node_id, rtt * 1000, threshold)
            self.slog.sample_rejected(rtt * 1000, threshold)
            return

        with self._lock:
            # Track RTT history
            if rtt > 0:
                self._rtt_samples.append(rtt)
                if len(self._rtt_samples) > self.SAMPLE_COUNT:
                    self._rtt_samples.pop(0)

            self._samples.append(offset)
            if len(self._samples) > self.SAMPLE_COUNT:
                self._samples.pop(0)
            self.offset = statistics.median(self._samples)

        if abs(self.offset) * 1000 > self.MAX_OFFSET_MS:
            logger.warning("Node %d: large clock offset: %.1f ms",
                           self.node_id, self.offset * 1000)
            self.slog.offset_warning(self.offset * 1000)

        self.slog.sync_complete(
            offset_ms=self.offset * 1000,
            rtt_ms=rtt * 1000,
            sample_count=len(self._samples),
            interval=self._current_interval,
        )

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

    def get_current_interval(self) -> float:
        """Return the current adaptive sync interval in seconds."""
        return self._current_interval

    def get_stats(self) -> dict:
        """Return a snapshot of all sync metrics for monitoring.

        Returns a dict with:
            node_id, offset_ms, sample_count, reference_addr,
            lamport_time, running, sync_interval, max_offset_ms
        """
        with self._lock:
            return {
                "node_id": self.node_id,
                "offset_ms": round(self.offset * 1000, 3),
                "sample_count": len(self._samples),
                "samples_ms": [round(s * 1000, 3) for s in self._samples],
                "reference_addr": self.reference_addr,
                "lamport_time": self.lamport.get(),
                "running": self._running,
                "sync_interval": self.SYNC_INTERVAL,
                "current_interval": round(self._current_interval, 3),
                "max_offset_ms": self.MAX_OFFSET_MS,
                "rejected_samples": self._rejected_count,
                "rtt_samples_ms": [round(r * 1000, 3) for r in self._rtt_samples],
            }


# SECTION 3: Causal Message Reordering
# Buffers messages that arrive before their causal dependencies and
# releases them in the correct order once all dependencies are met.
# Uses vector clocks to determine causal readiness.
class MessageReorderer:
    """Buffers out-of-order messages and delivers them in causal order.

    When a message arrives whose vector clock shows it depends on a
    message we haven't delivered yet, we hold it in a buffer.  Once
    the missing dependency arrives, we flush everything that is now
    unblocked.

    Expected message dict format:
        {
            "id":           str,
            "sender_id":    int,
            "vector_clock": {node_id: int, ...},
            "lamport_time": int,
            "timestamp":    float,
            "content":      str,
        }

    Integration:
        reorderer = MessageReorderer()
        reorderer.try_deliver(msg, callback_fn)
        # callback_fn is called for each message in causal order.
    """

    BUFFER_TIMEOUT: float = 10.0  # seconds before a buffered message is force-delivered

    def __init__(self, buffer_timeout: float = None,
                 config: Optional[SyncConfig] = None):
        self._delivered: dict[int, int] = {}   # node_id -> highest delivered seq
        self._delivered_ids: set[str]   = set()
        self._buffer: list[dict]        = []
        self._lock = threading.Lock()

        # Priority: explicit arg > config file > class default
        if buffer_timeout is not None:
            self._buffer_timeout = buffer_timeout
        elif config:
            self._buffer_timeout = config.get("buffer_timeout")
        else:
            self._buffer_timeout = self.BUFFER_TIMEOUT

    def try_deliver(self, message: dict, on_deliver) -> None:
        """Attempt to deliver a message, buffering it if dependencies are unmet.

        Parameters
        ----------
        message : dict
            The incoming message (must contain 'id', 'sender_id', 'vector_clock').
        on_deliver : callable
            Callback invoked for each message that is ready for delivery.
        """
        with self._lock:
            if message["id"] in self._delivered_ids:
                return  # duplicate, skip
            message["_buffered_at"] = time.time()
            self._buffer.append(message)
            self._flush(on_deliver)

    @staticmethod
    def _sort_key(msg: dict):
        """Sort key for buffered messages: Lamport time first, then timestamp.

        When multiple messages become deliverable at the same time (e.g. after
        a missing dependency arrives), we deliver the oldest ones first. This
        ensures a consistent delivery order across all nodes.
        """
        return (msg.get("lamport_time", 0), msg.get("timestamp", 0))

    def _flush(self, on_deliver) -> None:
        """Release all buffered messages whose causal dependencies are met,
        or that have exceeded the buffer timeout.

        Messages are sorted by Lamport time before delivery so that when
        multiple messages become ready simultaneously, older messages
        (lower Lamport time) are delivered first.
        """
        now = time.time()
        changed = True
        while changed:
            changed = False
            # Sort buffer so oldest messages are checked first
            self._buffer.sort(key=self._sort_key)
            still_buffered = []
            for msg in self._buffer:
                timed_out = (now - msg.get("_buffered_at", now)) >= self._buffer_timeout
                if self._can_deliver(msg) or timed_out:
                    if timed_out:
                        logger.warning("force-delivering message %s after %.1fs timeout",
                                       msg["id"], self._buffer_timeout)
                    on_deliver(msg)
                    self._mark_delivered(msg)
                    changed = True
                else:
                    still_buffered.append(msg)
            self._buffer = still_buffered

    def _can_deliver(self, msg: dict) -> bool:
        """Check whether all causal dependencies for this message are satisfied.

        A message from node S with vector_clock V can be delivered when:
          - V[S] == delivered[S] + 1  (it's the next expected from sender)
          - For all other nodes N: V[N] <= delivered[N]
            (we've already seen everything the sender saw)
        """
        vc = msg["vector_clock"]
        sender = msg["sender_id"]

        for node_id_str, seq in vc.items():
            node_id = int(node_id_str)
            delivered_seq = self._delivered.get(node_id, 0)

            if node_id == sender:
                # Must be the next expected sequence from the sender
                if seq != delivered_seq + 1:
                    return False
            else:
                # Must have already delivered everything the sender saw
                if seq > delivered_seq:
                    return False

        return True

    def _mark_delivered(self, msg: dict) -> None:
        """Record that a message has been delivered."""
        self._delivered_ids.add(msg["id"])
        sender = msg["sender_id"]
        sender_seq = msg["vector_clock"].get(str(sender),
                                              msg["vector_clock"].get(sender, 0))
        current = self._delivered.get(sender, 0)
        if sender_seq > current:
            self._delivered[sender] = sender_seq

    def get_buffer_size(self) -> int:
        """Return the number of messages currently buffered."""
        with self._lock:
            return len(self._buffer)

    def get_delivered_count(self) -> int:
        """Return the number of messages delivered so far."""
        with self._lock:
            return len(self._delivered_ids)

    def get_stats(self) -> dict:
        """Return a snapshot of reorderer metrics for monitoring.

        Returns a dict with:
            buffer_size, delivered_count, buffer_timeout,
            buffered_msg_ids, delivered_per_node
        """
        with self._lock:
            return {
                "buffer_size": len(self._buffer),
                "delivered_count": len(self._delivered_ids),
                "buffer_timeout": self._buffer_timeout,
                "buffered_msg_ids": [m["id"] for m in self._buffer],
                "delivered_per_node": dict(self._delivered),
            }


def get_sync_stats(time_syncer: 'TimeSyncer',
                   reorderer: 'MessageReorderer') -> dict:
    """Aggregate stats from TimeSyncer and MessageReorderer into one report.

    Useful for health checks, debugging, and admin dashboards.

    Parameters
    ----------
    time_syncer : TimeSyncer
    reorderer : MessageReorderer

    Returns
    -------
    dict with keys 'time_sync' and 'reorderer'.
    """
    return {
        "time_sync": time_syncer.get_stats(),
        "reorderer": reorderer.get_stats(),
    }
