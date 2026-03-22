"""
HiveChat - Time Synchronization Module
Member: Shagee (IT24103322)

Responsibilities:
  1. LamportClock     — logical clock for causal ordering
  2. TimeSyncer       — NTP-style offset correction for physical timestamps
  3. MessageReorderer — re-sort messages that arrive out of order
"""

import threading


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


# TODO: Implement TimeSyncer (NTP-style offset correction)
# - Periodically poll a reference node using GetTime() RPC
# - Compute offset = server_time - (send_time + rtt/2)
# - Median-filter the last N samples to reduce noise
# - Provide get_adjusted_time() for corrected timestamps

# TODO: Implement MessageReorderer (causal delivery buffer)
# - Buffer messages that arrive out of causal order
# - Release them once all dependencies are satisfied
# - Use vector clocks to check causal readiness
