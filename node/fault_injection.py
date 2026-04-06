"""
HiveChat - Time Sync Fault Injection
Member: Shagee (IT24103322)

Helpers for testing how the time sync system behaves under
adverse conditions: clock jumps, network delays, and partitions.

These are TESTING ONLY utilities — never enable in production.

Usage:
    from node.fault_injection import FaultInjector

    injector = FaultInjector(time_syncer)
    injector.inject_clock_jump(0.500)      # +500ms jump
    injector.inject_clock_jump(-0.200)     # -200ms jump
    injector.simulate_network_delay(0.1)   # 100ms extra RTT
    injector.simulate_partition()           # drop all sync attempts
    injector.clear_all()                   # remove all injections
"""

import time
import logging
from typing import Optional

logger = logging.getLogger("hivechat.fault_injection")


class FaultInjector:
    """Injects faults into TimeSyncer for testing resilience.

    Wraps a TimeSyncer instance and can:
      - Add sudden clock jumps (positive or negative)
      - Simulate network delays on sync round-trips
      - Simulate network partitions (reference becomes unreachable)
      - Corrupt individual offset samples
    """

    def __init__(self, time_syncer):
        self._syncer = time_syncer
        self._clock_jump: float = 0.0         # extra offset to inject
        self._network_delay: float = 0.0       # extra delay per sync
        self._partition: bool = False           # simulate unreachable reference
        self._original_do_sync = time_syncer._do_sync
        self._original_get_adjusted = time_syncer.get_adjusted_time
        self._active = False

    def enable(self):
        """Activate fault injection by patching the TimeSyncer methods."""
        if self._active:
            return
        self._active = True

        syncer = self._syncer
        injector = self

        # Patch _do_sync to add delays and partition simulation
        original_do_sync = self._original_do_sync

        def patched_do_sync():
            if injector._partition:
                logger.info("[FaultInjection] sync blocked — partition active")
                return
            if injector._network_delay > 0:
                time.sleep(injector._network_delay)
            original_do_sync()

        syncer._do_sync = patched_do_sync

        # Patch get_adjusted_time to add clock jump
        original_get_adjusted = self._original_get_adjusted

        def patched_get_adjusted():
            return original_get_adjusted() + injector._clock_jump

        syncer.get_adjusted_time = patched_get_adjusted
        logger.info("[FaultInjection] enabled for node %d", syncer.node_id)

    def disable(self):
        """Remove all patches and restore original TimeSyncer behavior."""
        if not self._active:
            return
        self._syncer._do_sync = self._original_do_sync
        self._syncer.get_adjusted_time = self._original_get_adjusted
        self._active = False
        self.clear_all()
        logger.info("[FaultInjection] disabled for node %d", self._syncer.node_id)

    def inject_clock_jump(self, seconds: float):
        """Inject a sudden clock offset jump.

        Parameters
        ----------
        seconds : float
            Offset to add to get_adjusted_time(). Positive = clock ahead,
            negative = clock behind.
        """
        self._clock_jump = seconds
        logger.info("[FaultInjection] clock jump: %+.3f s", seconds)

    def simulate_network_delay(self, seconds: float):
        """Add extra latency to each sync round-trip.

        Parameters
        ----------
        seconds : float
            Extra delay in seconds added before each _do_sync call.
        """
        self._network_delay = seconds
        logger.info("[FaultInjection] network delay: %.3f s", seconds)

    def simulate_partition(self):
        """Block all sync attempts, simulating a network partition."""
        self._partition = True
        logger.info("[FaultInjection] partition active — syncs blocked")

    def heal_partition(self):
        """Restore connectivity after a simulated partition."""
        self._partition = False
        logger.info("[FaultInjection] partition healed — syncs resumed")

    def corrupt_sample(self, offset: float, rtt: float = 0.001):
        """Inject a single corrupt offset sample directly into the syncer.

        Parameters
        ----------
        offset : float
            The fake offset value to inject (in seconds).
        rtt : float
            The fake RTT value (default 1ms).
        """
        self._syncer._add_sample(offset, rtt)
        logger.info("[FaultInjection] injected corrupt sample: offset=%.3f ms",
                    offset * 1000)

    def clear_all(self):
        """Reset all fault injection state."""
        self._clock_jump = 0.0
        self._network_delay = 0.0
        self._partition = False

    def get_state(self) -> dict:
        """Return current fault injection state."""
        return {
            "active": self._active,
            "clock_jump_s": self._clock_jump,
            "network_delay_s": self._network_delay,
            "partition": self._partition,
        }
