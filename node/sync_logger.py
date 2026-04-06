"""
HiveChat - Structured Time Sync Logger
Member: Shagee (IT24103322)

Provides a JSON-formatted logger for time sync events so that
logs can be parsed by monitoring tools, dashboards, and scripts.

Usage:
    from node.sync_logger import SyncLogger

    slog = SyncLogger(node_id=1)
    slog.sync_complete(offset_ms=3.2, rtt_ms=1.5, sample_count=5)
    slog.sample_rejected(rtt_ms=150.0, threshold_ms=18.0)
    slog.offset_warning(offset_ms=520.0)
    slog.interval_adapted(new_interval=2.5, drift_ms=60.0)
"""

import json
import time
import logging
from typing import Optional

logger = logging.getLogger("hivechat.sync_events")


class SyncLogger:
    """Emits structured JSON log entries for time sync events.

    Each log entry includes a timestamp, node_id, event type, and
    event-specific data. This makes it easy to grep, filter, and
    aggregate sync behaviour across a cluster.
    """

    def __init__(self, node_id: int):
        self.node_id = node_id

    def _emit(self, event: str, **data):
        """Emit a structured log entry."""
        entry = {
            "ts": round(time.time(), 3),
            "node": self.node_id,
            "event": event,
            **data,
        }
        logger.info(json.dumps(entry))

    def sync_complete(self, offset_ms: float, rtt_ms: float,
                      sample_count: int, interval: Optional[float] = None):
        """Log a successful sync round-trip."""
        self._emit("sync_complete",
                    offset_ms=round(offset_ms, 3),
                    rtt_ms=round(rtt_ms, 3),
                    samples=sample_count,
                    interval=round(interval, 3) if interval else None)

    def sample_rejected(self, rtt_ms: float, threshold_ms: float):
        """Log a rejected sample due to high RTT."""
        self._emit("sample_rejected",
                    rtt_ms=round(rtt_ms, 3),
                    threshold_ms=round(threshold_ms, 3))

    def offset_warning(self, offset_ms: float):
        """Log a large clock offset warning."""
        self._emit("offset_warning",
                    offset_ms=round(offset_ms, 3))

    def interval_adapted(self, new_interval: float, drift_ms: float,
                         direction: str = "faster"):
        """Log an adaptive interval change."""
        self._emit("interval_adapted",
                    interval=round(new_interval, 3),
                    drift_ms=round(drift_ms, 3),
                    direction=direction)

    def reference_changed(self, old_addr: Optional[str], new_addr: str):
        """Log a reference node change."""
        self._emit("reference_changed",
                    old=old_addr,
                    new=new_addr)

    def sync_started(self):
        """Log that the sync thread has started."""
        self._emit("sync_started")

    def sync_stopped(self):
        """Log that the sync thread has stopped."""
        self._emit("sync_stopped")

    def message_buffered(self, msg_id: str, buffer_size: int):
        """Log a message being buffered for causal reordering."""
        self._emit("message_buffered",
                    msg_id=msg_id,
                    buffer_size=buffer_size)

    def message_force_delivered(self, msg_id: str, wait_seconds: float):
        """Log a message being force-delivered after timeout."""
        self._emit("message_force_delivered",
                    msg_id=msg_id,
                    wait_s=round(wait_seconds, 3))

    def buffer_flushed(self, delivered_count: int, remaining: int):
        """Log a buffer flush event."""
        self._emit("buffer_flushed",
                    delivered=delivered_count,
                    remaining=remaining)
