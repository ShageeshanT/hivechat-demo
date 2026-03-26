"""
HiveChat - Time Sync Configuration
Member: Shagee (IT24103322)

Loads sync parameters from a JSON config file with sensible defaults.
Allows tuning sync interval, sample count, buffer timeout, etc.
without modifying source code.

Supported config keys (in config/time_sync.json):
  sync_interval  — seconds between NTP-style sync polls (default: 5.0)
  sample_count   — number of offset samples for median filter (default: 8)
  max_offset_ms  — warn threshold in milliseconds (default: 500)
  buffer_timeout — seconds before force-delivering stuck messages (default: 10.0)
  grpc_port      — default gRPC port for TimeSyncService (default: 50051)

Priority order:
  1. Explicit constructor arguments (e.g. buffer_timeout=30.0)
  2. Values from the JSON config file
  3. Built-in DEFAULTS defined in this module
"""

import json
import os
import logging
from typing import Optional

logger = logging.getLogger("hivechat.sync_config")

# Default values used when no config file is present
DEFAULTS = {
    "sync_interval": 5.0,       # seconds between NTP-style sync polls
    "sample_count": 8,          # number of offset samples for median filter
    "max_offset_ms": 500,       # warn if offset exceeds this (milliseconds)
    "buffer_timeout": 10.0,     # seconds before force-delivering buffered messages
    "grpc_port": 50051,         # default gRPC port for TimeSyncService
}


class SyncConfig:
    """Loads and provides access to time sync configuration values.

    Reads from a JSON file if available, otherwise uses defaults.
    Unknown keys in the config file are ignored.

    Usage:
        config = SyncConfig("config/time_sync.json")
        interval = config.get("sync_interval")
    """

    def __init__(self, config_path: Optional[str] = None):
        self._values = dict(DEFAULTS)
        self._config_path = config_path

        if config_path and os.path.exists(config_path):
            self._load_from_file(config_path)
        elif config_path:
            logger.info("config file not found at %s, using defaults", config_path)

    def _load_from_file(self, path: str) -> None:
        """Load config values from a JSON file, overriding defaults."""
        try:
            with open(path, "r") as f:
                data = json.load(f)

            for key in DEFAULTS:
                if key in data:
                    self._values[key] = data[key]
                    logger.info("config: %s = %s", key, data[key])

        except (json.JSONDecodeError, IOError) as e:
            logger.warning("failed to load config from %s: %s, using defaults", path, e)

    def get(self, key: str):
        """Get a config value by key. Returns the default if key is unknown."""
        return self._values.get(key, DEFAULTS.get(key))

    def get_all(self) -> dict:
        """Return a copy of all config values."""
        return dict(self._values)

    def save(self, path: Optional[str] = None) -> None:
        """Write current config values to a JSON file."""
        out_path = path or self._config_path
        if not out_path:
            logger.warning("no config path specified, cannot save")
            return

        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        with open(out_path, "w") as f:
            json.dump(self._values, f, indent=2)
        logger.info("config saved to %s", out_path)
