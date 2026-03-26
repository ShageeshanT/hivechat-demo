"""
Tests for HiveChat Sync Configuration
Member: Shagee (IT24103322)
"""

import unittest
import os
import json
import tempfile
from node.sync_config import SyncConfig, DEFAULTS


class TestSyncConfig(unittest.TestCase):
    """Tests for the SyncConfig loader."""

    def test_defaults_when_no_file(self):
        config = SyncConfig()
        self.assertEqual(config.get("sync_interval"), 5.0)
        self.assertEqual(config.get("sample_count"), 8)
        self.assertEqual(config.get("max_offset_ms"), 500)
        self.assertEqual(config.get("buffer_timeout"), 10.0)

    def test_defaults_when_file_not_found(self):
        config = SyncConfig("/nonexistent/path.json")
        self.assertEqual(config.get("sync_interval"), DEFAULTS["sync_interval"])

    def test_load_from_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"sync_interval": 2.0, "sample_count": 16}, f)
            path = f.name

        try:
            config = SyncConfig(path)
            self.assertEqual(config.get("sync_interval"), 2.0)
            self.assertEqual(config.get("sample_count"), 16)
            # unset values should still be defaults
            self.assertEqual(config.get("buffer_timeout"), DEFAULTS["buffer_timeout"])
        finally:
            os.unlink(path)

    def test_unknown_keys_ignored(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"sync_interval": 3.0, "unknown_key": "ignored"}, f)
            path = f.name

        try:
            config = SyncConfig(path)
            self.assertEqual(config.get("sync_interval"), 3.0)
            self.assertIsNone(config.get("unknown_key"))
        finally:
            os.unlink(path)

    def test_get_all(self):
        config = SyncConfig()
        all_vals = config.get_all()
        self.assertEqual(all_vals, DEFAULTS)

    def test_save_and_reload(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test_config.json")
            config = SyncConfig()
            config.save(path)

            # Reload from saved file
            config2 = SyncConfig(path)
            self.assertEqual(config2.get_all(), config.get_all())

    def test_invalid_json_uses_defaults(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("not valid json{{{")
            path = f.name

        try:
            config = SyncConfig(path)
            self.assertEqual(config.get("sync_interval"), DEFAULTS["sync_interval"])
        finally:
            os.unlink(path)

    def test_config_applied_to_timesyncer(self):
        from node.time_sync import TimeSyncer
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"sync_interval": 1.0, "sample_count": 4}, f)
            path = f.name

        try:
            config = SyncConfig(path)
            ts = TimeSyncer(node_id=1, config=config)
            self.assertEqual(ts.SYNC_INTERVAL, 1.0)
            self.assertEqual(ts.SAMPLE_COUNT, 4)
        finally:
            os.unlink(path)

    def test_config_applied_to_reorderer(self):
        from node.time_sync import MessageReorderer
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"buffer_timeout": 30.0}, f)
            path = f.name

        try:
            config = SyncConfig(path)
            r = MessageReorderer(config=config)
            self.assertEqual(r._buffer_timeout, 30.0)
        finally:
            os.unlink(path)


if __name__ == "__main__":
    unittest.main()
