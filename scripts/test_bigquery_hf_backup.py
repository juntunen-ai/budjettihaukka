#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from backup_bigquery_to_huggingface import (
    _file_record,
    _snapshot_id,
    _validate_snapshot_id,
    verify_snapshot,
)


def main() -> None:
    assert _snapshot_id(datetime(2026, 8, 18, 12, 34, 56, tzinfo=timezone.utc)) == "20260818T123456Z"
    assert _validate_snapshot_id("20260818T123456Z") == "20260818T123456Z"
    try:
        _validate_snapshot_id("2026-08-18")
    except argparse.ArgumentTypeError:
        pass
    else:
        raise AssertionError("invalid snapshot id was accepted")

    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        payload = root / "metadata" / "table.json"
        payload.parent.mkdir(parents=True)
        payload.write_text('{"ok": true}\n', encoding="utf-8")
        record = _file_record(root, payload)
        manifest = {
            "format_version": 1,
            "snapshot_id": "20260818T123456Z",
            "status": "COMPLETE",
            "files": [record],
        }
        (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        result = verify_snapshot(root)
        assert result == {"files": 1, "bytes": payload.stat().st_size}

        payload.write_text("tampered\n", encoding="utf-8")
        try:
            verify_snapshot(root)
        except RuntimeError as exc:
            assert "mismatch" in str(exc).lower()
        else:
            raise AssertionError("tampered snapshot was accepted")

    print("BigQuery -> Hugging Face backup tests PASSED")


if __name__ == "__main__":
    main()
