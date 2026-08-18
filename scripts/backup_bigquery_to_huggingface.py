#!/usr/bin/env python3
"""Create an integrity-checked BigQuery snapshot and optionally upload it to HF.

The exporter is intentionally read-only against BigQuery. Physical tables are
streamed to sharded Parquet files; views and all object metadata are stored as
JSON/SQL so the dataset can be reconstructed later. Hugging Face uploads use a
private Storage Bucket and never delete or overwrite older snapshot prefixes.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

FORMAT_VERSION = 1
DATA_OBJECT_TYPES = {"TABLE", "SNAPSHOT", "CLONE"}
SNAPSHOT_ID_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z$")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _snapshot_id(now: datetime | None = None) -> str:
    return (now or _utc_now()).strftime("%Y%m%dT%H%M%SZ")


def _validate_snapshot_id(value: str) -> str:
    if not SNAPSHOT_ID_RE.fullmatch(value):
        raise argparse.ArgumentTypeError("snapshot id must use YYYYMMDDTHHMMSSZ")
    return value


def _sha256(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _file_record(root: Path, path: Path, *, rows: int | None = None) -> dict[str, Any]:
    record: dict[str, Any] = {
        "path": path.relative_to(root).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }
    if rows is not None:
        record["rows"] = rows
    return record


def _write_checksums(snapshot_dir: Path, files: Iterable[dict[str, Any]]) -> Path:
    checksum_path = snapshot_dir / "checksums.sha256"
    lines = [f"{item['sha256']}  {item['path']}" for item in sorted(files, key=lambda x: x["path"])]
    checksum_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return checksum_path


def verify_snapshot(snapshot_dir: Path) -> dict[str, int]:
    manifest_path = snapshot_dir / "manifest.json"
    if not manifest_path.exists():
        raise RuntimeError(f"Missing manifest: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("status") != "COMPLETE":
        raise RuntimeError(f"Snapshot is not complete: {manifest.get('status')}")

    verified_bytes = 0
    verified_files = 0
    for item in manifest.get("files", []):
        path = snapshot_dir / item["path"]
        if not path.is_file():
            raise RuntimeError(f"Missing snapshot file: {item['path']}")
        actual_size = path.stat().st_size
        if actual_size != item["bytes"]:
            raise RuntimeError(
                f"Size mismatch for {item['path']}: expected {item['bytes']}, got {actual_size}"
            )
        actual_hash = _sha256(path)
        if actual_hash != item["sha256"]:
            raise RuntimeError(f"SHA-256 mismatch for {item['path']}")
        verified_files += 1
        verified_bytes += actual_size

    return {"files": verified_files, "bytes": verified_bytes}


def _export_table(client: Any, table: Any, snapshot_dir: Path, rows_per_shard: int) -> tuple[list[dict[str, Any]], int]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - operational dependency
        raise RuntimeError("Install requirements-backup.txt before exporting data") from exc

    table_dir = snapshot_dir / "tables" / table.table_id
    table_dir.mkdir(parents=True, exist_ok=True)
    rows = client.list_rows(table, page_size=min(rows_per_shard, 100_000))
    files: list[dict[str, Any]] = []
    total_rows = 0
    shard_rows = 0
    shard_index = 0
    writer = None
    shard_path: Path | None = None

    try:
        for batch in rows.to_arrow_iterable():
            if writer is None:
                shard_path = table_dir / f"part-{shard_index:05d}.parquet"
                writer = pq.ParquetWriter(
                    shard_path,
                    batch.schema,
                    compression="zstd",
                    use_dictionary=True,
                    write_statistics=True,
                )
            writer.write_batch(batch)
            batch_rows = batch.num_rows
            shard_rows += batch_rows
            total_rows += batch_rows
            if shard_rows >= rows_per_shard:
                writer.close()
                writer = None
                assert shard_path is not None
                files.append(_file_record(snapshot_dir, shard_path, rows=shard_rows))
                shard_index += 1
                shard_rows = 0
                shard_path = None
    finally:
        if writer is not None:
            writer.close()
            assert shard_path is not None
            files.append(_file_record(snapshot_dir, shard_path, rows=shard_rows))

    expected_rows = int(table.num_rows or 0)
    if total_rows != expected_rows:
        raise RuntimeError(
            f"Row count mismatch for {table.table_id}: expected {expected_rows}, exported {total_rows}"
        )
    return files, total_rows


def create_local_snapshot(args: argparse.Namespace) -> Path:
    try:
        from google.cloud import bigquery
    except ImportError as exc:  # pragma: no cover - required by main project
        raise RuntimeError("google-cloud-bigquery is required") from exc

    client = bigquery.Client(project=args.project, location=args.location)
    dataset_ref = f"{args.project}.{args.dataset}"
    dataset = client.get_dataset(dataset_ref)
    selected = set(args.table or [])
    listed = sorted(client.list_tables(dataset_ref), key=lambda item: item.table_id)
    if selected:
        available = {item.table_id for item in listed}
        unknown = sorted(selected - available)
        if unknown:
            raise RuntimeError(f"Unknown tables: {', '.join(unknown)}")
        listed = [item for item in listed if item.table_id in selected]

    plan = []
    for item in listed:
        table = client.get_table(item.reference)
        plan.append(
            {
                "table_id": table.table_id,
                "type": table.table_type,
                "rows": int(table.num_rows or 0),
                "logical_bytes": int(table.num_bytes or 0),
                "expires": table.expires.isoformat() if table.expires else None,
            }
        )

    physical = [item for item in plan if item["type"] in DATA_OBJECT_TYPES]
    print(
        f"Plan: objects={len(plan)}, physical={len(physical)}, "
        f"rows={sum(item['rows'] for item in physical)}, "
        f"logical_bytes={sum(item['logical_bytes'] for item in physical)}"
    )
    if args.dry_run:
        for item in plan:
            print(
                f"{item['type']:18} {item['table_id']:55} "
                f"rows={item['rows']:>9} bytes={item['logical_bytes']:>12} expires={item['expires']}"
            )
        return Path(args.output_root) / args.snapshot_id

    snapshot_dir = Path(args.output_root).resolve() / args.snapshot_id
    if snapshot_dir.exists() and any(snapshot_dir.iterdir()):
        raise RuntimeError(f"Snapshot directory is not empty: {snapshot_dir}")
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    manifest: dict[str, Any] = {
        "format_version": FORMAT_VERSION,
        "snapshot_id": args.snapshot_id,
        "status": "IN_PROGRESS",
        "created_at": _utc_now().isoformat(),
        "source": {
            "project": args.project,
            "dataset": args.dataset,
            "location": dataset.location,
            "dataset_api_resource": dataset.to_api_repr(),
        },
        "objects": [],
        "files": [],
    }
    _write_json(snapshot_dir / "manifest.json", manifest)

    all_files: list[dict[str, Any]] = []
    try:
        for index, item in enumerate(listed, start=1):
            table = client.get_table(item.reference)
            print(f"[{index}/{len(listed)}] {table.table_type} {table.table_id}", flush=True)
            metadata_path = snapshot_dir / "metadata" / f"{table.table_id}.json"
            _write_json(metadata_path, table.to_api_repr())
            object_files = [_file_record(snapshot_dir, metadata_path)]

            view_query = getattr(table, "view_query", None) or getattr(table, "mview_query", None)
            if view_query:
                sql_path = snapshot_dir / "views" / f"{table.table_id}.sql"
                sql_path.parent.mkdir(parents=True, exist_ok=True)
                sql_path.write_text(view_query.rstrip() + "\n", encoding="utf-8")
                object_files.append(_file_record(snapshot_dir, sql_path))

            exported_rows = 0
            if not args.metadata_only and table.table_type in DATA_OBJECT_TYPES:
                data_files, exported_rows = _export_table(
                    client, table, snapshot_dir, args.rows_per_shard
                )
                object_files.extend(data_files)

            all_files.extend(object_files)
            manifest["objects"].append(
                {
                    "table_id": table.table_id,
                    "type": table.table_type,
                    "source_rows": int(table.num_rows or 0),
                    "exported_rows": exported_rows,
                    "logical_bytes": int(table.num_bytes or 0),
                    "expires": table.expires.isoformat() if table.expires else None,
                    "files": [file["path"] for file in object_files],
                }
            )

        checksum_path = _write_checksums(snapshot_dir, all_files)
        manifest["status"] = "COMPLETE"
        manifest["completed_at"] = _utc_now().isoformat()
        manifest["files"] = all_files
        manifest["checksums_file"] = checksum_path.relative_to(snapshot_dir).as_posix()
        manifest["totals"] = {
            "objects": len(manifest["objects"]),
            "physical_objects": sum(
                item["type"] in DATA_OBJECT_TYPES for item in manifest["objects"]
            ),
            "source_rows": sum(item["source_rows"] for item in manifest["objects"]),
            "exported_rows": sum(item["exported_rows"] for item in manifest["objects"]),
            "logical_bytes": sum(item["logical_bytes"] for item in manifest["objects"]),
            "stored_bytes": sum(item["bytes"] for item in all_files),
            "files": len(all_files),
        }
        _write_json(snapshot_dir / "manifest.json", manifest)
    except Exception as exc:
        manifest["status"] = "FAILED"
        manifest["failed_at"] = _utc_now().isoformat()
        manifest["error"] = f"{type(exc).__name__}: {exc}"
        _write_json(snapshot_dir / "manifest.json", manifest)
        raise

    result = verify_snapshot(snapshot_dir)
    print(f"Local verification PASS: files={result['files']} bytes={result['bytes']}")
    return snapshot_dir


def upload_snapshot(snapshot_dir: Path, bucket_id: str, *, create_bucket: bool) -> dict[str, Any]:
    try:
        from huggingface_hub import (
            batch_bucket_files,
            bucket_info,
            create_bucket as hf_create_bucket,
            list_bucket_tree,
            sync_bucket,
        )
    except ImportError as exc:  # pragma: no cover - operational dependency
        raise RuntimeError("Install requirements-backup.txt before uploading") from exc

    verification = verify_snapshot(snapshot_dir)
    if create_bucket:
        hf_create_bucket(bucket_id, private=True, exist_ok=True)
    info = bucket_info(bucket_id)
    if not info.private:
        raise RuntimeError(f"Refusing to upload a backup to public bucket {bucket_id}")

    manifest = json.loads((snapshot_dir / "manifest.json").read_text(encoding="utf-8"))
    prefix = f"bigquery/snapshots/{manifest['snapshot_id']}"
    destination = f"hf://buckets/{bucket_id}/{prefix}"
    print(f"Uploading {snapshot_dir} -> {destination}", flush=True)
    sync_bucket(str(snapshot_dir), destination)

    expected = {
        f"{prefix}/{path.relative_to(snapshot_dir).as_posix()}": path.stat().st_size
        for path in snapshot_dir.rglob("*")
        if path.is_file()
    }
    remote = {
        item.path: item.size
        for item in list_bucket_tree(bucket_id, prefix=prefix, recursive=True)
        if getattr(item, "type", "file") == "file"
    }
    missing = sorted(set(expected) - set(remote))
    wrong_size = sorted(path for path, size in expected.items() if remote.get(path) != size)
    if missing or wrong_size:
        raise RuntimeError(
            f"Remote verification failed: missing={missing[:5]}, wrong_size={wrong_size[:5]}"
        )

    receipt = {
        "status": "VERIFIED",
        "verified_at": _utc_now().isoformat(),
        "bucket_id": bucket_id,
        "prefix": prefix,
        "files": len(expected),
        "bytes": sum(expected.values()),
        "local_verified_files": verification["files"],
        "local_verified_bytes": verification["bytes"],
    }
    receipt_path = snapshot_dir / "huggingface_upload_receipt.json"
    _write_json(receipt_path, receipt)
    batch_bucket_files(bucket_id, add=[(str(receipt_path), f"{prefix}/{receipt_path.name}")])
    print(
        f"Remote verification PASS: bucket={bucket_id} prefix={prefix} "
        f"files={receipt['files']} bytes={receipt['bytes']}"
    )
    return receipt


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only BigQuery snapshot with optional private Hugging Face upload"
    )
    parser.add_argument("--project", default=settings.project_id)
    parser.add_argument("--dataset", default=settings.dataset)
    parser.add_argument("--location", default="europe-west1")
    parser.add_argument("--output-root", default="backups/bigquery")
    parser.add_argument("--snapshot-id", type=_validate_snapshot_id, default=_snapshot_id())
    parser.add_argument("--table", action="append", help="Back up only this table/view; repeatable")
    parser.add_argument("--rows-per-shard", type=int, default=250_000)
    parser.add_argument("--metadata-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify", type=Path, help="Verify an existing local snapshot and exit")
    parser.add_argument("--upload", type=Path, help="Upload an existing complete snapshot")
    parser.add_argument("--hf-bucket", help="Hugging Face bucket as namespace/name")
    parser.add_argument("--create-hf-bucket", action="store_true")
    args = parser.parse_args()
    if args.rows_per_shard < 1:
        parser.error("--rows-per-shard must be positive")
    if (args.upload or args.create_hf_bucket) and not args.hf_bucket:
        parser.error("--hf-bucket is required for upload or bucket creation")
    return args


def main() -> int:
    args = parse_args()
    if args.verify:
        result = verify_snapshot(args.verify.resolve())
        print(f"Local verification PASS: files={result['files']} bytes={result['bytes']}")
        return 0
    if args.upload:
        upload_snapshot(args.upload.resolve(), args.hf_bucket, create_bucket=args.create_hf_bucket)
        return 0
    snapshot_dir = create_local_snapshot(args)
    if args.hf_bucket and not args.dry_run:
        upload_snapshot(snapshot_dir, args.hf_bucket, create_bucket=args.create_hf_bucket)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
