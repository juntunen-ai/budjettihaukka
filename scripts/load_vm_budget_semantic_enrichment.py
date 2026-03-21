#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings

DEFAULT_INPUT_DIR = ROOT / 'data' / 'semantic_enrichment' / 'vm_budget_site'

TABLE_SPECS = {
    'vm_budget_document_catalog': 'catalog_{label}.jsonl',
    'vm_budget_document_segments': 'segments_{label}.jsonl',
    'vm_budget_semantic_evidence': 'semantic_evidence_{label}.jsonl',
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Load VM budget semantic enrichment JSONL files into BigQuery.')
    parser.add_argument('--project', default=settings.project_id)
    parser.add_argument('--dataset', default=settings.dataset)
    parser.add_argument('--input-dir', type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument('--label', default='2002_2025', help='File label suffix, e.g. 2002_2025')
    parser.add_argument('--table-prefix', default='', help='Optional prefix for created tables')
    return parser.parse_args()


def _table_name(base_name: str, prefix: str) -> str:
    normalized = prefix.strip('_')
    if not normalized:
        return base_name
    return f'{normalized}_{base_name}'


def _load_jsonl(project: str, dataset: str, table_name: str, path: Path) -> None:
    cmd = [
        'bq',
        'load',
        '--replace',
        '--project_id',
        project,
        '--source_format=NEWLINE_DELIMITED_JSON',
        '--autodetect',
        f'{project}:{dataset}.{table_name}',
        str(path),
    ]
    subprocess.run(cmd, check=True)


def main() -> None:
    args = _parse_args()
    for base_name, pattern in TABLE_SPECS.items():
        path = args.input_dir / pattern.format(label=args.label)
        if not path.exists():
            raise SystemExit(f'Missing input file: {path}')
        table_name = _table_name(base_name, args.table_prefix)
        print(f'Loading {path.name} -> {args.project}.{args.dataset}.{table_name}')
        _load_jsonl(args.project, args.dataset, table_name, path)


if __name__ == '__main__':
    main()
