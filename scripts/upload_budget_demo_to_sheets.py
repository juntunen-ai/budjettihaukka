#!/usr/bin/env python3
"""Upload 2022-2024 BudjettitaloudenTapahtumat CSV to Google Sheets.

The dataset exceeds 10M cells if combined into one spreadsheet, so this script
creates one spreadsheet per year and uploads full rows for each year.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from collections import Counter
from pathlib import Path

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def col_to_a1(col_index_1_based: int) -> str:
    letters = []
    n = col_index_1_based
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(65 + rem))
    return "".join(reversed(letters))


def count_rows_per_year(csv_path: Path) -> Counter[str]:
    counts: Counter[str] = Counter()
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            counts[row["Vuosi"]] += 1
    return counts


def stream_year_rows(csv_path: Path, year: str, header: list[str]):
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["Vuosi"] == year:
                yield [row.get(col, "") for col in header]


def create_spreadsheet(sheets_service, title: str, tab_name: str) -> tuple[str, int]:
    body = {
        "properties": {"title": title},
        "sheets": [{"properties": {"title": tab_name}}],
    }
    resp = sheets_service.spreadsheets().create(body=body, fields="spreadsheetId,sheets.properties").execute()
    spreadsheet_id = resp["spreadsheetId"]
    sheet_id = resp["sheets"][0]["properties"]["sheetId"]
    return spreadsheet_id, sheet_id


def get_or_create_tab(sheets_service, spreadsheet_id: str, tab_name: str) -> int:
    meta = sheets_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets.properties(sheetId,title)",
    ).execute()
    sheets = meta.get("sheets", [])
    for sheet in sheets:
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            return props["sheetId"]

    resp = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]},
    ).execute()
    return resp["replies"][0]["addSheet"]["properties"]["sheetId"]


def resize_sheet(sheets_service, spreadsheet_id: str, sheet_id: int, rows: int, cols: int) -> None:
    body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {"rowCount": rows, "columnCount": cols},
                    },
                    "fields": "gridProperties.rowCount,gridProperties.columnCount",
                }
            }
        ]
    }
    sheets_service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()


def clear_tab_values(sheets_service, spreadsheet_id: str, tab_name: str) -> None:
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A:ZZ",
        body={},
    ).execute()


def upload_year(
    sheets_service,
    csv_path: Path,
    year: str,
    header: list[str],
    row_count: int,
    spreadsheet_id: str,
    tab_name: str,
    chunk_size: int,
) -> None:
    last_col = col_to_a1(len(header))

    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1:{last_col}1",
        valueInputOption="RAW",
        body={"values": [header]},
    ).execute()

    buffer: list[list[str]] = []
    start_row = 2
    uploaded = 0

    for row in stream_year_rows(csv_path, year, header):
        buffer.append(row)
        if len(buffer) >= chunk_size:
            end_row = start_row + len(buffer) - 1
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{tab_name}!A{start_row}:{last_col}{end_row}",
                valueInputOption="RAW",
                body={"values": buffer},
            ).execute()
            uploaded += len(buffer)
            print(f"{year}: uploaded {uploaded}/{row_count} rows")
            start_row = end_row + 1
            buffer = []

    if buffer:
        end_row = start_row + len(buffer) - 1
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A{start_row}:{last_col}{end_row}",
            valueInputOption="RAW",
            body={"values": buffer},
        ).execute()
        uploaded += len(buffer)
        print(f"{year}: uploaded {uploaded}/{row_count} rows")


def share_sheet(drive_service, spreadsheet_id: str, email: str) -> None:
    drive_service.permissions().create(
        fileId=spreadsheet_id,
        body={"type": "user", "role": "writer", "emailAddress": email},
        sendNotificationEmail=False,
    ).execute()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--csv-path",
        default="data/budjettitaloudentapahtumat_2022_2024_all_rows.csv",
        help="Merged CSV path",
    )
    parser.add_argument(
        "--creds-path",
        default="gcp-creds.json",
        help="Service account JSON key path",
    )
    parser.add_argument(
        "--share-email",
        default="vihreamies.juntunen@gmail.com",
        help="Google account to share spreadsheets with",
    )
    parser.add_argument(
        "--title-prefix",
        default="BudjettitaloudenTapahtumat Demo",
        help="Spreadsheet title prefix",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=2000,
        help="Rows per write request",
    )
    parser.add_argument(
        "--manifest-path",
        default="data/google_sheets_demo_manifest.json",
        help="Output JSON manifest of created spreadsheets",
    )
    parser.add_argument("--sheet-id-2022", default="", help="Existing spreadsheet ID for year 2022")
    parser.add_argument("--sheet-id-2023", default="", help="Existing spreadsheet ID for year 2023")
    parser.add_argument("--sheet-id-2024", default="", help="Existing spreadsheet ID for year 2024")
    parser.add_argument(
        "--skip-share",
        action="store_true",
        help="Skip share step (use when writing into already-shared existing sheets)",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    creds_path = Path(args.creds_path)
    manifest_path = Path(args.manifest_path)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")
    if not creds_path.exists():
        raise SystemExit(f"Credentials not found: {creds_path}")

    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)

    counts = count_rows_per_year(csv_path)
    years = sorted(counts.keys())
    print("row_counts_by_year:", dict(counts))

    creds = Credentials.from_service_account_file(str(creds_path), scopes=SCOPES)
    sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)

    created = []
    existing_ids = {
        "2022": args.sheet_id_2022.strip(),
        "2023": args.sheet_id_2023.strip(),
        "2024": args.sheet_id_2024.strip(),
    }
    for year in years:
        title = f"{args.title_prefix} {year} ({time.strftime('%Y-%m-%d')})"
        tab_name = f"data_{year}"
        row_count = counts[year]

        spreadsheet_id = existing_ids.get(year) or ""
        created_new = not bool(spreadsheet_id)
        if created_new:
            spreadsheet_id, sheet_id = create_spreadsheet(sheets_service, title=title, tab_name=tab_name)
        else:
            sheet_id = get_or_create_tab(sheets_service, spreadsheet_id=spreadsheet_id, tab_name=tab_name)
            clear_tab_values(sheets_service, spreadsheet_id=spreadsheet_id, tab_name=tab_name)

        resize_sheet(
            sheets_service,
            spreadsheet_id=spreadsheet_id,
            sheet_id=sheet_id,
            rows=row_count + 1,
            cols=len(header),
        )
        upload_year(
            sheets_service=sheets_service,
            csv_path=csv_path,
            year=year,
            header=header,
            row_count=row_count,
            spreadsheet_id=spreadsheet_id,
            tab_name=tab_name,
            chunk_size=args.chunk_size,
        )
        if (not args.skip_share) and created_new:
            share_sheet(drive_service, spreadsheet_id=spreadsheet_id, email=args.share_email)

        sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
        created.append(
            {
                "year": year,
                "spreadsheet_id": spreadsheet_id,
                "tab_name": tab_name,
                "url": sheet_url,
                "rows": row_count,
                "columns": len(header),
            }
        )
        print(f"{year}: done -> {sheet_url}")

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps({"sheets": created}, indent=2), encoding="utf-8")
    print(f"manifest written to {manifest_path}")


if __name__ == "__main__":
    main()
