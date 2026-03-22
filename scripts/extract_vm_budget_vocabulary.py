#!/usr/bin/env python3
"""
Extract structured budget vocabulary from VM budget numerotaulu segments.

Parses the line-broken numerotaulu text (2002–2025) into a canonical vocabulary
table: (hierarchy_code, official_name_fi, year, fiscal_side).

Handles two distinct formats:

**Modern format (2012+):** code + name on same line, amount on next line:
    11. VEROT JA VERONLUONTEISET TULOT
    66 270 150
    01. Tulon ja varallisuuden perusteella kannettavat verot
    33 392 000

**Legacy format (2002–2011):** code on one line, name on next, then amount(s):
    11.
    VEROT JA VERONLUONTEISET TULOT
    30 318 690 000
    180 266 745 000   (FIM column, 2002 only)

Output: data/semantic_enrichment/vm_budget_site/vm_budget_vocabulary.jsonl
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

# ── Paths ──────────────────────────────────────────────────────────────
DEFAULT_SEGMENTS_PATH = Path("data/semantic_enrichment/vm_budget_site/segments_2002_2025.jsonl")
DEFAULT_OUTPUT_DIR = Path("data/semantic_enrichment/vm_budget_site")
DEFAULT_REPORT_PATH = Path("docs/reports/vm_budget_vocabulary_report.md")

# ── Regex patterns ─────────────────────────────────────────────────────

# Modern format: "01. Tulo- ja varallisuusvero"
BUDGET_LINE_RE = re.compile(r"^(\d{2})\.\s+(.+?)$")

# Legacy format: bare "01." on its own line
BARE_CODE_RE = re.compile(r"^(\d{2})\.$")

# Numeric amount line
AMOUNT_LINE_RE = re.compile(r"^-?\s*[\d ]{3,}$")

# Unit markers
UNIT_LINE_RE = re.compile(r"^(1000\s*€|euroa|mk)$", re.IGNORECASE)

# Section headers
REVENUE_HEADER_RE = re.compile(r"^TULOARVIOT", re.IGNORECASE)
EXPENDITURE_HEADER_RE = re.compile(r"^MÄÄRÄRAHAT", re.IGNORECASE)

# "Osasto 11" or "Pääluokka 28" header in legacy format
OSASTO_HEADER_RE = re.compile(r"^(?:Osasto|Pääluokka)\s+(\d{2})\b", re.IGNORECASE)

# Skip lines: "Selvitysosa:", "Yhteensä", etc.
SKIP_RE = re.compile(r"^(Selvitysosa|Yhteensä|Yhteensä\s|Yhteenveto)", re.IGNORECASE)

# Budget type annotations: (arviomääräraha), (siirtomääräraha 2 v), (kiinteä määräraha)
BUDGET_TYPE_RE = re.compile(r"\((?:arviomääräraha|siirtomääräraha\s+\d+\s+v|kiinteä\s+määräraha)\)")

# Supplementary budget line suffix: ", lisäystä" or ", vähennystä"
SUPPLEMENTARY_SUFFIX_RE = re.compile(r",\s*(lisäystä|vähennystä)$")


class BudgetEntry(NamedTuple):
    year: int
    fiscal_side: str
    hierarchy_level: str
    hierarchy_code: str
    official_name_fi: str
    amount_1000eur: int | None
    stage_key: str


def _normalize_line(line: str) -> str:
    return " ".join(line.split()).strip()


def _parse_amount(text: str) -> int | None:
    cleaned = text.strip().replace(" ", "").replace("\xa0", "")
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _is_upper_heading(name: str) -> bool:
    letters = [c for c in name if c.isalpha()]
    if len(letters) < 3:
        return False
    return sum(1 for c in letters if c.isupper()) / len(letters) > 0.7


def _clean_name(name: str) -> str:
    """Remove budget type annotation and supplementary suffixes from name."""
    name = BUDGET_TYPE_RE.sub("", name).strip()
    name = SUPPLEMENTARY_SUFFIX_RE.sub("", name).strip()
    return name


def _looks_like_name(text: str) -> bool:
    """Check if a line looks like a budget item name (not a number, not a header)."""
    if not text or AMOUNT_LINE_RE.match(text) or UNIT_LINE_RE.match(text):
        return False
    if SKIP_RE.match(text):
        return False
    if REVENUE_HEADER_RE.match(text) or EXPENDITURE_HEADER_RE.match(text):
        return False
    # Must have some alphabetic content
    return any(c.isalpha() for c in text)


def parse_numerotaulu(text: str, year: int, stage_key: str) -> list[BudgetEntry]:
    """
    Parse a complete numerotaulu text into structured budget entries.

    Handles both modern (code+name on same line) and legacy (code on separate line)
    formats by first trying the modern pattern, then falling back to legacy.
    """
    lines = [_normalize_line(line) for line in text.splitlines()]
    lines = [line for line in lines if line]

    entries: list[BudgetEntry] = []
    fiscal_side = "unknown"
    current_osasto: str | None = None
    current_luku: str | None = None

    # Skip supplementary budget documents — they use different structure
    # with "lisäystä"/"vähennystä" that pollutes vocabulary
    is_supplementary = stage_key in ("supplementary_budget",)

    i = 0
    while i < len(lines):
        line = lines[i]

        # Detect fiscal side switches
        if REVENUE_HEADER_RE.match(line):
            fiscal_side = "revenue"
            i += 1
            continue
        if EXPENDITURE_HEADER_RE.match(line):
            fiscal_side = "expense"
            i += 1
            continue

        # Skip "Osasto XX" / "Pääluokka XX" header lines (legacy format)
        osasto_match = OSASTO_HEADER_RE.match(line)
        if osasto_match:
            i += 1
            continue

        # Skip unit markers, amount lines, and other non-data lines
        if UNIT_LINE_RE.match(line) or AMOUNT_LINE_RE.match(line) or SKIP_RE.match(line):
            i += 1
            continue

        # ── Try modern format: "01. Name" ──
        modern_match = BUDGET_LINE_RE.match(line)
        if modern_match:
            code_2d = modern_match.group(1)
            raw_name = modern_match.group(2).strip()

            # Skip supplementary budget adjustment lines
            if SUPPLEMENTARY_SUFFIX_RE.search(raw_name) and is_supplementary:
                i += 1
                continue

            # Look ahead for amount
            amount = None
            if i + 1 < len(lines) and AMOUNT_LINE_RE.match(lines[i + 1]):
                amount = _parse_amount(lines[i + 1])

            entry = _classify_and_create(
                code_2d, raw_name, amount, year, fiscal_side, stage_key,
                current_osasto, current_luku,
            )
            if entry:
                entries.append(entry)
                if entry.hierarchy_level == "osasto":
                    current_osasto = entry.hierarchy_code
                    current_luku = None
                elif entry.hierarchy_level == "luku":
                    current_luku = entry.hierarchy_code
            i += 1
            continue

        # ── Try legacy format: bare "01." ──
        bare_match = BARE_CODE_RE.match(line)
        if bare_match:
            code_2d = bare_match.group(1)

            # Look ahead for name on next line
            if i + 1 < len(lines) and _looks_like_name(lines[i + 1]):
                raw_name = lines[i + 1]
                # Look for amount after name
                amount = None
                if i + 2 < len(lines) and AMOUNT_LINE_RE.match(lines[i + 2]):
                    amount = _parse_amount(lines[i + 2])

                if SUPPLEMENTARY_SUFFIX_RE.search(raw_name) and is_supplementary:
                    i += 2
                    continue

                entry = _classify_and_create(
                    code_2d, raw_name, amount, year, fiscal_side, stage_key,
                    current_osasto, current_luku,
                )
                if entry:
                    entries.append(entry)
                    if entry.hierarchy_level == "osasto":
                        current_osasto = entry.hierarchy_code
                        current_luku = None
                    elif entry.hierarchy_level == "luku":
                        current_luku = entry.hierarchy_code
                i += 2  # Skip code + name lines
                continue

        i += 1

    return entries


def _classify_and_create(
    code_2d: str,
    raw_name: str,
    amount: int | None,
    year: int,
    fiscal_side: str,
    stage_key: str,
    current_osasto: str | None,
    current_luku: str | None,
) -> BudgetEntry | None:
    """Determine hierarchy level and create a BudgetEntry."""
    name_clean = _clean_name(raw_name)
    has_budget_type = bool(BUDGET_TYPE_RE.search(raw_name))

    if _is_upper_heading(raw_name):
        # Osasto level
        return BudgetEntry(
            year=year, fiscal_side=fiscal_side,
            hierarchy_level="osasto", hierarchy_code=f"{code_2d}.",
            official_name_fi=name_clean, amount_1000eur=amount,
            stage_key=stage_key,
        )

    if current_osasto is None:
        return None

    if has_budget_type and current_luku:
        # Has budget type annotation → momentti
        return BudgetEntry(
            year=year, fiscal_side=fiscal_side,
            hierarchy_level="momentti",
            hierarchy_code=f"{current_luku}{code_2d}.",
            official_name_fi=name_clean, amount_1000eur=amount,
            stage_key=stage_key,
        )

    if current_luku is None:
        # First sub-item under osasto → luku
        return BudgetEntry(
            year=year, fiscal_side=fiscal_side,
            hierarchy_level="luku",
            hierarchy_code=f"{current_osasto}{code_2d}.",
            official_name_fi=name_clean, amount_1000eur=amount,
            stage_key=stage_key,
        )

    # Heuristic: if no budget type and we already have a luku,
    # this is likely a new luku (not a momentti without annotation).
    # But in revenue sections, momentti often lacks the annotation.
    # Use context: if code suggests reset (smaller than last luku code), it's a new luku.
    current_luku_code = current_luku.split(".")[1] if current_luku else ""

    if not has_budget_type:
        # Items without budget type annotation in expense section are usually luku
        # In revenue section, they can be either
        if fiscal_side == "expense":
            return BudgetEntry(
                year=year, fiscal_side=fiscal_side,
                hierarchy_level="luku",
                hierarchy_code=f"{current_osasto}{code_2d}.",
                official_name_fi=name_clean, amount_1000eur=amount,
                stage_key=stage_key,
            )
        else:
            # Revenue side: treat as momentti if we have a luku
            return BudgetEntry(
                year=year, fiscal_side=fiscal_side,
                hierarchy_level="momentti",
                hierarchy_code=f"{current_luku}{code_2d}.",
                official_name_fi=name_clean, amount_1000eur=amount,
                stage_key=stage_key,
            )

    return None


def _read_jsonl(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def build_vocabulary(segments: list[dict]) -> list[dict]:
    """
    Extract vocabulary from all numerotaulu segments across all years.

    For each (hierarchy_code, year) pair, picks the best entry using
    stage priority: current_combined > budget_proposal > others.
    """
    STAGE_PRIORITY = {
        "current_combined": 0,
        "budget_proposal": 1,
        "complementary_budget_proposal": 2,
        "document_bundle": 3,
        "ministry_proposal": 4,
        "supplementary_budget": 5,
        "other": 6,
    }

    all_entries: list[BudgetEntry] = []
    parsed_count = 0
    for segment in segments:
        node_label = segment.get("node_label", "").lower()
        heading = segment.get("heading", "").lower()
        if "numerotaulu" not in node_label and "numerotaulu" not in heading:
            continue
        text = segment.get("text", "")
        if not text:
            continue
        entries = parse_numerotaulu(text, segment["year"], segment.get("stage_key", "other"))
        all_entries.extend(entries)
        parsed_count += 1

    print(f"Parsed {parsed_count} numerotaulu segments → {len(all_entries)} raw entries")

    # Deduplicate: for each (code, year), keep the highest-priority stage
    best: dict[tuple[str, int], BudgetEntry] = {}
    for entry in all_entries:
        key = (entry.hierarchy_code, entry.year)
        if key not in best or STAGE_PRIORITY.get(entry.stage_key, 99) < STAGE_PRIORITY.get(best[key].stage_key, 99):
            best[key] = entry

    # Group by hierarchy_code
    by_code: dict[str, list[BudgetEntry]] = defaultdict(list)
    for entry in best.values():
        by_code[entry.hierarchy_code].append(entry)

    vocab_rows: list[dict] = []
    for code in sorted(by_code.keys()):
        entries = sorted(by_code[code], key=lambda e: e.year)
        year_first = entries[0].year
        year_last = entries[-1].year
        years_present = sorted(set(e.year for e in entries))

        # Collect name variants
        name_variants: dict[str, list[int]] = defaultdict(list)
        for e in entries:
            name_variants[e.official_name_fi].append(e.year)

        canonical_name = entries[-1].official_name_fi

        fiscal_sides = Counter(e.fiscal_side for e in entries)
        fiscal_side = fiscal_sides.most_common(1)[0][0]
        hierarchy_level = entries[0].hierarchy_level

        # Build name tokens for fuzzy matching
        name_tokens = sorted(set(
            token.lower()
            for name in name_variants
            for token in name.split()
            if len(token) > 2 and not token.isdigit()
        ))

        row = {
            "hierarchy_code": code,
            "hierarchy_level": hierarchy_level,
            "canonical_name_fi": canonical_name,
            "fiscal_side": fiscal_side,
            "year_first": year_first,
            "year_last": year_last,
            "years_present": years_present,
            "year_count": len(years_present),
            "name_variants": [
                {"name": name, "years": yrs}
                for name, yrs in sorted(name_variants.items(), key=lambda x: max(x[1]), reverse=True)
            ],
            "name_variant_count": len(name_variants),
            "name_tokens": name_tokens,
            "is_stable": len(name_variants) == 1,
            "latest_amount_1000eur": entries[-1].amount_1000eur,
        }
        vocab_rows.append(row)

    return vocab_rows


def _render_report(path: Path, vocab_rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    by_level = Counter(r["hierarchy_level"] for r in vocab_rows)
    by_side = Counter(r["fiscal_side"] for r in vocab_rows)
    renamed = [r for r in vocab_rows if r["name_variant_count"] > 1]
    stable = [r for r in vocab_rows if r["is_stable"]]
    long_lived = [r for r in vocab_rows if r["year_count"] >= 15]

    with path.open("w", encoding="utf-8") as f:
        f.write("# VM Budget Vocabulary Extraction Report\n\n")
        f.write(f"- Generated: {datetime.now(timezone.utc).isoformat()}\n")
        f.write(f"- Total vocabulary entries: {len(vocab_rows)}\n")
        f.write(f"- Stable entries (no name changes): {len(stable)}\n")
        f.write(f"- Renamed entries (name changed across years): {len(renamed)}\n")
        f.write(f"- Long-lived entries (15+ years): {len(long_lived)}\n\n")

        f.write("## By hierarchy level\n")
        for level, count in sorted(by_level.items()):
            f.write(f"- {level}: {count}\n")

        f.write("\n## By fiscal side\n")
        for side, count in sorted(by_side.items()):
            f.write(f"- {side}: {count}\n")

        # Year coverage
        all_years = set()
        for r in vocab_rows:
            all_years.update(r["years_present"])
        f.write(f"\n## Year coverage\n")
        f.write(f"- Years: {min(all_years)}–{max(all_years)} ({len(all_years)} years)\n")
        year_counts = Counter(y for r in vocab_rows for y in r["years_present"])
        for y in sorted(year_counts.keys()):
            f.write(f"  - {y}: {year_counts[y]} entries\n")

        f.write("\n## Name changes detected (renamed budget items)\n\n")
        for row in sorted(renamed, key=lambda r: r["hierarchy_code"])[:30]:
            f.write(f"### {row['hierarchy_code']} ({row['hierarchy_level']})\n")
            f.write(f"- Current: **{row['canonical_name_fi']}**\n")
            f.write(f"- Years: {row['year_first']}–{row['year_last']} ({row['year_count']} years)\n")
            for variant in row["name_variants"]:
                yr = variant["years"]
                year_range = f"{min(yr)}–{max(yr)}" if len(yr) > 1 else str(yr[0])
                f.write(f"  - \"{variant['name']}\" ({year_range})\n")
            f.write("\n")

        f.write("## Sample vocabulary entries\n\n")
        for level in ("osasto", "luku", "momentti"):
            level_rows = [r for r in vocab_rows if r["hierarchy_level"] == level]
            f.write(f"### {level.title()} examples (showing first 10)\n")
            for row in level_rows[:10]:
                f.write(f"- `{row['hierarchy_code']}` {row['canonical_name_fi']}")
                f.write(f" ({row['year_first']}–{row['year_last']})")
                if row['latest_amount_1000eur']:
                    f.write(f" [{row['latest_amount_1000eur']:,}k]")
                f.write("\n")
            f.write("\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract structured budget vocabulary from VM budget numerotaulu segments."
    )
    parser.add_argument("--segments-path", type=Path, default=DEFAULT_SEGMENTS_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    segments = _read_jsonl(args.segments_path)
    vocab_rows = build_vocabulary(segments)

    output_path = args.output_dir / "vm_budget_vocabulary.jsonl"
    _write_jsonl(output_path, vocab_rows)
    _render_report(args.report_path, vocab_rows)

    print(f"Vocabulary entries: {len(vocab_rows)} → {output_path}")
    print(f"Report: {args.report_path}")

    by_level = Counter(r["hierarchy_level"] for r in vocab_rows)
    renamed = sum(1 for r in vocab_rows if r["name_variant_count"] > 1)
    print(f"  osasto: {by_level.get('osasto', 0)}, luku: {by_level.get('luku', 0)}, momentti: {by_level.get('momentti', 0)}")
    print(f"  Name variants detected: {renamed} entries have changed names across years")


if __name__ == "__main__":
    main()
