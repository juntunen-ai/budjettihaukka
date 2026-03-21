#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, quote, urljoin, urlparse, urlsplit, urlunsplit

import requests

BASE_URL = "https://budjetti.vm.fi"
INDEX_URL = f"{BASE_URL}/tae/etusivu_aiemmat.jsp?lang=fi"
DEFAULT_OUTPUT_DIR = Path("data/semantic_enrichment/vm_budget_site")
DEFAULT_REPORT_DIR = Path("docs/reports")
USER_AGENT = "BudjettihaukkaSemanticIngest/1.0"
TIMEOUT = 30
MOMENTTI_CODE_RE = re.compile(r"\b\d{2}\.\d{2}\.\d{2}\.")
LUKU_CODE_RE = re.compile(r"\b\d{2}\.\d{2}\.\b")
OSASTO_CODE_RE = re.compile(r"\b\d{2}\.\s")
WHITESPACE_RE = re.compile(r"\s+")
FILE_RE = re.compile(r"var file = '([^']+)'")
TWO_DIGIT_CODE_RE = re.compile(r"^\d{2}\.$")
PAA_LUOKKA_RE = re.compile(r"^(Pääluokka|Osasto)\s+\d{2}\b", re.IGNORECASE)


@dataclass
class LinkItem:
    href: str
    text: str


class AnchorCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[LinkItem] = []
        self._current_href: str | None = None
        self._text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attrs_map = {key.lower(): value or "" for key, value in attrs}
        self._current_href = attrs_map.get("href")
        self._text_parts = []

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            text = _normalize_text(data)
            if text:
                self._text_parts.append(text)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._current_href is None:
            return
        text = _normalize_text(" ".join(self._text_parts))
        self.links.append(LinkItem(href=self._current_href, text=text))
        self._current_href = None
        self._text_parts = []


class VisibleTextCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style"}:
            self._skip_depth += 1
        elif tag.lower() in {"p", "div", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6", "td", "th", "br"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style"} and self._skip_depth > 0:
            self._skip_depth -= 1
        elif tag.lower() in {"p", "div", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6", "table"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        text = _normalize_text(data)
        if text:
            self.parts.append(text)

    def text(self) -> str:
        raw = " ".join(self.parts)
        raw = raw.replace(" \n ", "\n").replace(" \n", "\n").replace("\n ", "\n")
        lines = [_normalize_text(line) for line in raw.splitlines()]
        lines = [line for line in lines if line]
        return "\n".join(lines)


class Session:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def get(self, url: str) -> str:
        response = self.session.get(url, timeout=TIMEOUT)
        response.raise_for_status()
        return response.text


def _normalize_text(value: str | None) -> str:
    text = unescape(str(value or ""))
    text = text.replace("\xa0", " ")
    return WHITESPACE_RE.sub(" ", text).strip()


def _clean_href(href: str) -> str:
    href = (href or "").strip()
    if not href:
        return ""
    parsed = urlsplit(urljoin(BASE_URL, href))
    path = re.sub(r";jsessionid=[^?]+", "", parsed.path, flags=re.IGNORECASE)
    path = re.sub(r"/{2,}", "/", path)
    return urlunsplit((parsed.scheme, parsed.netloc, path, parsed.query, parsed.fragment))


def _absolute_download_url(file_path: str) -> str:
    return f"{BASE_URL}/download.jsp?lang=fi&file={quote(file_path, safe='/')}"


def _slug_from_url(url: str) -> str:
    path = urlparse(url).path
    slug = Path(path).name
    return slug or hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]


def _document_stage(label: str, url: str) -> tuple[str, str]:
    text = f"{label} {_slug_from_url(url)}".lower()
    if "aky_" in text or "ajantasais" in text or "yhdistelmä" in text:
        return "current_combined", "Ajantasainen yhdistelmä"
    if "vmkanta" in text or "valtiovarainministeriön ehdotus" in text or "etusivu_vm" in text:
        return "ministry_proposal", "Valtiovarainministeriön ehdotus"
    if "he_taet" in text or "täydentävä" in text:
        return "complementary_budget_proposal", "Täydentävä talousarvioesitys"
    if "he_tae" in text:
        return "budget_proposal", "Talousarvioesitys"
    if "ltae" in text:
        return "supplementary_budget", "Lisätalousarvioesitys"
    if "asiakirjayhdistel" in text:
        return "document_bundle", "Asiakirjayhdistelmä"
    return "other", "Muu budjettiasiakirja"


def _parse_query_params(url: str) -> dict[str, str]:
    values = parse_qs(urlparse(url).query)
    return {key: vals[0] for key, vals in values.items() if vals}


def _extract_links(html: str) -> list[LinkItem]:
    parser = AnchorCollector()
    parser.feed(html)
    return parser.links


def _extract_visible_text(html: str) -> str:
    parser = VisibleTextCollector()
    parser.feed(html)
    return parser.text()


def _next_nonempty(lines: list[str], start: int) -> str:
    for index in range(start, len(lines)):
        if lines[index]:
            return lines[index]
    return ""


def _is_upper_heading(value: str) -> bool:
    letters = [char for char in value if char.isalpha()]
    if not letters:
        return False
    upper_ratio = sum(1 for char in letters if char.isupper()) / len(letters)
    return upper_ratio > 0.8


def _looks_like_momentti_label(value: str) -> bool:
    lowered = value.lower()
    return "(" in value or "määräraha" in lowered or "tuloarvio" in lowered


def _extract_hierarchy_refs(text: str) -> tuple[list[str], list[str], list[str]]:
    """
    Reconstruct osasto/luku/momentti references from line-broken historical budget HTML.

    The old VM pages often render the hierarchy as:

        35.
        YMPÄRISTÖMINISTERIÖN HALLINNONALA
        ...
        30.
        Asumisen edistäminen
        ...
        54.
        Asumistuki (arviomääräraha)

    In those cases direct regex matching for 35.30.54. fails, so we keep a small state
    machine over non-empty lines and rebuild the hierarchy from context.
    """

    lines = [_normalize_text(line) for line in text.splitlines()]
    lines = [line for line in lines if line]

    osastot: list[str] = []
    luvut: list[str] = []
    momentit: list[str] = []
    current_osasto: str | None = None
    current_luku: str | None = None
    pending_context: str | None = None

    for index, line in enumerate(lines):
        next_line = _next_nonempty(lines, index + 1)

        if PAA_LUOKKA_RE.match(line):
            pending_context = "osasto"
            continue

        if not TWO_DIGIT_CODE_RE.match(line):
            continue

        code = line[:2]

        if pending_context == "osasto" or _is_upper_heading(next_line):
            current_osasto = f"{code}."
            current_luku = None
            pending_context = None
            osastot.append(current_osasto)
            continue

        if current_osasto and current_luku and _looks_like_momentti_label(next_line):
            luku_code = current_luku.split(".")[1]
            momentit.append(f"{current_osasto}{luku_code}.{code}.")
            continue

        if current_osasto:
            current_luku = f"{current_osasto}{code}."
            luvut.append(current_luku)
            continue

    return sorted(set(osastot)), sorted(set(luvut)), sorted(set(momentit))


def fetch_year_documents(client: Session, year: int) -> list[dict]:
    url = f"{BASE_URL}/tae/frame_year.jsp?year={year}&lang=fi"
    html = client.get(url)
    rows: list[dict] = []
    seen: set[str] = set()
    for link in _extract_links(html):
        href = _clean_href(link.href)
        if not re.search(rf"/indox/tae/{year}/", href) or not href.endswith(".jsp"):
            continue
        if href in seen:
            continue
        seen.add(href)
        stage_key, stage_label = _document_stage(link.text, href)
        rows.append(
            {
                "year": year,
                "document_label": link.text or _slug_from_url(href),
                "document_url": href,
                "document_slug": _slug_from_url(href).removesuffix(".jsp"),
                "stage_key": stage_key,
                "stage_label": stage_label,
                "source_page_url": url,
            }
        )
    return rows


def fetch_document_nodes(client: Session, document_row: dict) -> list[dict]:
    html = client.get(document_row["document_url"])
    rows: list[dict] = []
    seen: set[str] = set()
    for link in _extract_links(html):
        href = _clean_href(link.href)
        if "/sisalto.jsp" not in href:
            continue
        params = _parse_query_params(href)
        maindoc = params.get("maindoc", "")
        opennode = params.get("opennode", "")
        if not maindoc or not opennode:
            continue
        node_id = f"{document_row['document_slug']}::{opennode}"
        if node_id in seen:
            continue
        seen.add(node_id)
        rows.append(
            {
                "year": document_row["year"],
                "document_slug": document_row["document_slug"],
                "document_label": document_row["document_label"],
                "document_url": document_row["document_url"],
                "stage_key": document_row["stage_key"],
                "stage_label": document_row["stage_label"],
                "node_label": link.text or opennode,
                "node_url": href,
                "opennode": opennode,
                "maindoc": maindoc,
            }
        )
    return rows


def fetch_node_segment(client: Session, node_row: dict) -> dict | None:
    html = client.get(node_row["node_url"])
    match = FILE_RE.search(html)
    if not match:
        return None
    file_path = match.group(1)
    content_url = _absolute_download_url(file_path)
    content_html = client.get(content_url)
    text = _extract_visible_text(content_html)
    if not text:
        return None
    heading = text.splitlines()[0].strip()
    regex_momentit = sorted(set(MOMENTTI_CODE_RE.findall(text)))
    inferred_osastot, inferred_luvut, inferred_momentit = _extract_hierarchy_refs(text)
    momentit = sorted(set(regex_momentit) | set(inferred_momentit))
    luvut = inferred_luvut
    osastot = inferred_osastot
    digest = hashlib.sha1(f"{node_row['node_url']}|{file_path}|{text}".encode("utf-8")).hexdigest()
    return {
        **node_row,
        "content_file_path": file_path,
        "content_url": content_url,
        "heading": heading,
        "text": text,
        "text_length": len(text),
        "momentti_codes": momentit,
        "luku_codes": luvut,
        "osasto_codes": osastot,
        "content_hash": digest,
    }


def crawl_budget_site(
    years: Iterable[int],
    *,
    max_documents_per_year: int | None = None,
    max_nodes_per_document: int | None = None,
    sleep_seconds: float = 0.0,
) -> tuple[list[dict], list[dict]]:
    client = Session()
    catalog_rows: list[dict] = []
    segment_rows: list[dict] = []
    for year in years:
        documents = fetch_year_documents(client, year)
        if max_documents_per_year is not None:
            documents = documents[:max_documents_per_year]
        for document in documents:
            catalog_rows.append(document)
            nodes = fetch_document_nodes(client, document)
            if max_nodes_per_document is not None:
                nodes = nodes[:max_nodes_per_document]
            for node in nodes:
                segment = fetch_node_segment(client, node)
                if segment:
                    segment_rows.append(segment)
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
    return catalog_rows, segment_rows


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            serializable = dict(row)
            handle.write(json.dumps(serializable, ensure_ascii=False) + "\n")


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            normalized = {key: row.get(key) for key in fieldnames}
            for key, value in list(normalized.items()):
                if isinstance(value, list):
                    normalized[key] = " | ".join(str(item) for item in value)
            writer.writerow(normalized)


def _render_report(report_path: Path, years: list[int], catalog_rows: list[dict], segment_rows: list[dict]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    stage_counts = Counter(row["stage_label"] for row in catalog_rows)
    year_counts = Counter(row["year"] for row in catalog_rows)
    doc_segment_counts = Counter(row["document_slug"] for row in segment_rows)
    sample_segments = segment_rows[:10]
    with report_path.open("w", encoding="utf-8") as handle:
        handle.write("# VM Budjettisivuston Semantic Enrichment -pilotti\n\n")
        handle.write(f"- Aikaleima: {datetime.now(timezone.utc).isoformat()}\n")
        handle.write(f"- Vuodet: {min(years)}-{max(years)}\n")
        handle.write(f"- Dokumentteja: {len(catalog_rows)}\n")
        handle.write(f"- Tekstisegmenttejä: {len(segment_rows)}\n\n")
        handle.write("## Dokumentit vaiheittain\n")
        for label, count in sorted(stage_counts.items()):
            handle.write(f"- {label}: {count}\n")
        handle.write("\n## Dokumentit vuosittain\n")
        for year, count in sorted(year_counts.items()):
            handle.write(f"- {year}: {count}\n")
        handle.write("\n## Segmenttirikkaimmat dokumentit\n")
        for slug, count in doc_segment_counts.most_common(10):
            handle.write(f"- {slug}: {count}\n")
        handle.write("\n## Esimerkkisegmentit\n")
        for row in sample_segments:
            snippet = row["text"][:500].replace("\n", " ")
            handle.write(f"- {row['year']} / {row['document_label']} / {row['node_label']}\n")
            handle.write(f"  - heading: {row['heading']}\n")
            handle.write(f"  - source: {row['content_url']}\n")
            handle.write(f"  - snippet: {snippet}\n")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest historical VM budget-site documents for semantic enrichment.")
    parser.add_argument("--years", nargs="+", type=int, default=[2002, 2003, 2004, 2005])
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_DIR / "vm_budget_semantic_enrichment_pilot.md")
    parser.add_argument("--max-documents-per-year", type=int, default=6)
    parser.add_argument("--max-nodes-per-document", type=int, default=18)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    years = sorted(set(args.years))
    max_documents_per_year = None if args.max_documents_per_year <= 0 else args.max_documents_per_year
    max_nodes_per_document = None if args.max_nodes_per_document <= 0 else args.max_nodes_per_document
    catalog_rows, segment_rows = crawl_budget_site(
        years,
        max_documents_per_year=max_documents_per_year,
        max_nodes_per_document=max_nodes_per_document,
        sleep_seconds=args.sleep_seconds,
    )

    run_label = f"{min(years)}_{max(years)}"
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    catalog_jsonl = output_dir / f"catalog_{run_label}.jsonl"
    catalog_csv = output_dir / f"catalog_{run_label}.csv"
    segments_jsonl = output_dir / f"segments_{run_label}.jsonl"
    segments_csv = output_dir / f"segments_{run_label}.csv"

    _write_jsonl(catalog_jsonl, catalog_rows)
    _write_jsonl(segments_jsonl, segment_rows)
    _write_csv(
        catalog_csv,
        catalog_rows,
        ["year", "document_label", "document_slug", "stage_key", "stage_label", "document_url", "source_page_url"],
    )
    _write_csv(
        segments_csv,
        segment_rows,
        [
            "year",
            "document_slug",
            "document_label",
            "stage_key",
            "stage_label",
            "node_label",
            "node_url",
            "opennode",
            "heading",
            "text_length",
            "momentti_codes",
            "luku_codes",
            "osasto_codes",
            "content_url",
        ],
    )
    _render_report(args.report_path, years, catalog_rows, segment_rows)

    print(f"Years: {years}")
    print(f"Catalog rows: {len(catalog_rows)} -> {catalog_jsonl}")
    print(f"Segment rows: {len(segment_rows)} -> {segments_jsonl}")
    print(f"Report: {args.report_path}")


if __name__ == "__main__":
    main()
