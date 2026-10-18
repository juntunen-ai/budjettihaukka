"""Build the public 2027 accounting dataset from a locally corrected CSV.

Usage: python scripts/build_liberaali_2027.py --csv /path/to/corrected.csv
The allowlist deliberately excludes working formulas, explanations, comments,
personal names, and historical working columns from the internal source.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def amount(value: str) -> int | None:
    if not value.strip():
        return None
    return int(Decimal(''.join(value.split()).replace('−', '-').replace(',', '.')))


def build(path: Path) -> dict:
    raw = path.read_bytes()
    with path.open(encoding='utf-8-sig', newline='') as handle:
        table = list(csv.reader(handle))
    required = {1: 'Budjettipuoli', 2: 'momenttitaso', 3: 'momenttipuunumerot',
                9: 'rahanumerona', 10: 'Lib budjetti 27', 11: 'Leikattavaa momentista'}
    if not table or any(table[0][col] != label for col, label in required.items()):
        raise ValueError('Unexpected CSV headers; no files written.')
    rows = []
    for source_row, row in enumerate(table[1:], 2):
        proposed = amount(row[10])
        if proposed is None:
            raise ValueError(f'Missing Lib27 amount at source row {source_row}')
        rows.append({
            'source_row': source_row, 'side': row[1], 'level': int(row[2]),
            'code': row[3], 'department': row[5], 'chapter': row[6], 'name': row[8],
            'baseline_eur': amount(row[9]), 'alternative_eur': proposed,
            'change_eur': amount(row[11]),
        })
    keys = [(r['side'], r['code']) for r in rows]
    if len(keys) != len(set(keys)):
        raise ValueError('Duplicate moment keys')
    for i, row in enumerate(rows):
        children = []
        for child in rows[i+1:]:
            if child['side'] != row['side'] or child['level'] <= row['level']:
                break
            if child['level'] == row['level'] + 1:
                children.append(child)
        if children and sum(c['alternative_eur'] for c in children) != row['alternative_eur']:
            raise ValueError(f'Hierarchy mismatch: {row["side"]} {row["code"]}')
        if row['baseline_eur'] is not None and row['change_eur'] != row['alternative_eur'] - row['baseline_eur']:
            raise ValueError(f'Change mismatch at source row {row["source_row"]}')
    totals = {r['side']: r for r in rows if r['level'] == 0}
    income = totals['tulo']['alternative_eur']
    appropriations = totals['meno']['alternative_eur']
    amortization = next(r['alternative_eur'] for r in rows if r['side'] == 'meno' and r['code'] == '37.01.94.')
    return {
        'meta': {'dataset_id': 'liberaali_vaihtoehtobudjetti_2027_accounting_v1',
                 'budget_year': 2027, 'source_sha256': hashlib.sha256(raw).hexdigest(),
                 'revision_date': '2026-10-18', 'currency': 'EUR',
                 'basis': 'Vuoden 2027 nimelliset eurot; käyttäjän toimittama vaihtoehtobudjetti.',
                 'scope': 'Kirjanpidollinen vertailu; ei makroennuste tai etuuksien riittävyyslaskelma.'},
        'accounting': {'nonborrowing_revenue_eur': income,
                       'appropriations_including_amortization_eur': appropriations,
                       'expenditure_excluding_amortization_eur': appropriations - amortization,
                       'amortization_reserve_eur': amortization,
                       'deficit_excluding_amortization_eur': appropriations - amortization - income,
                       'financing_need_including_amortization_eur': appropriations - income,
                       'baseline_appropriations_eur': totals['meno']['baseline_eur']},
        'corrections': [
            {'code': '33.30.60.', 'label': 'Sairausvakuutus', 'before_eur': 1603000, 'after_eur': 1603000000},
            {'code': '35.01.29.', 'label': 'Ympäristöministeriön ALV-määräraha', 'before_eur': None, 'after_eur': 5152000},
            {'code': '33.60.', 'label': 'Sosiaali- ja terveydenhuollon välisumma', 'before_eur': 198851000, 'after_eur': 237947000},
            {'code': '21.10.80.lib', 'label': 'Uuden puoluerahoitusmomentin vertailumäärä', 'before_eur': 23245500, 'after_eur': 0},
        ],
        'open_findings': [
            'Metsähallituksen siirtoperusteluun liittyy mahdollinen 6,201 M€ päällekkäinen määräraha. Sitä ei ole poistettu.',
            'Tuloveron perusteluteksti ja suhteellisen lähdeveron laskenta vaativat vielä täsmennyksen. Muut veromäärät on säilytetty.',
            'Perhe-etuuksien perustulosiirron perustelussa on 13,3 M€ ristiriita. Siirtomäärää ei ole muutettu.',
            'Perustulon saajamäärät ja verotus sekä korkosäästön ajoitus tarvitsevat erilliset laskelmat.',
        ], 'rows': rows,
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', type=Path, required=True)
    data = build(parser.parse_args().csv)
    payload = json.dumps(data, ensure_ascii=False, indent=2) + '\n'
    for dest in [ROOT/'data/reference/liberaali_vaihtoehtobudjetti/vaihtoehtobudjetti_2027_v1.json',
                 ROOT/'frontend/public/data/liberaali-vaihtoehtobudjetti-2027.json']:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(payload, encoding='utf-8')
    template = (ROOT/'scripts/liberaali_2027.template.html').read_text(encoding='utf-8')
    # Escape HTML-significant characters in script data, independently of labels.
    embedded = json.dumps(data, ensure_ascii=False, separators=(',', ':')).replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026')
    page = template.replace('__BUDGET_DATA__', embedded)
    for dest in [ROOT/'liberaali-vaihtoehtobudjetti-2027.html',
                 ROOT/'frontend/public/liberaali-vaihtoehtobudjetti-2027.html']:
        dest.write_text(page, encoding='utf-8')
    print(json.dumps(data['accounting'], ensure_ascii=False, indent=2))
