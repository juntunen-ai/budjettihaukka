import importlib.util
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class Budget2027Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data = json.loads((ROOT/'data/reference/liberaali_vaihtoehtobudjetti/vaihtoehtobudjetti_2027_v1.json').read_text())

    def test_hierarchy_and_deficit(self):
        rows = self.data['rows']
        for side in ['tulo', 'meno']:
            total = next(r['alternative_eur'] for r in rows if r['side'] == side and r['level'] == 0)
            for level in [1, 3]:
                self.assertEqual(total, sum(r['alternative_eur'] for r in rows if r['side'] == side and r['level'] == level))
        a = self.data['accounting']
        self.assertEqual(a['financing_need_including_amortization_eur'], 1645645000)
        self.assertEqual(a['deficit_excluding_amortization_eur'], 1644645000)
        self.assertEqual(a['expenditure_excluding_amortization_eur'] + a['amortization_reserve_eur'], a['appropriations_including_amortization_eur'])
        self.assertEqual(a['nonborrowing_revenue_eur'] + a['financing_need_including_amortization_eur'], a['appropriations_including_amortization_eur'])

    def test_confirmed_amounts_and_public_allowlist(self):
        rows = self.data['rows']
        required = {'source_row', 'side', 'level', 'code', 'department', 'chapter', 'name', 'baseline_eur', 'alternative_eur', 'change_eur'}
        for row in rows:
            self.assertEqual(set(row), required)
            self.assertIsInstance(row['alternative_eur'], int)
        def alternative(code):
            return next(r['alternative_eur'] for r in rows if r['code'] == code)
        self.assertEqual(alternative('33.30.60.'), 1603000000)
        self.assertEqual(alternative('35.01.29.'), 5152000)
        self.assertEqual(len({(r['side'], r['code']) for r in rows}), len(rows))

    def test_generated_copies_and_embedded_data(self):
        public = ROOT/'frontend/public'
        self.assertEqual(self.data, json.loads((public/'data/liberaali-vaihtoehtobudjetti-2027.json').read_text()))
        page = (public/'liberaali-vaihtoehtobudjetti-2027.html').read_text()
        self.assertEqual(page, (ROOT/'liberaali-vaihtoehtobudjetti-2027.html').read_text())
        embedded = page.split('<script id="budget-data" type="application/json">', 1)[1].split('</script>', 1)[0]
        self.assertEqual(json.loads(embedded), self.data)
        self.assertNotIn('__BUDGET_DATA__', page)


if __name__ == '__main__':
    unittest.main()
