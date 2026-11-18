"""Offline tests for the npm audit exception gate (scripts/check_npm_audit.py).

These tests use synthetic `npm audit --json` reports; they need no network access.
"""
import datetime as dt
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_spec = importlib.util.spec_from_file_location("check_npm_audit", ROOT / "scripts" / "check_npm_audit.py")
check_npm_audit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_npm_audit)
evaluate = check_npm_audit.evaluate
load_exceptions = check_npm_audit.load_exceptions

ID = "GHSA-fc5q-39xc-vjpf"
OTHER = "GHSA-2222-3333-4444"
TODAY = dt.date(2026, 11, 17)


def advisory(ident, severity="high"):
    return {"source": 1, "name": "x", "url": f"https://github.com/advisories/{ident}", "severity": severity}


def report(**vulns):
    return {"vulnerabilities": vulns}


def chain(ident=ID):
    """The real picomatch -> fdir/tinyglobby -> vite shape from npm audit."""
    return report(
        picomatch={"severity": "high", "via": [advisory(ident)]},
        fdir={"severity": "high", "via": ["picomatch"]},
        tinyglobby={"severity": "high", "via": ["fdir", "picomatch"]},
        vite={"severity": "high", "via": ["fdir", "picomatch", "tinyglobby"]},
    )


def exceptions(expires="2026-12-31", packages=("picomatch", "fdir", "tinyglobby", "vite")):
    return {ID: {"id": ID, "packages": list(packages), "expires": dt.date.fromisoformat(expires), "reason": "r"}}


class NpmAuditGateTests(unittest.TestCase):
    def test_listed_dev_only_chain_is_accepted(self):
        blocked, accepted, unused = evaluate(chain(), report(), exceptions(), TODAY)
        self.assertEqual(blocked, [])
        self.assertEqual(len(accepted), 4)
        self.assertEqual(unused, [])

    def test_last_valid_day_passes_and_next_day_fails(self):
        self.assertEqual(evaluate(chain(), report(), exceptions(), dt.date(2026, 12, 31))[0], [])
        blocked, _, unused = evaluate(chain(), report(), exceptions(), dt.date(2027, 1, 1))
        self.assertEqual(len(blocked), 4)
        self.assertTrue(all("expired" in line for line in blocked))
        self.assertEqual(unused, [], "an expired exception that still matches is not unused")

    def test_unlisted_advisory_is_blocked(self):
        blocked = evaluate(chain(OTHER), report(), exceptions(), TODAY)[0]
        self.assertEqual(len(blocked), 4)
        self.assertTrue(all(f"{OTHER} has no exception" in line for line in blocked))

    def test_extra_advisory_on_covered_package_is_blocked(self):
        full = chain()
        full["vulnerabilities"]["picomatch"]["via"].append(advisory(OTHER, "critical"))
        blocked = evaluate(full, report(), exceptions(), TODAY)[0]
        self.assertEqual(len(blocked), 4)
        self.assertTrue(all(OTHER in line for line in blocked))

    def test_package_not_listed_in_exception_is_blocked(self):
        blocked = evaluate(chain(), report(), exceptions(packages=("picomatch",)), TODAY)[0]
        self.assertEqual(sorted(line.split(" ")[0] for line in blocked), ["fdir", "tinyglobby", "vite"])

    def test_advisory_reaching_production_is_blocked(self):
        prod = report(picomatch={"severity": "high", "via": [advisory(ID)]})
        blocked = evaluate(chain(), prod, exceptions(), TODAY)[0]
        self.assertEqual(blocked, ["picomatch (high): reaches production dependencies"])

    def test_moderate_findings_do_not_block(self):
        full = report(lodash={"severity": "moderate", "via": [advisory(OTHER, "moderate")]})
        blocked, accepted, unused = evaluate(full, report(), exceptions(), TODAY)
        self.assertEqual((blocked, accepted, unused), ([], [], [ID]))

    def test_advisory_without_ghsa_id_is_blocked(self):
        full = report(pkg={"severity": "high", "via": [{"source": 99, "url": "https://example.invalid/x"}]})
        blocked = evaluate(full, report(), exceptions(), TODAY)[0]
        self.assertEqual(blocked, ["pkg (high): unidentified:99 has no exception"])

    def test_exception_file_validation(self):
        bad = [
            {"exceptions": [{"id": "CVE-2026-1", "packages": ["a"], "expires": "2026-12-31", "reason": "r"}]},
            {"exceptions": [{"id": ID, "packages": [], "expires": "2026-12-31", "reason": "r"}]},
            {"exceptions": [{"id": ID, "packages": ["a"], "expires": "2026-12-31", "reason": " "}]},
            {"exceptions": [{"id": ID, "packages": ["a"], "expires": "soon", "reason": "r"}]},
            {"exceptions": [{"id": ID, "packages": ["a"], "expires": "2026-12-31", "reason": "r"}] * 2},
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "exceptions.json"
            for data in bad:
                with self.subTest(data=data):
                    path.write_text(json.dumps(data))
                    with self.assertRaises(ValueError):
                        load_exceptions(path)

    def test_repository_exception_file_is_valid(self):
        load_exceptions(ROOT / "frontend" / "npm-audit-exceptions.json")


if __name__ == "__main__":
    unittest.main()
