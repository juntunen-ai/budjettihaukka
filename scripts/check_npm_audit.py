#!/usr/bin/env python3
"""Fail on high or critical npm advisories, except listed, unexpired, dev-only ones.

`npm audit` has no way to accept a single advisory. This gate runs the full
audit and the production-only audit (`--omit=dev`) and fails unless every high
or critical finding is covered by an exception that:

- names the advisory (GHSA id) and the vulnerable package,
- has not expired (the exception lapses at the end of its `expires` day), and
- does not reach production dependencies.

Any other finding, an expired exception, or an advisory that reaches the
production tree fails the build exactly as `npm audit --audit-level=high` did.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
from pathlib import Path

BLOCKING = {"high", "critical"}
GHSA = re.compile(r"GHSA(-[23456789cfghjmpqrvwx]{4}){3}")


def load_exceptions(path: Path) -> dict[str, dict]:
    data = json.loads(path.read_text())
    out: dict[str, dict] = {}
    for item in data.get("exceptions", []):
        ident = item.get("id", "")
        if not GHSA.fullmatch(ident):
            raise ValueError(f"exception id is not a GHSA id: {ident!r}")
        if ident in out:
            raise ValueError(f"duplicate exception: {ident}")
        if not item.get("packages") or not str(item.get("reason", "")).strip():
            raise ValueError(f"{ident}: packages and reason are required")
        out[ident] = {**item, "expires": dt.date.fromisoformat(item["expires"])}
    return out


def run_audit(prefix: str, omit_dev: bool) -> dict:
    cmd = ["npm", "--prefix", prefix, "audit", "--json"]
    if omit_dev:
        cmd.append("--omit=dev")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    report = json.loads(proc.stdout or "{}")
    if "vulnerabilities" not in report:
        raise RuntimeError(f"npm audit failed: {proc.stdout[:500]} {proc.stderr[:500]}")
    return report


def advisories(vulns: dict, name: str, seen: frozenset = frozenset()) -> set[str]:
    """GHSA ids behind a package's finding, following `via` through other packages."""
    found: set[str] = set()
    for via in vulns.get(name, {}).get("via", []):
        if isinstance(via, str):
            if via not in seen:
                found |= advisories(vulns, via, seen | {name})
            continue
        match = GHSA.search(via.get("url", ""))
        # An advisory without a GHSA id can never match an exception.
        found.add(match.group(0) if match else f"unidentified:{via.get('source')}")
    return found


def evaluate(full: dict, prod: dict, exceptions: dict[str, dict], today: dt.date):
    vulns = full["vulnerabilities"]
    prod_names = set(prod["vulnerabilities"])
    blocked: list[str] = []
    accepted: list[str] = []
    seen: set[str] = set()
    for name in sorted(vulns):
        if vulns[name].get("severity") not in BLOCKING:
            continue
        ids = advisories(vulns, name)
        seen |= ids
        problems = []
        if name in prod_names:
            problems.append("reaches production dependencies")
        if not ids:
            problems.append("no advisory found")
        for ident in sorted(ids):
            exc = exceptions.get(ident)
            if exc is None:
                problems.append(f"{ident} has no exception")
            elif today > exc["expires"]:
                problems.append(f"{ident} exception expired {exc['expires']}")
            elif name not in exc["packages"]:
                problems.append(f"{ident} exception does not list {name}")
        if problems:
            blocked.append(f"{name} ({vulns[name]['severity']}): " + "; ".join(problems))
        else:
            accepted.append(f"{name} ({vulns[name]['severity']}): " + ", ".join(
                f"{i} until {exceptions[i]['expires']}" for i in sorted(ids)))
    unused = sorted(set(exceptions) - seen)
    return blocked, accepted, unused


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--prefix", default="frontend")
    parser.add_argument("--exceptions", type=Path, default=Path("frontend/npm-audit-exceptions.json"))
    parser.add_argument("--today", type=dt.date.fromisoformat, default=dt.datetime.now(dt.timezone.utc).date())
    args = parser.parse_args(argv)

    exceptions = load_exceptions(args.exceptions)
    blocked, accepted, unused = evaluate(
        run_audit(args.prefix, omit_dev=False), run_audit(args.prefix, omit_dev=True), exceptions, args.today)
    for line in accepted:
        print("ACCEPTED (exception)", line)
    for ident in unused:
        print(f"NOTICE: exception {ident} no longer matches any finding; remove it")
    for line in blocked:
        print("BLOCKED", line)
    if blocked:
        print(f"FAIL: {len(blocked)} high or critical finding(s) not covered by a valid exception")
        return 1
    print("PASS: no uncovered high or critical npm advisories")
    return 0


if __name__ == "__main__":
    sys.exit(main())
