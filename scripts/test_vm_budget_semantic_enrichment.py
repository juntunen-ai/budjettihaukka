#!/usr/bin/env python3
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.ingest_vm_budget_semantic_enrichment import _extract_hierarchy_refs


def main() -> None:
    sample = """
    Pääluokka 35
    35.
    YMPÄRISTÖMINISTERIÖN HALLINNONALA
    30.
    Asumisen edistäminen
    54.
    Asumistuki (arviomääräraha)
    60.
    Siirto valtion asuntorahastoon
    """
    osastot, luvut, momentit = _extract_hierarchy_refs(sample)
    assert "35." in osastot, osastot
    assert "35.30." in luvut, luvut
    assert "35.30.54." in momentit, momentit
    print("VM semantic enrichment tests PASSED")


if __name__ == "__main__":
    main()
