"""Keep the published raw-audit metric definitions covered without REDD data."""
import importlib.util
from pathlib import Path


def load_tests(loader, standard_tests, pattern):
    path = (Path(__file__).resolve().parents[1] / "docs/source/whitepaper/analysis"
            / "fresh_audit/work/redd_full_audit/test_audit_raw.py")
    spec = importlib.util.spec_from_file_location("published_raw_audit_tests", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return loader.loadTestsFromModule(module)
