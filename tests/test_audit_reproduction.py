"""Exercise the documented fresh-audit path without a REDD download."""

from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


REPOSITORY = Path(__file__).resolve().parents[1]
FROZEN_WORK = REPOSITORY / "docs/source/whitepaper/analysis/fresh_audit/work"
CHANNEL_COUNTS = {1: 20, 2: 11, 3: 22, 4: 20, 5: 26, 6: 17}
TIMESTAMPS = (1_300_000_000, 1_300_000_001, 1_300_000_002)


class AuditReproductionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.workspace = Path(self.temporary.name)
        self.work = self.workspace / "work"
        self.audit = self.work / "redd_full_audit"
        self.importer = self.work / "DataWrangle_REDD/Import_REDD_dataset.py"
        self.importer.parent.mkdir(parents=True)
        self.audit.mkdir(parents=True)
        shutil.copy2(FROZEN_WORK / "DataWrangle_REDD/Import_REDD_dataset.py", self.importer)
        for name in ("audit_raw.py", "summarize_results.py"):
            shutil.copy2(FROZEN_WORK / "redd_full_audit" / name, self.audit / name)

        self.raw = self.workspace / "source/low_freq"
        for house, count in CHANNEL_COUNTS.items():
            directory = self.raw / f"house_{house}"
            directory.mkdir(parents=True)
            labels = []
            for channel in range(1, count + 1):
                value = "1000" if channel <= 2 else "1"
                (directory / f"channel_{channel}.dat").write_text(
                    "".join(f"{timestamp} {value}\n" for timestamp in TIMESTAMPS),
                    encoding="utf-8",
                )
                labels.append(f"{channel} synthetic_channel_{channel}\n")
            (directory / "labels.dat").write_text("".join(labels), encoding="utf-8")

    def command(self, script, *arguments):
        return subprocess.run(
            [sys.executable, str(script), *map(str, arguments)],
            cwd=self.workspace,
            text=True,
            capture_output=True,
            timeout=60,
            check=False,
        )

    def assert_succeeded(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def prepare_direct_import_and_audit(self):
        self.assert_succeeded(self.command(
            self.importer,
            "--input-dir", self.raw,
            "--output-dir", self.audit / "imported",
        ))
        self.assert_succeeded(self.command(
            self.audit / "audit_raw.py",
            "--input-dir", self.raw,
            "--output-dir", self.audit / "raw_results",
        ))
        # The public direct-import command intentionally does not run the
        # optional execution-record wrapper.
        self.assertFalse((self.audit / "import_execution.json").exists())

    def test_direct_import_audit_and_summary_without_wrapper_record(self):
        self.prepare_direct_import_and_audit()
        self.assert_succeeded(self.command(self.audit / "summarize_results.py"))
        summary = json.loads((self.audit / "reviewed/summary.json").read_text())
        pooled = summary["pooled"]
        self.assertEqual(pooled["source_observations"], 348)
        self.assertEqual(pooled["union_timestamps"], 18)
        self.assertEqual(pooled["both_mains_timestamps"], 18)
        self.assertEqual(pooled["all_channel_complete_timestamps"], 18)
        self.assertEqual(pooled["exact_complete_fraction_of_mains"], 1)
        self.assertEqual(pooled["negative_residuals"], 0)
        self.assertEqual(pooled["negative_residual_fraction"], 0)
        for key in ("negative_magnitude_median", "negative_magnitude_p90", "negative_magnitude_max"):
            self.assertIsNone(pooled[key])
        self.assertEqual([record["house"] for record in summary["houses"]], list(range(1, 7)))
        for house in summary["houses"]:
            self.assertEqual(house["all_channel_complete_timestamps"], 3)
            self.assertEqual(house["both_mains_timestamps"], 3)
            self.assertEqual(house["negative_residuals"], 0)
        for balance in summary["balances"]:
            self.assertEqual(balance["eligible"], 3)
            self.assertEqual(balance["positive"], 3)
            self.assertEqual(balance["negative"], 0)
            self.assertEqual(balance["minimum_residual"], 2000 - (CHANNEL_COUNTS[balance["house"]] - 2))
        self.assertFalse((self.audit / "import_execution.json").exists())

    def test_present_failed_execution_record_is_rejected(self):
        self.prepare_direct_import_and_audit()
        (self.audit / "import_execution.json").write_text(
            json.dumps({"status": "failed"}), encoding="utf-8"
        )
        result = self.command(self.audit / "summarize_results.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ValueError", result.stderr)
        self.assertIn("execution", result.stderr.lower())
        self.assertFalse((self.audit / "reviewed/summary.json").exists())

    def test_incomplete_raw_house_inventory_is_rejected(self):
        self.prepare_direct_import_and_audit()
        path = self.audit / "raw_results/summary.json"
        raw = json.loads(path.read_text())
        raw["house_summary"] = [record for record in raw["house_summary"] if record["house"] != 6]
        path.write_text(json.dumps(raw), encoding="utf-8")
        result = self.command(self.audit / "summarize_results.py")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("ValueError", result.stderr)
        self.assertIn("house", result.stderr.lower())
        self.assertFalse((self.audit / "reviewed/summary.json").exists())


if __name__ == "__main__":
    unittest.main()
