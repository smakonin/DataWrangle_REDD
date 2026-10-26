"""Exercise the default CLI against every supported house schema."""
import csv
from decimal import Decimal
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


class AllHousesTests(unittest.TestCase):
    def test_default_cli_preserves_mains_across_all_six_houses(self):
        script = Path(__file__).resolve().parents[1] / "Import_REDD_dataset.py"
        channel_counts = {1: 20, 2: 11, 3: 22, 4: 20, 5: 26, 6: 17}
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            source, output = base / "low_freq", base / "export"
            for house, count in channel_counts.items():
                folder = source / f"house_{house}"
                folder.mkdir(parents=True)
                for channel in range(1, count + 1):
                    value = "10.125" if channel == 1 else "20.125" if channel == 2 else str(channel)
                    (folder / f"channel_{channel}.dat").write_text(
                        f"1300000000 {value}\n1300000003 {value}\n", encoding="utf-8")
            completed = subprocess.run(
                [sys.executable, str(script), "--input-dir", str(source), "--output-dir", str(output)],
                text=True, capture_output=True, check=False)
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual({p.name for p in output.iterdir()},
                             {f"house_{house}" for house in channel_counts})
            for house, count in channel_counts.items():
                with self.subTest(house=house):
                    folder = output / f"house_{house}"
                    with (folder / "loads.csv").open(newline="") as handle:
                        rows = list(csv.DictReader(handle))
                    self.assertEqual(len(rows), 2)
                    expected_residual = Decimal("30.250") - Decimal(count * (count + 1) // 2 - 3)
                    for row in rows:
                        self.assertEqual(Decimal(row["MAIN_1"]), Decimal("10.125"))
                        self.assertEqual(Decimal(row["MAIN_2"]), Decimal("20.125"))
                        self.assertEqual(Decimal(row["MAIN"]), Decimal("30.250"))
                        self.assertEqual(Decimal(row["DIFF"]), expected_residual)
                    metadata = json.loads((folder / "manifest.json").read_text())
                    self.assertEqual(metadata["counts"]["source_observations"], count * 2)
                    self.assertEqual(metadata["counts"]["complete_rows"], 2)
                    self.assertEqual(metadata["counts"]["negative_residual_rows"], 2)
                    self.assertEqual(len(metadata["source_files"]), count)
                    if house in {1, 5, 6}:
                        self.assertTrue(metadata["mapping_notes"])


if __name__ == "__main__":
    unittest.main()
