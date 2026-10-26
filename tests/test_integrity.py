"""Independent input-preservation and transactional regression tests.

Synthetic fixtures exercise importer promises; they do not certify raw REDD.
Only Python's standard library is required.
"""
from __future__ import annotations

import csv
from decimal import Decimal
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("redd_integrity_importer", REPO / "Import_REDD_dataset.py")
assert SPEC and SPEC.loader
importer = importlib.util.module_from_spec(SPEC)
# A normal imported module is registered before execution (needed by dataclasses).
import sys
sys.modules[SPEC.name] = importer
SPEC.loader.exec_module(importer)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


class IntegrityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.raw = self.base / "raw"
        self.house = self.raw / "house_2"
        self.house.mkdir(parents=True)
        self.output = self.base / "output"
        for channel in range(1, 12):
            self.channel(channel).write_text(f"1000 {100 if channel <= 2 else 1}.0\n", encoding="utf-8")

    def channel(self, number: int) -> Path:
        return self.house / f"channel_{number}.dat"

    def run_import(self) -> Path:
        return Path(importer.import_house(self.raw, self.output, 2))

    def source_hashes(self):
        return {path.relative_to(self.raw).as_posix(): sha256(path)
                for path in self.raw.rglob("*") if path.is_file()}

    def assert_no_published_or_staged_output(self):
        self.assertFalse((self.output / "house_2").exists())
        if self.output.exists():
            self.assertEqual(list(self.output.iterdir()), [], "Failed import left partial output/staging files")

    def test_all_duplicate_source_observations_retained_but_not_double_counted(self):
        self.channel(1).write_text("1000 100.0\n1000 100.0\n1001 101.0\n", encoding="utf-8")
        before = self.source_hashes()
        destination = self.run_import()
        raw_rows = [row for row in read_csv(destination / "channels.csv") if row["Channel"] == "1"]
        self.assertEqual(len(raw_rows), 3)
        self.assertEqual([int(row["SourceLine"]) for row in raw_rows], [1, 2, 3])
        self.assertEqual([Decimal(row["Value"]) for row in raw_rows], [Decimal(100), Decimal(100), Decimal(101)])
        self.assertEqual([row["TimeStamp"] for row in raw_rows], ["1000", "1000", "1001"])
        first = next(row for row in read_csv(destination / "loads.csv") if row["TimeStamp"] == "1000")
        self.assertEqual(Decimal(first["MAIN"]), Decimal(200))
        self.assertEqual(Decimal(first["MAIN_1"]), Decimal(100))
        self.assertEqual(self.source_hashes(), before)

    def test_conflicting_duplicate_is_fatal_and_atomic(self):
        self.channel(11).write_text("1000 1.0\n1000 2.0\n", encoding="utf-8")
        before = self.source_hashes()
        with self.assertRaises(importer.REDDImportError):
            self.run_import()
        self.assert_no_published_or_staged_output()
        self.assertEqual(self.source_hashes(), before)

    def test_equivalent_decimal_duplicate_spellings_are_not_conflicts(self):
        self.channel(1).write_text("1000 100.0\n1000 100.00\n", encoding="utf-8")
        destination = self.run_import()
        raw_rows = [row for row in read_csv(destination / "channels.csv") if row["Channel"] == "1"]
        self.assertEqual(len(raw_rows), 2)
        self.assertEqual([int(row["SourceLine"]) for row in raw_rows], [1, 2])
        self.assertTrue(all(Decimal(row["Value"]) == Decimal(100) for row in raw_rows))
        self.assertEqual(Decimal(read_csv(destination / "loads.csv")[0]["MAIN"]), Decimal(200))
        manifest = json.loads((destination / "manifest.json").read_text())
        self.assertEqual(manifest["counts"]["identical_duplicate_records"], 1)

    def test_malformed_and_nonfinite_observations_are_rejected_atomically(self):
        cases = ["1000 NaN\n", "1000 sNaN\n", "1000 Infinity\n", "1000 -Infinity\n",
                 "1000\n", "1000 1.0 unexpected\n", "not_a_time 1.0\n", "1000.5 1.0\n"]
        for invalid in cases:
            with self.subTest(observation=invalid):
                self.channel(11).write_text(invalid, encoding="utf-8")
                before = self.source_hashes()
                with self.assertRaises(importer.REDDImportError):
                    self.run_import()
                self.assert_no_published_or_staged_output()
                self.assertEqual(self.source_hashes(), before)

    def test_missing_required_channel_file_is_fatal_without_output(self):
        self.channel(11).unlink()
        before = self.source_hashes()
        with self.assertRaises(importer.REDDImportError):
            self.run_import()
        self.assert_no_published_or_staged_output()
        self.assertEqual(self.source_hashes(), before)

    def test_unmapped_extra_channel_is_rejected_without_silent_discard(self):
        self.channel(12).write_text("1000 12345.6789\n", encoding="utf-8")
        before = self.source_hashes()
        with self.assertRaises(importer.REDDImportError):
            self.run_import()
        self.assert_no_published_or_staged_output()
        self.assertEqual(self.source_hashes(), before)

    def test_existing_output_is_refused_and_preserved(self):
        destination = self.run_import()
        marker = destination / "user_notes.txt"
        marker.write_text("keep this existing output", encoding="utf-8")
        before = {path.name: sha256(path) for path in destination.iterdir() if path.is_file()}
        with self.assertRaises(importer.REDDImportError):
            self.run_import()
        self.assertEqual({path.name: sha256(path) for path in destination.iterdir() if path.is_file()}, before)
        self.assertEqual(marker.read_text(), "keep this existing output")

    def test_source_values_remain_unchanged_on_success(self):
        self.channel(3).write_text("1000 1.234567890123456789\n", encoding="utf-8")
        before = self.source_hashes()
        destination = self.run_import()
        self.assertEqual(self.source_hashes(), before)
        observed = [row for row in read_csv(destination / "channels.csv") if row["Channel"] == "3"]
        self.assertEqual(len(observed), 1)
        self.assertEqual(Decimal(observed[0]["Value"]), Decimal("1.234567890123456789"))

    def test_negative_residual_does_not_reconstruct_or_shift_mains(self):
        self.channel(1).write_text("1000 1.25\n1001 9999.5\n", encoding="utf-8")
        self.channel(2).write_text("1000 2.75\n1001 9999.5\n", encoding="utf-8")
        self.channel(3).write_text("1000 10000.125\n", encoding="utf-8")
        destination = self.run_import()
        row = next(row for row in read_csv(destination / "loads.csv") if row["TimeStamp"] == "1000")
        self.assertEqual(Decimal(row["MAIN_1"]), Decimal("1.25"))
        self.assertEqual(Decimal(row["MAIN_2"]), Decimal("2.75"))
        self.assertEqual(Decimal(row["MAIN"]), Decimal("4.0"))
        self.assertLess(Decimal(row["DIFF"]), 0)

    def test_accepted_large_fractional_values_sum_without_rounding(self):
        large = "123456789012345678901234567890.12345678901234567890"
        tiny = "0.00000000000000000001"
        for channel in range(1, 12):
            self.channel(channel).write_text("1000 0\n", encoding="utf-8")
        self.channel(1).write_text(f"1000 {large}\n", encoding="utf-8")
        self.channel(2).write_text(f"1000 {tiny}\n", encoding="utf-8")
        self.channel(3).write_text(f"1000 {tiny}\n", encoding="utf-8")
        destination = self.run_import()
        row = read_csv(destination / "loads.csv")[0]
        self.assertEqual(Decimal(row["MAIN_1"]), Decimal(large))
        self.assertEqual(Decimal(row["MAIN_2"]), Decimal(tiny))
        self.assertEqual(Decimal(row["MAIN"]), Decimal("123456789012345678901234567890.12345678901234567891"))
        self.assertEqual(Decimal(row["DIFF"]), Decimal(large))

    def test_source_and_output_hashes_and_duplicate_provenance_reconcile(self):
        self.channel(1).write_text("1000 100.0\n1000 100.0\n1001 101.0\n", encoding="utf-8")
        before = self.source_hashes()
        destination = self.run_import()
        manifest = json.loads((destination / "manifest.json").read_text(encoding="utf-8"))
        sources = manifest["source_files"]
        self.assertEqual(len(sources), 11)
        self.assertEqual({entry["channel"] for entry in sources}, set(range(1, 12)))
        for entry in sources:
            path = self.raw / entry["path"]
            self.assertEqual(entry["sha256"], sha256(path))
            self.assertEqual(entry["size_bytes"], path.stat().st_size)
        first = next(entry for entry in sources if entry["channel"] == 1)
        self.assertEqual(first["source_rows"], 3)
        self.assertEqual(first["unique_observations"], 2)
        self.assertEqual(first["identical_duplicates"], 1)
        self.assertEqual(first["first_timestamp"], 1000)
        self.assertEqual(first["last_timestamp"], 1001)
        self.assertEqual(manifest["counts"]["source_observations"], 13)
        self.assertEqual(manifest["counts"]["identical_duplicate_records"], 1)
        self.assertEqual(manifest["counts"]["timestamp_rows"], 2)
        for required in ["channels.csv", "loads.csv", "quality.csv", "issues.csv"]:
            self.assertIn(required, manifest["outputs"])
        self.assertNotIn("manifest.json", manifest["outputs"])
        for filename, metadata in manifest["outputs"].items():
            path = destination / filename
            self.assertTrue(path.is_file())
            self.assertEqual(metadata["sha256"], sha256(path))
            self.assertEqual(metadata["rows"], len(read_csv(path)))
        self.assertEqual(self.source_hashes(), before)

    def test_conventional_labels_dat_is_preserved_in_provenance(self):
        labels = self.house / "labels.dat"
        labels.write_text("".join(f"{channel} original label {channel}\n" for channel in range(1, 12)),
                          encoding="utf-8")
        before = self.source_hashes()
        destination = self.run_import()
        manifest = json.loads((destination / "manifest.json").read_text())
        self.assertIsNotNone(manifest["labels"])
        self.assertEqual(manifest["labels"]["path"], "house_2/labels.dat")
        self.assertEqual(manifest["labels"]["sha256"], sha256(labels))
        self.assertEqual(manifest["labels"]["names"]["3"], "original label 3")
        self.assertEqual(self.source_hashes(), before)

    def test_two_label_files_are_rejected_as_ambiguous(self):
        text = "".join(f"{channel} label_{channel}\n" for channel in range(1, 12))
        (self.house / "labels.dat").write_text(text, encoding="utf-8")
        (self.house / "labels.txt").write_text(text, encoding="utf-8")
        before = self.source_hashes()
        with self.assertRaises(importer.REDDImportError):
            self.run_import()
        self.assert_no_published_or_staged_output()
        self.assertEqual(self.source_hashes(), before)

    def test_labels_txt_is_explicitly_preserved_when_conventional_file_absent(self):
        labels = self.house / "labels.txt"
        labels.write_text("".join(f"{channel} fallback label {channel}\n" for channel in range(1, 12)),
                          encoding="utf-8")
        destination = self.run_import()
        manifest = json.loads((destination / "manifest.json").read_text())
        self.assertEqual(manifest["labels"]["path"], "house_2/labels.txt")
        self.assertEqual(manifest["labels"]["sha256"], sha256(labels))

    def test_interval_histogram_is_bounded_and_discloses_omissions(self):
        timestamp = 1000
        lines = [f"{timestamp} 100.0\n"]
        for delta in range(1, 301):
            timestamp += delta
            lines.append(f"{timestamp} 100.0\n")
        self.channel(1).write_text("".join(lines), encoding="utf-8")
        destination = self.run_import()
        manifest = json.loads((destination / "manifest.json").read_text())
        channel = next(entry for entry in manifest["source_files"] if entry["channel"] == 1)
        intervals = channel["sample_interval_seconds"]
        self.assertEqual(channel["unique_observations"], 301)
        self.assertEqual(intervals["interval_count"], 300)
        self.assertEqual(intervals["minimum"], 1)
        self.assertEqual(intervals["maximum"], 300)
        self.assertEqual(len(intervals["histogram"]), 256)
        self.assertFalse(intervals["histogram_complete"])
        self.assertEqual(intervals["omitted_intervals"], 44)
        self.assertEqual(sum(intervals["histogram"].values()) + intervals["omitted_intervals"],
                         intervals["interval_count"])


if __name__ == "__main__":
    unittest.main()
