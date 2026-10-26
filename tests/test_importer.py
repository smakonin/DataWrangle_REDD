"""Functional integration tests for exact-time, measurement-preserving imports.

Every measurement in these tests is synthetic. The fixtures reproduce the old
importer's failure triggers without asserting their prevalence in real REDD.
Run with ``python -m unittest discover -s tests`` from the repository root.
"""

from __future__ import annotations

import csv
from decimal import Decimal
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
IMPORTER_PATH = ROOT / "Import_REDD_dataset.py"
SPEC = importlib.util.spec_from_file_location("redd_functional_tests", IMPORTER_PATH)
assert SPEC is not None and SPEC.loader is not None
importer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = importer
SPEC.loader.exec_module(importer)

BASE = 1_300_000_000
LOAD_COLUMNS = [
    "TimeStamp", "MAIN", "KTCH", "LITE", "STOV", "MICR", "DRYR",
    "REFG", "DISH", "GARB", "DIFF", "MAIN_1", "MAIN_2",
]
QUALITY_COLUMNS = [
    "TimeStamp", "observed_channels", "missing_channels", "mains_complete",
    "targets_complete", "row_complete", "negative_residual",
    "seconds_since_previous_row",
]
RAW_COLUMNS = ["TimeStamp", "Channel", "Value", "SourceLine"]


def measurements(times=(0, 1, 2)):
    """House 2: two mains legs, eight target groups, nine target channels."""
    return {
        channel: [(BASE + t, "100" if channel <= 2 else "10") for t in times]
        for channel in range(1, 12)
    }


def replace_value(rows, channel, offset, value):
    rows[channel] = [
        (timestamp, str(value) if timestamp == BASE + offset else original)
        for timestamp, original in rows[channel]
    ]


def read_csv(path):
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return reader.fieldnames, list(reader)


def channel_set(value):
    return {int(part) for part in value.split(";") if part}


class ImporterFunctionalTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.input_root = self.directory / "raw"
        self.output_root = self.directory / "output"

    def write_house(self, rows, *, input_root=None, newline="\n", final_newline=True):
        raw_root = input_root or self.input_root
        house = raw_root / "house_2"
        house.mkdir(parents=True)
        for channel, points in rows.items():
            text = newline.join(f"{timestamp} {value}" for timestamp, value in points)
            if final_newline:
                text += newline
            (house / f"channel_{channel}.dat").write_bytes(text.encode("ascii"))
        return raw_root

    def run_import(self, rows=None):
        self.write_house(measurements() if rows is None else rows)
        result = importer.import_house(self.input_root, self.output_root, 2)
        self.assertEqual(result, self.output_root / "house_2")
        return result

    def loads_and_quality(self, result):
        load_columns, loads = read_csv(result / "loads.csv")
        quality_columns, quality = read_csv(result / "quality.csv")
        self.assertEqual(load_columns, LOAD_COLUMNS)
        self.assertEqual(quality_columns, QUALITY_COLUMNS)
        self.assertEqual(
            [row["TimeStamp"] for row in loads],
            [row["TimeStamp"] for row in quality],
        )
        return loads, quality

    def test_complete_control_has_expected_group_sums_and_raw_observations(self):
        result = self.run_import()
        loads, quality = self.loads_and_quality(result)
        self.assertEqual(len(loads), 3)
        for row in loads:
            self.assertEqual(Decimal(row["MAIN"]), Decimal("200"))
            self.assertEqual(Decimal(row["KTCH"]), Decimal("20"))
            for name in ("LITE", "STOV", "MICR", "DRYR", "REFG", "DISH", "GARB"):
                self.assertEqual(Decimal(row[name]), Decimal("10"))
            self.assertEqual(Decimal(row["DIFF"]), Decimal("110"))
            self.assertEqual(Decimal(row["MAIN_1"]), Decimal("100"))
            self.assertEqual(Decimal(row["MAIN_2"]), Decimal("100"))
        for row in quality:
            self.assertEqual(channel_set(row["observed_channels"]), set(range(1, 12)))
            self.assertEqual(row["missing_channels"], "")
            for flag in ("mains_complete", "targets_complete", "row_complete"):
                self.assertEqual(row[flag], "1")
            self.assertEqual(row["negative_residual"], "0")
        self.assertEqual([row["seconds_since_previous_row"] for row in quality], ["", "1", "1"])
        columns, raw = read_csv(result / "channels.csv")
        self.assertEqual(columns, RAW_COLUMNS)
        self.assertEqual(len(raw), 33)
        for channel in range(1, 12):
            observed = [row for row in raw if int(row["Channel"]) == channel]
            self.assertEqual([int(row["SourceLine"]) for row in observed], [1, 2, 3])
            self.assertEqual([int(row["TimeStamp"]) for row in observed], [BASE, BASE + 1, BASE + 2])
        self.assertEqual(read_csv(result / "issues.csv")[1], [])
        manifest = json.loads((result / "manifest.json").read_text())
        self.assertEqual(manifest["counts"]["timestamp_rows"], 3)
        self.assertEqual(manifest["counts"]["complete_rows"], 3)
        self.assertEqual(manifest["counts"]["source_observations"], 33)
        self.assertEqual(manifest["counts"]["negative_residual_rows"], 0)

    def test_missing_constituent_makes_group_unavailable(self):
        rows = measurements()
        rows[8] = [(timestamp, value) for timestamp, value in rows[8] if timestamp != BASE + 1]
        result = self.run_import(rows)
        loads, quality = self.loads_and_quality(result)
        self.assertEqual(loads[1]["KTCH"], "")
        self.assertEqual(loads[1]["MAIN"], "200")
        self.assertEqual(loads[1]["DIFF"], "")
        self.assertEqual(channel_set(quality[1]["missing_channels"]), {8})
        self.assertEqual(quality[1]["mains_complete"], "1")
        self.assertEqual(quality[1]["targets_complete"], "0")
        self.assertEqual(quality[1]["row_complete"], "0")
        self.assertEqual(quality[1]["negative_residual"], "")

    def test_missing_whole_group_keeps_timestamp_and_other_measurements(self):
        rows = measurements()
        rows[11] = [(timestamp, value) for timestamp, value in rows[11] if timestamp != BASE + 1]
        result = self.run_import(rows)
        loads, quality = self.loads_and_quality(result)
        self.assertEqual(len(loads), 3)
        self.assertEqual(loads[1]["GARB"], "")
        self.assertEqual(loads[1]["KTCH"], "20")
        self.assertEqual(loads[1]["MAIN"], "200")
        self.assertEqual(loads[1]["DIFF"], "")
        self.assertEqual(channel_set(quality[1]["missing_channels"]), {11})

    def test_either_missing_mains_leg_is_explicit_without_discarding_targets(self):
        for missing in (1, 2):
            with self.subTest(missing_leg=missing):
                rows = measurements()
                rows[missing] = [(timestamp, value) for timestamp, value in rows[missing] if timestamp != BASE + 1]
                raw_root = self.directory / f"raw_leg_{missing}"
                self.write_house(rows, input_root=raw_root)
                result = importer.import_house(raw_root, self.directory / f"out_leg_{missing}", 2)
                loads, quality = self.loads_and_quality(result)
                self.assertEqual(loads[1]["MAIN"], "")
                self.assertEqual(loads[1][f"MAIN_{missing}"], "")
                self.assertEqual(loads[1][f"MAIN_{3 - missing}"], "100")
                self.assertEqual(loads[1]["KTCH"], "20")
                self.assertEqual(loads[1]["DIFF"], "")
                self.assertEqual(quality[1]["mains_complete"], "0")
                self.assertEqual(quality[1]["targets_complete"], "1")
                self.assertEqual(quality[1]["row_complete"], "0")
                self.assertEqual(quality[1]["negative_residual"], "")

    def test_union_includes_submeter_observations_before_and_after_mains(self):
        rows = measurements()
        rows[11].insert(0, (BASE - 1, "12.5"))
        rows[11].append((BASE + 3, "14.5"))
        result = self.run_import(rows)
        loads, quality = self.loads_and_quality(result)
        self.assertEqual([int(row["TimeStamp"]) for row in loads], list(range(BASE - 1, BASE + 4)))
        for index, value in ((0, "12.5"), (-1, "14.5")):
            self.assertEqual(loads[index]["GARB"], value)
            self.assertEqual(loads[index]["MAIN"], "")
            self.assertEqual(loads[index]["DIFF"], "")
            self.assertEqual(channel_set(quality[index]["observed_channels"]), {11})
            self.assertEqual(channel_set(quality[index]["missing_channels"]), set(range(1, 11)))

    def test_disorder_is_reported_while_raw_source_order_is_preserved(self):
        rows = measurements()
        rows[1] = [rows[1][2], rows[1][0], rows[1][1]]
        result = self.run_import(rows)
        loads, _ = self.loads_and_quality(result)
        self.assertEqual([int(row["TimeStamp"]) for row in loads], [BASE, BASE + 1, BASE + 2])
        _, raw = read_csv(result / "channels.csv")
        mains_one = [row for row in raw if row["Channel"] == "1"]
        self.assertEqual([int(row["TimeStamp"]) for row in mains_one], [BASE + 2, BASE, BASE + 1])
        self.assertEqual([int(row["SourceLine"]) for row in mains_one], [1, 2, 3])
        _, issues = read_csv(result / "issues.csv")
        self.assertTrue(any("order" in " ".join(row.values()).lower() for row in issues))

    def test_first_and_last_negative_residuals_preserve_measured_mains(self):
        rows = measurements()
        for channel in range(3, 12):
            replace_value(rows, channel, 0, "30")
            replace_value(rows, channel, 2, "30")
        result = self.run_import(rows)
        loads, quality = self.loads_and_quality(result)
        for index in (0, 2):
            self.assertEqual(loads[index]["MAIN"], "200")
            self.assertEqual(loads[index]["MAIN_1"], "100")
            self.assertEqual(loads[index]["MAIN_2"], "100")
            self.assertEqual(loads[index]["DIFF"], "-70")
            self.assertEqual(quality[index]["negative_residual"], "1")
            self.assertEqual(quality[index]["row_complete"], "1")
        self.assertEqual(loads[1]["DIFF"], "110")
        counts = json.loads((result / "manifest.json").read_text())["counts"]
        self.assertEqual(counts["negative_residual_rows"], 2)

    def test_interior_negative_residual_does_not_trigger_legacy_fallback(self):
        rows = measurements(times=range(31))
        for channel in range(3, 12):
            replace_value(rows, channel, 15, "1000")
        replace_value(rows, 1, 16, "200")
        result = self.run_import(rows)
        loads, quality = self.loads_and_quality(result)
        self.assertEqual(loads[15]["MAIN"], "200")
        self.assertEqual(loads[15]["KTCH"], "2000")
        self.assertEqual(loads[15]["DIFF"], "-8800")
        self.assertEqual(quality[15]["negative_residual"], "1")
        self.assertEqual(loads[16]["MAIN"], "300")
        self.assertEqual(loads[16]["DIFF"], "210")

    def test_computed_minus_999_is_a_value_not_a_missing_marker(self):
        rows = measurements()
        for channel in range(3, 11):
            replace_value(rows, channel, 1, "133")
        replace_value(rows, 11, 1, "135")
        result = self.run_import(rows)
        loads, quality = self.loads_and_quality(result)
        self.assertEqual(loads[1]["DIFF"], "-999")
        self.assertEqual(quality[1]["row_complete"], "1")
        self.assertEqual(quality[1]["negative_residual"], "1")
        self.assertEqual(quality[1]["missing_channels"], "")

    def test_observed_minus_999_is_preserved(self):
        rows = measurements()
        replace_value(rows, 11, 1, "-999")
        result = self.run_import(rows)
        loads, quality = self.loads_and_quality(result)
        self.assertEqual(loads[1]["GARB"], "-999")
        self.assertEqual(loads[1]["DIFF"], "1119")
        self.assertEqual(quality[1]["row_complete"], "1")
        self.assertEqual(quality[1]["negative_residual"], "0")

    def test_decimal_precision_does_not_create_artificial_negative_residual(self):
        rows = {
            channel: [(BASE + offset, "1.9" if channel <= 2 else "0") for offset in range(3)]
            for channel in range(1, 12)
        }
        replace_value(rows, 3, 1, "3.7")
        result = self.run_import(rows)
        loads, quality = self.loads_and_quality(result)
        self.assertEqual(Decimal(loads[1]["MAIN"]), Decimal("3.8"))
        self.assertEqual(Decimal(loads[1]["KTCH"]), Decimal("3.7"))
        self.assertEqual(Decimal(loads[1]["DIFF"]), Decimal("0.1"))
        self.assertEqual(quality[1]["negative_residual"], "0")

    def test_native_cadence_produces_union_and_explicit_unavailable_targets(self):
        rows = measurements(times=range(7))
        for channel in range(3, 12):
            rows[channel] = [(timestamp, value) for timestamp, value in rows[channel] if (timestamp - BASE) % 3 == 0]
        result = self.run_import(rows)
        loads, quality = self.loads_and_quality(result)
        self.assertEqual(len(loads), 7)
        for offset, (load, status) in enumerate(zip(loads, quality)):
            self.assertEqual(load["MAIN"], "200")
            self.assertEqual(status["mains_complete"], "1")
            if offset % 3 == 0:
                self.assertEqual(load["DIFF"], "110")
                self.assertEqual(status["row_complete"], "1")
            else:
                self.assertEqual(load["KTCH"], "")
                self.assertEqual(load["DIFF"], "")
                self.assertEqual(status["row_complete"], "0")
                self.assertEqual(channel_set(status["observed_channels"]), {1, 2})

    def test_timestamp_gaps_are_retained_and_reported(self):
        result = self.run_import(measurements(times=(0, 3, 10)))
        loads, quality = self.loads_and_quality(result)
        self.assertEqual([int(row["TimeStamp"]) for row in loads], [BASE, BASE + 3, BASE + 10])
        self.assertEqual([row["seconds_since_previous_row"] for row in quality], ["", "3", "7"])

    def test_sparse_large_span_exports_only_observed_union_timestamps(self):
        gap = 2_000_000_000
        result = self.run_import(measurements(times=(0, gap)))
        loads, quality = self.loads_and_quality(result)
        self.assertEqual([int(row["TimeStamp"]) for row in loads], [BASE, BASE + gap])
        self.assertEqual([row["seconds_since_previous_row"] for row in quality], ["", str(gap)])
        self.assertEqual(len(read_csv(result / "channels.csv")[1]), 22)

    def test_integer_lf_crlf_and_no_final_newline_are_equivalent(self):
        for label, newline, final in (("lf", "\n", True), ("crlf", "\r\n", True), ("no_newline", "\n", False)):
            with self.subTest(format=label):
                raw_root = self.directory / f"raw_{label}"
                self.write_house(measurements(), input_root=raw_root, newline=newline, final_newline=final)
                result = importer.import_house(raw_root, self.directory / f"out_{label}", 2)
                loads, _ = self.loads_and_quality(result)
                self.assertEqual(len(loads), 3)
                self.assertTrue(all(row["MAIN"] == "200" and row["KTCH"] == "20" for row in loads))

    def test_module_import_has_no_execution_side_effects(self):
        probe = self.directory / "import_probe"
        probe.mkdir()
        code = (
            "import runpy; "
            f"runpy.run_path({str(IMPORTER_PATH)!r}, run_name='import_safety_probe')"
        )
        process = subprocess.run([sys.executable, "-c", code], cwd=probe, text=True, capture_output=True, timeout=30)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(process.stdout, "")
        self.assertEqual(list(probe.iterdir()), [])

    def test_cli_imports_requested_house(self):
        self.write_house(measurements())
        process = subprocess.run(
            [sys.executable, str(IMPORTER_PATH), "--input-dir", str(self.input_root),
             "--output-dir", str(self.output_root), "--houses", "2"],
            cwd=self.directory, text=True, capture_output=True, timeout=30,
        )
        self.assertEqual(process.returncode, 0, process.stderr)
        loads, _ = self.loads_and_quality(self.output_root / "house_2")
        self.assertEqual(len(loads), 3)


if __name__ == "__main__":
    unittest.main()
