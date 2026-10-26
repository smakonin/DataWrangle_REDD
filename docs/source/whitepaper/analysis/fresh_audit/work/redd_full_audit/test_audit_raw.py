"""Synthetic checks of raw-audit definitions, not actual REDD results."""
import contextlib
import csv
import gzip
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("audit_raw_tests", HERE / "audit_raw.py")
audit_raw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit_raw)


def rows(path):
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


class AuditTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.raw = self.base / "raw"
        self.house = self.raw / "house_2"
        self.house.mkdir(parents=True)
        self.output = self.base / "audit"
        for channel in range(1, 12):
            self.channel(channel).write_text("1000 1\n1001 1\n1002 1\n1003 1\n")

    def channel(self, channel):
        return self.house / f"channel_{channel}.dat"

    def run_audit(self):
        before = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in self.house.iterdir()}
        with contextlib.redirect_stdout(io.StringIO()):
            audit_raw.audit(self.raw, self.output, houses=(2,), keep_db=True)
        after = {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in self.house.iterdir()}
        self.assertEqual(before, after)
        return json.loads((self.output / "summary.json").read_text())

    def test_continue_after_conflicts_malformed_and_disorder_without_using_tainted_key(self):
        self.channel(1).write_text("1000 10\n1001 11\n1001 11\n1001 12\nbad data\n1003 13\n1002 12\n")
        summary = self.run_audit()
        source = next(item for item in summary["sources"] if item["channel"] == 1)
        self.assertEqual(source["source_rows"], 7)
        self.assertEqual(source["valid_rows"], 6)
        self.assertEqual(source["malformed_rows"], 1)
        self.assertEqual(source["unique_timestamps"], 4)
        self.assertEqual(source["identical_to_first_duplicates"], 1)
        self.assertEqual(source["conflicting_with_first_duplicates"], 1)
        self.assertEqual(source["out_of_order_records"], 1)
        self.assertEqual(source["unambiguous_unique_timestamps"], 3)
        house = summary["house_summary"][0]
        self.assertEqual(house["union_timestamps"], 4)
        self.assertEqual(house["unambiguous_mains_timestamps"], 3)
        self.assertEqual(house["unambiguous_complete_timestamps"], 3)
        spans = [row for row in rows(self.output / "threshold_spans.csv")
                 if row["channel"] == "1" and row["threshold_name"] == "fixed_60s"]
        self.assertEqual(int(spans[0]["threshold_connected_seconds"]), 3)
        self.assertEqual(int(spans[0]["unambiguous_connected_seconds"]), 1)
        simultaneous = next(row for row in rows(self.output / "simultaneous_spans.csv")
                            if row["group"] == "ALL_REQUIRED" and row["threshold_seconds"] == "60")
        self.assertEqual(int(simultaneous["simultaneous_bounded_gap_span_seconds"]), 1)

    def test_known_invalid_timestamp_breaks_interval_and_record_mask_keeps_location(self):
        self.channel(1).write_text("1000 10\n1001 NaN\n1002 10\n")
        summary = self.run_audit()
        source = next(item for item in summary["sources"] if item["channel"] == 1)
        self.assertEqual(source["timestamped_invalid_records"], 1)
        self.assertEqual(source["unique_timestamps"], 2)
        span = next(row for row in rows(self.output / "threshold_spans.csv")
                    if row["channel"] == "1" and row["threshold_name"] == "fixed_60s")
        self.assertEqual(int(span["threshold_connected_seconds"]), 2)
        self.assertEqual(int(span["unambiguous_connected_seconds"]), 0)
        mask = next(row for row in rows(self.output / "record_masks.csv.gz")
                    if row["file_id"] == str(source["file_id"]) and row["source_line"] == "2")
        self.assertEqual(mask["timestamp"], "1001")
        self.assertEqual(mask["parse_valid"], "0")
        coverage = next(row for row in rows(self.output / "simultaneous_spans.csv")
                        if row["group"] == "MAIN" and row["threshold_seconds"] == "60")
        self.assertEqual(int(coverage["simultaneous_bounded_gap_span_seconds"]), 0)

    def test_exact_histogram_exceeds_256_distinct_deltas_and_span_partition(self):
        timestamp = 1000
        content = ["1000 1\n"]
        for delta in range(1, 301):
            timestamp += delta
            content.append(f"{timestamp} 1\n")
        self.channel(1).write_text("".join(content))
        summary = self.run_audit()
        source = next(item for item in summary["sources"] if item["channel"] == 1)
        histogram = [row for row in rows(self.output / "interval_histogram.csv") if row["channel"] == "1"]
        self.assertEqual(len(histogram), 300)
        self.assertEqual(sum(int(row["frequency"]) for row in histogram), 300)
        self.assertEqual(source["descriptive_modal_interval_seconds"], 1)
        span = next(row for row in rows(self.output / "threshold_spans.csv")
                    if row["channel"] == "1" and row["threshold_name"] == "fixed_30s")
        self.assertEqual(int(span["threshold_connected_seconds"]), sum(range(1, 31)))
        self.assertEqual(int(span["threshold_connected_seconds"]) + int(span["long_interval_seconds"]),
                         source["elapsed_span_seconds"])

    def test_all_malformed_channel_is_reported_without_crash(self):
        self.channel(1).write_text("1000 NaN\nmalformed\n")
        summary = self.run_audit()
        source = next(item for item in summary["sources"] if item["channel"] == 1)
        self.assertEqual(source["unique_timestamps"], 0)
        self.assertIsNone(source["first_valid_timestamp"])
        self.assertEqual(summary["house_summary"][0]["unambiguous_mains_timestamps"], 0)

    def test_legacy_any_member_rule_exposes_partial_group_and_missing_mains_leg(self):
        self.channel(8).write_text("1000 1\n1002 1\n1003 1\n")
        self.channel(2).write_text("1000 1\n1001 1\n1003 1\n")
        summary = self.run_audit()
        house = summary["house_summary"][0]
        self.assertEqual(house["union_timestamps"], 4)
        self.assertEqual(house["unambiguous_mains_timestamps"], 3)
        self.assertEqual(house["unambiguous_complete_timestamps"], 2)
        self.assertEqual(house["legacy_any_member_row_candidates"], 4)
        self.assertEqual(house["legacy_candidates_missing_required_constituents"], 2)
        self.assertEqual(house["channel1_timestamps_missing_unambiguous_main2"], 1)
        partial = {row["group"]: row for row in house["grouped_presence_at_channel1"]}
        self.assertEqual(partial["MAIN"]["partial_group_and_channel1"], 1)
        self.assertEqual(partial["KTCH"]["partial_group_and_channel1"], 1)


if __name__ == "__main__":
    unittest.main()
