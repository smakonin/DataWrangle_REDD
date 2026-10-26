#!/usr/bin/env python3
"""Audit an archived DataWrangle_REDD import log, without loading raw REDD.

All denominators describe the archived pipeline. They are not raw sensor-gap,
time-uptime, energy, or NILM-performance measurements. Power numerals are kept
in the source's integer units rather than assigned an unverified measurand.
"""

from __future__ import annotations

import argparse
import ast
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
from statistics import mean, median
from typing import Any


REPO_COMMIT = "66ea7e01e499be2bb6b88dbaf8f3cee3445cbe0d"
REPO_URL = "https://github.com/smakonin/DataWrangle_REDD"
HOUSE_RE = re.compile(r"^Processing REDD House\s+(\d+)\s*\.\.\.\s*$")
READ_RE = re.compile(r"^Reading file:\s+(.+?/channel_(\d+)\.dat)\s*$")
CHANNEL_RE = re.compile(
    r"^\s*TimeStamp:\s*dmin\s*=\s*(\d+)\s*,\s*dmax\s*=\s*(\d+)"
    r"\s*,\s*dlen\s*=\s*([\d,]+)\s*,\s*actual len\s*=\s*([\d,]+)\s*$"
)
COUNTER_RE = re.compile(r"^ERRORS:\s*(.+?)\s*=\s*([\d,]+)\s*$")
DLEN_RE = re.compile(
    r"^DLEN now\s+([\d,]+)\s+was\s+([\d,]+)\s+shrunk by\s+([\d.]+)\s*%\s*$"
)
OLD_RE = re.compile(
    r"^\s*ERROR:\s*neg noise:\s*(\[[^\]]*\])\s*,\s*sum\s*=\s*(-?[\d,]+)"
    r"\s*,\s*new noise\s*=\s*(-?[\d,]+(?:\.\d+)?)\s*$"
)
NEW_RE = re.compile(r"^\s*NEW:\s*(\[[^\]]*\])\s*$")


def integer(text: str) -> int:
    return int(text.replace(",", ""))


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def metadata_path(path: Path) -> str:
    """Use portable paths in published provenance; hashes identify the bytes."""
    try:
        return path.resolve().relative_to(Path(__file__).resolve().parents[1]).as_posix()
    except ValueError:
        return path.name


def utc_timestamp(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def numeric_vector(text: str, location: str) -> list[int]:
    value = ast.literal_eval(text)
    require(isinstance(value, list) and len(value) >= 4, f"Invalid vector at {location}")
    require(all(type(item) is int for item in value), f"Noninteger vector at {location}")
    return value


def quantile(values: list[float], probability: float) -> float | None:
    """Linear quantile, equivalent to Hyndman-Fan type 7 / NumPy default."""
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def distribution(values: list[float]) -> dict[str, int | float | None]:
    return {
        "count": len(values),
        "min": min(values) if values else None,
        "median": median(values) if values else None,
        "p90": quantile(values, 0.90),
        "p95": quantile(values, 0.95),
        "max": max(values) if values else None,
        "mean": mean(values) if values else None,
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    require(bool(rows), f"No rows for {path.name}")
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def parse_log(log_path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    houses: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    pending_channel: dict[str, Any] | None = None
    pending_rebuild: dict[str, Any] | None = None

    for line_number, line in enumerate(log_path.read_text(encoding="utf-8").splitlines(), 1):
        match = HOUSE_RE.match(line)
        if match:
            require(pending_rebuild is None, f"Unpaired rebuild before line {line_number}")
            require(pending_channel is None, f"Unpaired channel before line {line_number}")
            current = {"house": int(match[1]), "start_line": line_number, "channels": [], "counters": {}}
            houses.append(current)
            continue
        if current is None:
            require(not (OLD_RE.match(line) or NEW_RE.match(line)), "Event outside house section")
            continue
        match = READ_RE.match(line)
        if match:
            require(pending_channel is None, f"Missing channel metadata before line {line_number}")
            pending_channel = {"source_file": match[1], "channel": int(match[2]), "source_line": line_number}
            continue
        match = CHANNEL_RE.match(line)
        if match:
            require(pending_channel is not None, f"Unassociated channel metadata at line {line_number}")
            pending_channel.update({
                "first_timestamp": integer(match[1]),
                "last_timestamp": integer(match[2]),
                "grid_rows": integer(match[3]),
                "observed_rows": integer(match[4]),
                "metadata_line": line_number,
            })
            current["channels"].append(pending_channel)
            pending_channel = None
            continue
        match = COUNTER_RE.match(line)
        if match:
            require(match[1] not in current["counters"], f"Duplicate counter at line {line_number}")
            current["counters"][match[1]] = integer(match[2])
            continue
        match = DLEN_RE.match(line)
        if match:
            current.update({"exported_rows": integer(match[1]), "grid_rows": integer(match[2]),
                            "reported_shrinkage_percent": float(match[3]), "summary_line": line_number})
            continue
        if line.startswith("Data saved to:"):
            current["historical_export_path"] = line.split(":", 1)[1].strip()
            continue
        match = OLD_RE.match(line)
        if match:
            require(pending_rebuild is None, f"Missing NEW pair before line {line_number}")
            pending_rebuild = {
                "house": current["house"], "source_old_line": line_number,
                "old_vector": numeric_vector(match[1], f"line {line_number}"),
                "stated_submeter_sum": integer(match[2]),
                "stated_new_residual": float(match[3].replace(",", "")),
            }
            continue
        match = NEW_RE.match(line)
        if match:
            require(pending_rebuild is not None, f"Unassociated NEW vector at line {line_number}")
            before = pending_rebuild["old_vector"]
            after = numeric_vector(match[1], f"line {line_number}")
            location = f"house {current['house']}, lines {pending_rebuild['source_old_line']}-{line_number}"
            require(len(before) == len(after), f"Vector width changed at {location}")
            require(before[0] == after[0], f"Timestamp changed at {location}")
            require(before[2:-1] == after[2:-1], f"Submeter groups changed at {location}")
            submeter_sum = sum(before[2:-1])
            require(submeter_sum == pending_rebuild["stated_submeter_sum"], f"Stated sum mismatch at {location}")
            require(before[1] - submeter_sum == before[-1], f"Original residual mismatch at {location}")
            require(after[1] - submeter_sum == after[-1], f"New residual mismatch at {location}")
            require(after[-1] == pending_rebuild["stated_new_residual"], f"Stated new residual mismatch at {location}")
            require(before[-1] < 0, f"Rebuild without negative residual at {location}")
            delta = after[1] - before[1]
            require(delta == after[-1] - before[-1], f"Delta arithmetic mismatch at {location}")
            events.append({
                "house": current["house"],
                "timestamp_unix_utc": before[0], "timestamp_utc": utc_timestamp(before[0]),
                "main_before": before[1], "main_after": after[1],
                "main_delta": delta, "abs_main_delta": abs(delta),
                "main_after_to_before_ratio": after[1] / before[1] if before[1] != 0 else None,
                "original_residual": before[-1], "new_residual": after[-1],
                "grouped_submeter_sum": submeter_sum,
                "grouped_submeter_count": len(before) - 3,
                "source_old_line": pending_rebuild["source_old_line"], "source_new_line": line_number,
            })
            pending_rebuild = None
            continue
        if "ERROR: neg noise:" in line or "NEW:" in line or "TimeStamp: dmin" in line:
            raise ValueError(f"Recognized marker with unsupported format at line {line_number}")

    require(pending_rebuild is None, "Unpaired final rebuild")
    require(pending_channel is None, "Unpaired final channel")
    require(bool(houses), "No house sections found")
    require(len({house["house"] for house in houses}) == len(houses), "Duplicate house section")
    return houses, events


def build_summary(houses: list[dict[str, Any]], events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    summaries: list[dict[str, Any]] = []
    checks: list[str] = []
    counter_names = {"no timestamp", "incomplete rows", "timestamp lead", "timestamp lag", "negative noise"}
    for house in houses:
        number = house["house"]
        require(set(house["counters"]) == counter_names, f"Unexpected counters for house {number}")
        require(house.get("historical_export_path"), f"Missing export path for house {number}")
        require(house["channels"][0]["channel"] == 1, f"First channel not mains channel 1 for house {number}")
        require(len({channel["channel"] for channel in house["channels"]}) == len(house["channels"]),
                f"Duplicate channel metadata for house {number}")
        for channel in house["channels"]:
            require(channel["last_timestamp"] - channel["first_timestamp"] + 1 == channel["grid_rows"],
                    f"Channel span mismatch for house {number}, channel {channel['channel']}")
            require(0 < channel["observed_rows"] <= channel["grid_rows"],
                    f"Invalid channel row count for house {number}, channel {channel['channel']}")
        mains = house["channels"][0]
        counters = house["counters"]
        grid = house["grid_rows"]
        observed = mains["observed_rows"]
        retained = house["exported_rows"]
        no_timestamp = counters["no timestamp"]
        incomplete = counters["incomplete rows"]
        house_events = [event for event in events if event["house"] == number]
        require(grid == mains["grid_rows"], f"Mains/summary span mismatch for house {number}")
        require(no_timestamp + incomplete + retained == grid, f"Partition mismatch for house {number}")
        require(grid - no_timestamp == observed, f"Observed mains count mismatch for house {number}")
        require(len(house_events) == counters["negative noise"], f"Parsed rebuild count mismatch for house {number}")
        require(abs(house["reported_shrinkage_percent"] - 100 * (1 - retained / grid)) < 0.0051,
                f"Reported shrinkage rounding mismatch for house {number}")
        event_timestamps = [event["timestamp_unix_utc"] for event in house_events]
        require(event_timestamps == sorted(event_timestamps), f"Rebuild events out of order for house {number}")
        require(len(event_timestamps) == len(set(event_timestamps)), f"Duplicate rebuild timestamp for house {number}")
        require(all(mains["first_timestamp"] <= timestamp <= mains["last_timestamp"] for timestamp in event_timestamps),
                f"Rebuild outside mains span for house {number}")
        repairs = counters["timestamp lead"] + counters["timestamp lag"] + counters["negative noise"]
        row = {
            "house": number,
            "mains_first_timestamp_unix_utc": mains["first_timestamp"],
            "mains_last_timestamp_unix_utc": mains["last_timestamp"],
            "mains_first_timestamp_utc": utc_timestamp(mains["first_timestamp"]),
            "mains_last_timestamp_utc": utc_timestamp(mains["last_timestamp"]),
            "grid_rows": grid, "grid_span_days": grid / 86400,
            "observed_mains_channel1_rows": observed,
            "observed_mains_row_density_of_grid": observed / grid,
            "exported_rows": retained,
            "exported_fraction_of_grid": retained / grid,
            "exported_fraction_of_observed_mains_rows": retained / observed,
            "no_timestamp_grid_rows": no_timestamp,
            "no_timestamp_fraction_of_grid": no_timestamp / grid,
            "incomplete_grouped_rows": incomplete,
            "incomplete_fraction_of_grid": incomplete / grid,
            "incomplete_fraction_of_observed_mains_rows": incomplete / observed,
            "lead_substitutions": counters["timestamp lead"],
            "lag_substitutions": counters["timestamp lag"],
            "fallback_rebuilds": counters["negative noise"],
            "all_substitutions_and_rebuilds": repairs,
            "altered_fraction_of_exported_rows": repairs / retained,
            "fallback_fraction_of_exported_rows": counters["negative noise"] / retained,
            "reported_shrinkage_percent": house["reported_shrinkage_percent"],
            "summary_source_line": house["summary_line"],
            "historical_export_path": house["historical_export_path"],
        }
        summaries.append(row)
        checks.append(f"House {number}: span, source-channel counts, row partition, shrinkage rounding, "
                      f"rebuild pair arithmetic, timestamp uniqueness/order/bounds, and {len(house_events)} fallback count reconcile.")
    return summaries, checks


def main() -> None:
    here = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", type=Path, default=here / "DataWrangle_REDD" / "REDD_import.log")
    parser.add_argument("--outdir", type=Path, default=here / "stats")
    args = parser.parse_args()
    source = args.log.resolve()
    outdir = args.outdir.resolve()
    houses, events = parse_log(source)
    summaries, checks = build_summary(houses, events)
    outdir.mkdir(parents=True, exist_ok=True)

    sum_keys = ["grid_rows", "observed_mains_channel1_rows", "exported_rows", "no_timestamp_grid_rows",
                "incomplete_grouped_rows", "lead_substitutions", "lag_substitutions", "fallback_rebuilds",
                "all_substitutions_and_rebuilds"]
    overall = {key: sum(row[key] for row in summaries) for key in sum_keys}
    overall.update({
        "houses": len(summaries),
        "grid_span_house_days": overall["grid_rows"] / 86400,
        "observed_mains_row_density_of_grid": overall["observed_mains_channel1_rows"] / overall["grid_rows"],
        "exported_fraction_of_grid": overall["exported_rows"] / overall["grid_rows"],
        "exported_fraction_of_observed_mains_rows": overall["exported_rows"] / overall["observed_mains_channel1_rows"],
        "no_timestamp_fraction_of_grid": overall["no_timestamp_grid_rows"] / overall["grid_rows"],
        "incomplete_fraction_of_grid": overall["incomplete_grouped_rows"] / overall["grid_rows"],
        "incomplete_fraction_of_observed_mains_rows": overall["incomplete_grouped_rows"] / overall["observed_mains_channel1_rows"],
        "altered_fraction_of_exported_rows": overall["all_substitutions_and_rebuilds"] / overall["exported_rows"],
        "fallback_fraction_of_exported_rows": overall["fallback_rebuilds"] / overall["exported_rows"],
    })
    require(overall["fallback_rebuilds"] == len(events), "Overall fallback count mismatch")
    require(all(event["main_delta"] > 0 for event in events), "A fallback MAIN change was not an increase")
    checks.append(f"All {len(events)} fallback changes are increases; pooled counters and event count reconcile.")

    measures = ["main_before", "main_after", "main_delta", "abs_main_delta", "original_residual",
                "new_residual", "grouped_submeter_sum", "main_after_to_before_ratio"]
    distributions: dict[str, dict[str, Any]] = {}
    distribution_rows: list[dict[str, Any]] = []
    histograms: list[dict[str, Any]] = []
    edges: list[float] = [0, 10, 50, 100, 250, 500, 1000, 2000, 5000, math.inf]
    for key in [str(house["house"]) for house in houses] + ["ALL"]:
        selected = events if key == "ALL" else [event for event in events if str(event["house"]) == key]
        distributions[key] = {}
        for measure in measures:
            values = [event[measure] for event in selected if event[measure] is not None]
            stats = distribution(values)
            distributions[key][measure] = stats
            distribution_rows.append({"house": key, "measure": measure, **stats})
        for low, high in zip(edges, edges[1:]):
            count = sum(low <= event["abs_main_delta"] < high for event in selected)
            histograms.append({
                "house": key, "bin_left_inclusive": low,
                "bin_right_exclusive": high if math.isfinite(high) else None,
                "bin_label": f"{low:g} to <{high:g}" if math.isfinite(high) else f">={low:g}",
                "count": count, "fraction_of_fallback_rebuilds": count / len(selected) if selected else None,
            })
        require(sum(row["count"] for row in histograms if row["house"] == key) == len(selected),
                f"Histogram partition mismatch for {key}")

    metadata = {
        "scope": "Reanalysis of an archived historical import log; no raw REDD ingestion or original Python 2 execution.",
        "source_log_path": metadata_path(source),
        "source_log_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "parser_path": metadata_path(Path(__file__)),
        "parser_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "repository": REPO_URL, "repository_snapshot_commit": REPO_COMMIT,
        "repository_log_url": f"{REPO_URL}/blob/{REPO_COMMIT}/REDD_import.log",
        "timestamp_timezone": "UTC; Unix source timestamps preserved without a local-time assumption.",
        "fraction_scale": "All fraction/density fields are on 0..1 scale; multiply by 100 for percent.",
        "power_units": "Source integer power numerals. Export names say VA, but this analysis does not establish channel measurands, calibration, or physical accuracy.",
        "quantile_definition": "Linear interpolation of sorted values at (n-1)*p; Hyndman-Fan type 7.",
        "source_version_limit": "Archived export paths contain 3sec while the pinned script uses lowf; source/log execution identity is not established.",
        "limits": [
            "Grid-row density is not continuous-time uptime, a raw sensor-missingness rate, or a corruption rate.",
            "Incomplete rows mix cadence, timestamp-coincidence, boundaries, and unavailable grouped fields; causes cannot be separated from this log.",
            "Row-level altered counts include heuristic lead/lag substitutions and fallback rebuilds; only fallback rebuild magnitudes are logged in detail.",
            "Fallback magnitudes describe a selected subset of negative-residual rows, not the distribution of all mismatches or all repairs.",
            "Do not integrate sparse retained/rebuild rows as if they were contiguous one-second energy observations.",
            "No estimate of raw gap lengths, changed energy, model accuracy, or causal score effect is supported by these log-only statistics.",
        ],
    }
    dictionary = {
        "grid_rows": "Inclusive integer-second positions from channel-1 first timestamp to last timestamp, as reported in the archived log.",
        "grid_span_days": "grid_rows/86400; calendar-grid span, not observed recording uptime.",
        "grid_span_house_days": "Sum of six per-house grid spans/86400; household exposure denominator, not one wall-clock interval.",
        "observed_mains_channel1_rows": "Archived source actual len for mains channel 1; not both mains legs verified individually.",
        "observed_mains_row_density_of_grid": "Observed channel-1 rows/grid_rows; describes source timestamp density.",
        "no_timestamp_grid_rows": "Rows skipped by historical no-timestamp counter; grid_rows minus observed channel-1 rows reconciles exactly.",
        "incomplete_grouped_rows": "Rows with a mains timestamp skipped for a sentinel after grouping; not a certified count of failed individual sensors.",
        "exported_rows": "Retained historical rows, including reported substitutions/rebuilds.",
        "lead_substitutions": "Historical timestamp-lead counter; heuristic successful future-mains replacements, not independent clock-error measurements.",
        "lag_substitutions": "Historical timestamp-lag counter; heuristic successful past-mains replacements, not independent clock-error measurements.",
        "fallback_rebuilds": "Historical negative-noise counter; count of paired ERROR: neg noise / NEW records.",
        "main_before": "MAIN value immediately before a logged fallback rebuild, already processed/quantized by the historical importer.",
        "main_after": "MAIN value in its paired NEW vector.",
        "main_delta": "main_after-main_before; not a metrological error estimate or integrated energy.",
        "abs_main_delta": "Absolute logged MAIN change; all parsed fallback changes happen to be positive.",
        "main_after_to_before_ratio": "main_after/main_before, undefined when main_before=0.",
        "grouped_submeter_sum": "Sum of grouped submeter numerals excluding timestamp, MAIN, and residual; validated against each log's sum field.",
        "original_residual": "main_before-grouped_submeter_sum.",
        "new_residual": "main_after-grouped_submeter_sum, also validated against printed new noise.",
    }
    result = {"metadata": metadata, "overall": overall, "houses": summaries,
              "fallback_distributions": distributions,
              "validation": {"status": "passed", "checks": checks, "house_count": len(houses),
                             "paired_fallback_count": len(events)},
              "channels": [{"house": house["house"], **channel} for house in houses for channel in house["channels"]],
              "data_dictionary": dictionary}
    (outdir / "log_statistics.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    (outdir / "data_dictionary.json").write_text(json.dumps(dictionary, indent=2) + "\n", encoding="utf-8")
    write_csv(outdir / "house_summary.csv", summaries)
    write_csv(outdir / "fallback_rebuild_events.csv", events)
    write_csv(outdir / "fallback_distributions.csv", distribution_rows)
    write_csv(outdir / "fallback_delta_histogram.csv", histograms)
    write_csv(outdir / "largest_fallback_changes.csv", sorted(events, key=lambda event: event["abs_main_delta"], reverse=True)[:25])
    print(json.dumps({"validation": result["validation"], "overall": overall,
                      "overall_fallback_delta": distributions["ALL"]["main_delta"],
                      "files": sorted(path.name for path in outdir.iterdir() if path.is_file())}, indent=2))


if __name__ == "__main__":
    main()
