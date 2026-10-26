"""Independent, read-only reconciliation of the completed REDD audit.

Uses saved histograms, independent SQL bitmask aggregation, and fresh timestamp
streams from the read-only audit database. Does not import the primary auditor
or reuse its supported-span table or intersection function. No raw values or
repository/publication files are edited.
"""
from __future__ import annotations

import csv
from collections import Counter, defaultdict
from datetime import datetime, timezone
from fractions import Fraction
import hashlib
import gzip
import json
from pathlib import Path
import sqlite3

BASE = Path(__file__).resolve().parents[1]
RESULTS = BASE / "raw_results"
IMPORTED = BASE / "imported"
OUT = Path(__file__).resolve().parent
THRESHOLDS = (30, 60, 120, 300)


def read_csv(name):
    with (RESULTS / name).open(newline="") as handle:
        return list(csv.DictReader(handle))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


checks = []


def check(name, actual, expected):
    if isinstance(actual, float) or isinstance(expected, float):
        good = abs(actual - expected) <= 1e-12
    else:
        good = actual == expected
    checks.append({"name": name, "passed": good, "actual": actual, "expected": expected})
    if not good:
        print("MISMATCH", name, actual, expected, flush=True)


def weighted_quantile(histogram, probability):
    count = sum(histogram.values())
    if count == 0:
        return None
    rank = (count - 1) * probability
    low = rank.numerator // rank.denominator
    high = low if rank.denominator == 1 else low + 1
    cumulative = 0
    a = b = None
    for value in sorted(histogram):
        following = cumulative + histogram[value]
        if cumulative <= low < following:
            a = value
        if cumulative <= high < following:
            b = value
        cumulative = following
    value = Fraction(a) + (b - a) * (rank - low)
    return int(value) if value.denominator == 1 else float(value)


def timestamp_profile(connection, file_id):
    """Build connected blocks by splitting at long spacings, independently."""
    initial = previous = None
    block_start = {}
    blocks = {threshold: [] for threshold in THRESHOLDS}
    largest = []
    count = 0
    for (timestamp,) in connection.execute(
        "SELECT ts FROM observations WHERE file_id=? ORDER BY ts", (file_id,)
    ):
        if previous is None:
            initial = timestamp
            block_start = dict.fromkeys(THRESHOLDS, timestamp)
        else:
            delta = timestamp - previous
            assert delta > 0
            if delta > 60:
                largest.append((delta, previous, timestamp))
                largest = sorted(largest, reverse=True)[:10]
            for threshold in THRESHOLDS:
                if delta > threshold:
                    if previous > block_start[threshold]:
                        blocks[threshold].append((block_start[threshold], previous))
                    block_start[threshold] = timestamp
        previous = timestamp
        count += 1
    for threshold in THRESHOLDS:
        if previous > block_start[threshold]:
            blocks[threshold].append((block_start[threshold], previous))
    return {
        "first": initial, "last": previous, "observations": count,
        "blocks": blocks, "largest_long_gaps": largest,
    }


def shared_duration(a, b):
    """Two-pointer pairwise interval intersection, not primary k-way code."""
    i = j = total = 0
    while i < len(a) and j < len(b):
        left, right = max(a[i][0], b[j][0]), min(a[i][1], b[j][1])
        if right > left:
            total += right - left
        if a[i][1] < b[j][1]:
            i += 1
        elif b[j][1] < a[i][1]:
            j += 1
        else:
            i += 1
            j += 1
    return total


def main():
    summary = json.loads((RESULTS / "summary.json").read_text())
    sources = summary["sources"]
    histograms = defaultdict(Counter)
    for row in read_csv("interval_histogram.csv"):
        histograms[int(row["file_id"])][int(row["interval_seconds"])] += int(row["frequency"])
        check(f"histogram ambiguity file {row['file_id']} delta {row['interval_seconds']}",
              int(row["conflict_touched"]), 0)
    thresholds = read_csv("threshold_spans.csv")
    simultaneous = read_csv("simultaneous_spans.csv")
    source_by_id = {row["file_id"]: row for row in sources}
    source_by_pair = {(row["house"], row["channel"]): row for row in sources}
    check("files", len(sources), 116)
    totals = {
        key: sum(row[key] for row in sources)
        for key in ("source_rows", "malformed_rows", "identical_to_first_duplicates",
                    "conflicting_with_first_duplicates", "out_of_order_records")
    }
    for key in ("malformed_rows", "identical_to_first_duplicates", "conflicting_with_first_duplicates"):
        check(key, totals[key], 0)

    interval_summaries = []
    for file_id, source in source_by_id.items():
        hist = histograms[file_id]
        count = sum(hist.values())
        span = sum(delta * number for delta, number in hist.items())
        check(f"interval count file {file_id}", count, source["unique_timestamps"] - 1)
        check(f"elapsed partition file {file_id}", span, source["elapsed_span_seconds"])
        mode = min(hist, key=lambda delta: (-hist[delta], delta))
        check(f"modal interval file {file_id}", mode, source["descriptive_modal_interval_seconds"])
        check(f"grid endpoint file {file_id}", source["calendar_grid_positions"], span + 1)
        interval_summaries.append({
            "file_id": file_id, "house": source["house"], "channel": source["channel"],
            "mode": mode, "minimum": min(hist), "median": weighted_quantile(hist, Fraction(1, 2)),
            "p90": weighted_quantile(hist, Fraction(9, 10)),
            "p95": weighted_quantile(hist, Fraction(19, 20)),
            "p99": weighted_quantile(hist, Fraction(99, 100)), "maximum": max(hist),
        })
    for row in thresholds:
        fid, threshold = int(row["file_id"]), int(row["threshold_seconds"])
        histogram = histograms[fid]
        connected = sum(delta * n for delta, n in histogram.items() if delta <= threshold)
        long_seconds = sum(delta * n for delta, n in histogram.items() if delta > threshold)
        long_count = sum(n for delta, n in histogram.items() if delta > threshold)
        name = f"file {fid} {row['threshold_name']}"
        check(name + " connected", connected, int(row["threshold_connected_seconds"]))
        check(name + " long seconds", long_seconds, int(row["long_interval_seconds"]))
        check(name + " long count", long_count, int(row["long_interval_count"]))
        check(name + " span", connected + long_seconds, int(row["elapsed_span_seconds"]))
        check(name + " fraction", connected / (connected + long_seconds), float(row["connected_fraction_of_elapsed_span"]))

    connection = sqlite3.connect(f"file:{RESULTS / 'audit.sqlite3'}?mode=ro", uri=True)
    house_results = []
    for house in summary["house_summary"]:
        number = house["house"]
        channels = house["expected_channels"]
        full = sum(1 << (channel - 1) for channel in channels)
        sub = full ^ 3
        query = """
            SELECT bits,COUNT(*) FROM (
                SELECT ts,SUM(1 << (channel-1)) AS bits
                FROM observations WHERE house=? GROUP BY ts
            ) GROUP BY bits
        """
        masks = dict(connection.execute(query, (number,)))
        union = sum(masks.values())
        mains = sum(n for bits, n in masks.items() if bits & 3 == 3)
        complete = masks.get(full, 0)
        partial_sub = sum(n for bits, n in masks.items() if bits & sub not in (0, sub))
        partial_main = sum(n for bits, n in masks.items() if bits & 3 not in (0, 3))
        check(f"house {number} exact union", union, house["union_timestamps"])
        check(f"house {number} exact mains", mains, house["unambiguous_mains_timestamps"])
        check(f"house {number} exact all complete", complete, house["unambiguous_complete_timestamps"])
        check(f"house {number} main timestamp equality", partial_main, 0)
        check(f"house {number} all-circuit timestamp equality", partial_sub, 0)
        for group in house["group_overlap"]:
            required = 3 | sum(1 << (channel - 1) for channel in group["channels"])
            count = sum(n for bits, n in masks.items() if bits & required == required)
            check(f"house {number} group {group['group']} exact", count,
                  group["unambiguous_group_and_mains_timestamps"])
            check(f"house {number} group {group['group']} fraction", count / mains,
                  group["fraction_of_unambiguous_mains"])
        manifest = json.loads((IMPORTED / f"house_{number}" / "manifest.json").read_text())
        check(f"house {number} importer union", manifest["counts"]["timestamp_rows"], union)
        check(f"house {number} importer complete", manifest["counts"]["complete_rows"], complete)
        check(f"house {number} importer mains", manifest["counts"]["mains_complete_rows"], mains)
        for source in manifest["source_files"]:
            raw = source_by_pair[number, source["channel"]]
            for key in ("sha256", "size_bytes", "source_rows", "out_of_order_records"):
                check(f"house {number} channel {source['channel']} importer {key}", source[key], raw[key])

        # Zero partial masks above independently establish that every circuit in
        # this particular archive has the same timestamps. Therefore two streams
        # suffice for the all-channel interval intersection; no such assumption
        # is made for a different archive.
        assert partial_main == 0 and partial_sub == 0
        profiles = {}
        for channel in (1, 3):
            source = source_by_pair[number, channel]
            profiles[channel] = timestamp_profile(connection, source["file_id"])
            check(f"house {number} channel {channel} fresh timestamp count",
                  profiles[channel]["observations"], source["unique_timestamps"])
        first = max(profiles[c]["first"] for c in profiles)
        last = min(profiles[c]["last"] for c in profiles)
        main_span = profiles[1]["last"] - profiles[1]["first"]
        common_span = last - first
        shared = {}
        for threshold in THRESHOLDS:
            shared[threshold] = shared_duration(profiles[1]["blocks"][threshold], profiles[3]["blocks"][threshold])
            for row in simultaneous:
                if int(row["house"]) != number or int(row["threshold_seconds"]) != threshold:
                    continue
                expected = (sum(b-a for a, b in profiles[1]["blocks"][threshold])
                            if row["group"] == "MAIN" else shared[threshold])
                denominator = main_span if row["group"] == "MAIN" else common_span
                tag = f"house {number} group {row['group']} T{threshold}"
                check(tag + " fresh shared duration", expected, int(row["simultaneous_bounded_gap_span_seconds"]))
                check(tag + " main denominator", main_span, int(row["mains_elapsed_window_seconds"]))
                check(tag + " common denominator", denominator, int(row["common_elapsed_window_seconds"]))
                check(tag + " main fraction", expected / main_span, float(row["fraction_of_mains_window"]))
                check(tag + " common fraction", expected / denominator, float(row["fraction_of_common_window"]))
        biggest = []
        for channel, profile in profiles.items():
            for delta, before, after in profile["largest_long_gaps"]:
                biggest.append({
                    "channel_class": "mains (both legs)" if channel == 1 else "all circuits",
                    "gap_seconds": delta,
                    "before_utc": datetime.fromtimestamp(before, timezone.utc).isoformat(),
                    "after_utc": datetime.fromtimestamp(after, timezone.utc).isoformat(),
                })
        house_results.append({
            "house": number, "union": union, "mains_exact": mains, "all_complete_exact": complete,
            "all_complete_fraction_of_mains": complete / mains,
            "all_complete_fraction_of_union": complete / union,
            "mains_elapsed_span_seconds": main_span, "common_elapsed_span_seconds": common_span,
            "shared_threshold_connected_seconds": shared,
            "shared_fraction_of_mains_window": {t: d/main_span for t, d in shared.items()},
            "shared_fraction_of_common_window": {t: d/common_span for t, d in shared.items()},
            "timestamp_mask_distribution": masks,
            "largest_long_gaps": sorted(biggest, key=lambda x: x["gap_seconds"], reverse=True)[:10],
        })
        print(f"Crosschecked house {number}: U={union}, M={mains}, C={complete}, shared60={shared[60]}", flush=True)
    connection.close()
    with gzip.open(RESULTS / "issues.csv.gz", "rt", newline="") as handle:
        descending = [row for row in csv.DictReader(handle) if row["kind"] == "out_of_order"]
    order_summary = json.loads((BASE / "source_order_summary.json").read_text())
    check("descending source rows independently counted", len(descending), order_summary["flagged_source_rows"])
    unique_order_keys = {(row["house"], row["timestamp"]) for row in descending}
    check("distinct descending house/timestamp pairs", len(unique_order_keys), order_summary["distinct_house_timestamp_pairs"])
    for house in order_summary["houses"]:
        rows = [row for row in descending if int(row["house"]) == house["house"]]
        check(f"house {house['house']} descending source rows", len(rows), house["flagged_rows"])
        check(f"house {house['house']} descending distinct timestamps", len({row["timestamp"] for row in rows}), house["distinct_timestamps"])
        check(f"house {house['house']} descending channels", len({row["channel"] for row in rows}), house["channels_with_flags"])
    totals["distinct_descending_house_timestamp_pairs"] = len(unique_order_keys)
    payload = {
        "scope": "Independent reconciliation of saved raw audit, read-only SQLite timestamps, and importer manifests; no model inference or physical-error claim.",
        "inputs": {name: sha(RESULTS / name) for name in ("summary.json", "source_files.csv", "interval_histogram.csv", "threshold_spans.csv", "simultaneous_spans.csv")},
        "totals": totals, "checks": len(checks), "failed_checks": [c for c in checks if not c["passed"]],
        "house_results": house_results, "interval_summaries": interval_summaries,
        "definitions": {
            "span": "last timestamp minus first; no implicit endpoint duration",
            "shared": "Intersection of half-open intervals connecting consecutive observations no more than T seconds apart; not measured continuous uptime.",
            "primary_threshold": 60,
            "long_gaps": "Consecutive sorted timestamp spacings strictly exceeding 60 seconds; no cause inferred.",
            "quantiles": "Type 7, zero-based rank(n-1)p, exact weighted rank from histogram",
        },
    }
    (OUT / "independent_checks.json").write_text(json.dumps(payload, indent=2) + "\n")
    coverage_rows = []
    longest_rows = []
    for house in house_results:
        coverage_rows.append({
            key: house[key] for key in (
                "house", "union", "mains_exact", "all_complete_exact",
                "all_complete_fraction_of_mains", "all_complete_fraction_of_union",
                "mains_elapsed_span_seconds", "common_elapsed_span_seconds",
            )
        })
        for threshold in THRESHOLDS:
            coverage_rows[-1][f"shared_seconds_T{threshold}"] = house["shared_threshold_connected_seconds"][threshold]
            coverage_rows[-1][f"shared_fraction_mains_T{threshold}"] = house["shared_fraction_of_mains_window"][threshold]
            coverage_rows[-1][f"shared_fraction_common_T{threshold}"] = house["shared_fraction_of_common_window"][threshold]
        longest_rows.extend({"house": house["house"], **row} for row in house["largest_long_gaps"])
    for filename, rows in (("house_coverage_crosscheck.csv", coverage_rows), ("longest_gaps_crosscheck.csv", longest_rows)):
        with (OUT / filename).open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    print(f"Checks: {len(checks)}; failures: {len(payload['failed_checks'])}", flush=True)
    if payload["failed_checks"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
