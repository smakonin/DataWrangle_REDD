"""Summarize verified raw/import outputs without imputing or rewriting values."""
import csv
from decimal import Decimal, localcontext
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
RAW = HERE / "raw_results"
OUT = HERE / "reviewed"
OUT.mkdir(exist_ok=True)

def read_csv(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))

def write_csv(path, rows):
    if not rows:
        return
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

def quantile(values, p):
    if not values:
        return None
    values = sorted(values)
    rank = Decimal(len(values) - 1) * Decimal(str(p))
    lo = int(rank)
    hi = min(len(values) - 1, lo + 1)
    return values[lo] + (values[hi] - values[lo]) * (rank - lo)

def histogram_quantile(histogram, p):
    total = sum(n for _, n in histogram)
    if not total:
        return None
    rank = Decimal(total - 1) * Decimal(str(p))
    lo = int(rank); hi = min(total - 1, lo + 1)
    positions = {}
    cumulative = 0
    for value, count in sorted(histogram):
        for position in (lo, hi):
            if position not in positions and position < cumulative + count:
                positions[position] = Decimal(value)
        cumulative += count
    return positions[lo] + (positions[hi] - positions[lo]) * (rank - lo)

def numeric(value):
    return None if value is None else float(value)

raw = json.loads((RAW / "summary.json").read_text())
execution = json.loads((HERE / "import_execution.json").read_text())
assert execution["status"] == "success"
intervals = {}
for row in read_csv(RAW / "interval_histogram.csv"):
    key = (int(row["house"]), int(row["channel"]))
    intervals.setdefault(key, []).append((int(row["interval_seconds"]), int(row["frequency"])))
interval_rows = []
for source in raw["sources"]:
    histogram = intervals[(source["house"], source["channel"])]
    assert sum(n for _, n in histogram) == source["unique_timestamps"] - 1
    assert sum(delta * n for delta, n in histogram) == source["elapsed_span_seconds"]
    interval_rows.append({
        "house": source["house"], "channel": source["channel"],
        "unique_timestamps": source["unique_timestamps"],
        "mode_seconds": source["descriptive_modal_interval_seconds"],
        "mode_fraction_of_intervals": source["modal_interval_frequency"] / source["positive_interval_count"],
        "median_seconds": numeric(histogram_quantile(histogram, .5)),
        "p90_seconds": numeric(histogram_quantile(histogram, .9)),
        "p99_seconds": numeric(histogram_quantile(histogram, .99)),
        "max_seconds": max(delta for delta, _ in histogram),
        "intervals_gt_60_seconds": sum(n for delta, n in histogram if delta > 60),
        "span_seconds_in_intervals_gt_60": sum(delta * n for delta, n in histogram if delta > 60),
    })
write_csv(OUT / "channel_intervals.csv", interval_rows)

balance_rows, events, all_negative = [], [], []
for house in range(1, 7):
    print("Summarizing balances, house", house, flush=True)
    counts = {"eligible": 0, "negative": 0, "zero": 0, "positive": 0,
              "main_abs_ge_10": 0, **{f"residual_lt_minus_{a}": 0 for a in [1, 5, 10, 50]},
              **{f"relative_negative_gt_{q}_pct": 0 for q in [1, 5, 10]}}
    negative = []
    absolute_main = Decimal(0)
    absolute_residual = Decimal(0)
    minimum = maximum = None
    with localcontext() as context:
        context.prec = 64
        with (HERE / f"imported/house_{house}/loads.csv").open(newline="") as handle:
            reader = csv.reader(handle)
            fields = next(reader)
            main_index, residual_index = fields.index("MAIN"), fields.index("DIFF")
            for row in reader:
                if not row[residual_index]:
                    continue
                main, residual = Decimal(row[main_index]), Decimal(row[residual_index])
                counts["eligible"] += 1
                counts["negative" if residual < 0 else "positive" if residual > 0 else "zero"] += 1
                absolute_main += abs(main)
                absolute_residual += abs(residual)
                minimum = residual if minimum is None else min(minimum, residual)
                maximum = residual if maximum is None else max(maximum, residual)
                if residual < 0:
                    negative.append(-residual)
                    events.append({"house": house, "timestamp": int(row[0]),
                                   "MAIN": str(main), "residual": str(residual),
                                   "negative_magnitude": str(-residual)})
                for a in [1, 5, 10, 50]:
                    counts[f"residual_lt_minus_{a}"] += int(residual < -a)
                if abs(main) >= 10:
                    counts["main_abs_ge_10"] += 1
                    for q in [1, 5, 10]:
                        counts[f"relative_negative_gt_{q}_pct"] += int(
                            residual < -Decimal(q) / 100 * abs(main))
    manifest = json.loads((HERE / f"imported/house_{house}/manifest.json").read_text())
    assert counts["eligible"] == manifest["counts"]["complete_rows"]
    assert counts["negative"] == manifest["counts"]["negative_residual_rows"]
    all_negative.extend(negative)
    balance_rows.append({
        "house": house, **counts, "negative_fraction": counts["negative"] / counts["eligible"],
        "negative_magnitude_median": numeric(quantile(negative, .5)),
        "negative_magnitude_p90": numeric(quantile(negative, .9)),
        "negative_magnitude_p95": numeric(quantile(negative, .95)),
        "negative_magnitude_max": numeric(max(negative, default=None)),
        "minimum_residual": numeric(minimum), "maximum_residual": numeric(maximum),
        "sum_abs_MAIN": str(absolute_main), "sum_abs_residual": str(absolute_residual),
        "sample_weighted_abs_residual_ratio": numeric(absolute_residual / absolute_main)
    })
write_csv(OUT / "balance_by_house.csv", balance_rows)
write_csv(OUT / "negative_residual_events.csv", events)

spans = read_csv(RAW / "simultaneous_spans.csv")
summary_rows = []
for record in raw["house_summary"]:
    house = record["house"]
    source = next(s for s in raw["sources"] if s["house"] == house and s["channel"] == 1)
    all60 = next(r for r in spans if int(r["house"]) == house and r["group"] == "ALL_REQUIRED" and r["threshold_seconds"] == "60")
    main60 = next(r for r in spans if int(r["house"]) == house and r["group"] == "MAIN" and r["threshold_seconds"] == "60")
    summary_rows.append({
        "house": house,
        "channel_count": len(record["expected_channels"]),
        "source_observations": sum(s["source_rows"] for s in raw["sources"] if s["house"] == house),
        "source_order_descents": sum(s["out_of_order_records"] for s in raw["sources"] if s["house"] == house),
        "union_timestamps": record["union_timestamps"],
        "both_mains_timestamps": record["unambiguous_mains_timestamps"],
        "all_channel_complete_timestamps": record["unambiguous_complete_timestamps"],
        "exact_complete_fraction_of_mains": record["complete_fraction_of_mains"],
        "mains_envelope_seconds": int(all60["mains_elapsed_window_seconds"]),
        "mains_envelope_days": int(all60["mains_elapsed_window_seconds"]) / 86400,
        "mains_connected_60_seconds": int(main60["simultaneous_bounded_gap_span_seconds"]),
        "all_channel_connected_60_seconds": int(all60["simultaneous_bounded_gap_span_seconds"]),
        "mains_connected_fraction": float(main60["fraction_of_mains_window"]),
        "all_channel_connected_fraction": float(all60["fraction_of_mains_window"]),
        "all_channel_common_endpoint_seconds": int(all60["common_elapsed_window_seconds"]),
        "main_mode_seconds": source["descriptive_modal_interval_seconds"],
        "circuit_mode_seconds": next(s for s in raw["sources"] if s["house"] == house and s["channel"] == 3)["descriptive_modal_interval_seconds"],
        "legacy_candidates_missing_constituents": record["legacy_candidates_missing_required_constituents"],
        "negative_residuals": balance_rows[house - 1]["negative"],
        "negative_residual_fraction": balance_rows[house - 1]["negative_fraction"],
        "longest_main_gap_seconds": next(s for s in interval_rows if s["house"] == house and s["channel"] == 1)["max_seconds"],
    })
write_csv(OUT / "house_summary.csv", summary_rows)
pooled = {key: sum(row[key] for row in summary_rows) for key in [
    "source_observations", "source_order_descents", "union_timestamps", "both_mains_timestamps",
    "all_channel_complete_timestamps", "mains_envelope_seconds", "mains_connected_60_seconds",
    "all_channel_connected_60_seconds", "negative_residuals"]}
pooled.update({
    "exact_complete_fraction_of_mains": pooled["all_channel_complete_timestamps"] / pooled["both_mains_timestamps"],
    "negative_residual_fraction": pooled["negative_residuals"] / pooled["all_channel_complete_timestamps"],
    "shared_connected_fraction_of_mains_envelopes": pooled["all_channel_connected_60_seconds"] / pooled["mains_envelope_seconds"],
    "negative_magnitude_median": numeric(quantile(all_negative, .5)),
    "negative_magnitude_p90": numeric(quantile(all_negative, .9)),
    "negative_magnitude_max": numeric(max(all_negative)),
})
(OUT / "summary.json").write_text(json.dumps({"scope": "Fresh raw-archive audit and unchanged importer outputs",
                                             "pooled": pooled, "houses": summary_rows,
                                             "balances": balance_rows}, indent=2) + "\n")
print(json.dumps(pooled, indent=2), flush=True)
