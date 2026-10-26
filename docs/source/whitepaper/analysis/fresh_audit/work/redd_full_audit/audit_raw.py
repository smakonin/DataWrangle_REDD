#!/usr/bin/env python3
"""Read-only, disk-backed quality audit of canonical REDD house directories.

This diagnostic audit continues after malformed/conflicting observations. It is
not the strict importer, does not repair data, and does not certify measurements.
"""
from __future__ import annotations

import argparse
from collections import Counter
import csv
from decimal import Decimal
from fractions import Fraction
import gzip
import hashlib
import heapq
import importlib.util
import itertools
import json
import math
from pathlib import Path
import re
import sqlite3
import tempfile


HERE = Path(__file__).resolve().parent
IMPORTER_PATH = HERE.parent / "DataWrangle_REDD" / "Import_REDD_dataset.py"
spec = importlib.util.spec_from_file_location("raw_audit_importer_helpers", IMPORTER_PATH)
assert spec and spec.loader
IMPORTER = importlib.util.module_from_spec(spec)
spec.loader.exec_module(IMPORTER)
FIXED_THRESHOLDS = (30, 60, 120, 300)
CADENCE_MULTIPLIERS = (Fraction(3, 2), Fraction(2), Fraction(3), Fraction(5))
BATCH_INTERVALS = 4096


def digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def csv_output(path, fields, compressed=False):
    handle = (gzip.open(path, "wt", encoding="utf-8", newline="", compresslevel=1)
              if compressed else path.open("w", encoding="utf-8", newline=""))
    writer = csv.DictWriter(handle, fieldnames=fields)
    writer.writeheader()
    return handle, writer


def ratio(a, b):
    return a / b if b else None


def known_timestamp(raw):
    try:
        fields = raw.decode("utf-8-sig").split()
        if fields and IMPORTER._TIMESTAMP.fullmatch(fields[0]):
            timestamp = int(fields[0])
            return timestamp if -(2**63) <= timestamp < 2**63 else None
    except (ValueError, UnicodeDecodeError):
        pass
    return None


def source_audit(db, path, file_id, house, channel, mask, issue):
    before = path.stat()
    metadata = dict(file_id=file_id, house=house, channel=channel,
                    source_path=f"house_{house}/{path.name}", source_rows=0,
                    valid_rows=0, malformed_rows=0, identical_to_first_duplicates=0,
                    conflicting_with_first_duplicates=0, out_of_order_records=0)
    digest = hashlib.sha256()
    size = 0
    previous = None
    with path.open("rb") as handle:
        for line, raw in enumerate(handle, 1):
            digest.update(raw)
            size += len(raw)
            metadata["source_rows"] += 1
            flags = dict(file_id=file_id, source_line=line, timestamp="", parse_valid=0,
                         duplicate_status="invalid", out_of_order=0)
            try:
                timestamp, value = IMPORTER._parse_record(raw, path, line)
            except (ValueError, OverflowError) as exc:
                metadata["malformed_rows"] += 1
                timestamp = known_timestamp(raw)
                if timestamp is not None:
                    flags["timestamp"] = timestamp
                    db.execute("INSERT INTO bad_keys(file_id,ts,invalid_records) VALUES(?,?,1) "
                               "ON CONFLICT(file_id,ts) DO UPDATE SET invalid_records=invalid_records+1",
                               (file_id, timestamp))
                issue.writerow(dict(house=house, channel=channel, file_id=file_id,
                                    source_line=line, timestamp=timestamp if timestamp is not None else "",
                                    kind="malformed", detail=str(exc)))
                mask.writerow(flags)
                continue
            metadata["valid_rows"] += 1
            flags.update(timestamp=timestamp, parse_valid=1, duplicate_status="first")
            if previous is not None and timestamp < previous:
                flags["out_of_order"] = 1
                metadata["out_of_order_records"] += 1
                issue.writerow(dict(house=house, channel=channel, file_id=file_id,
                                    source_line=line, timestamp=timestamp, kind="out_of_order",
                                    detail=f"Previous parsed source timestamp: {previous}"))
            previous = timestamp
            inserted = db.execute(
                "INSERT INTO observations(file_id,house,channel,ts,value,source_line,conflicts) "
                "VALUES(?,?,?,?,?,?,0) ON CONFLICT(file_id,ts) DO NOTHING",
                (file_id, house, channel, timestamp, str(value), line))
            if inserted.rowcount == 0:
                old, first_line = db.execute(
                    "SELECT value,source_line FROM observations WHERE file_id=? AND ts=?",
                    (file_id, timestamp)).fetchone()
                if Decimal(old) == value:
                    flags["duplicate_status"] = "identical_to_first"
                    metadata["identical_to_first_duplicates"] += 1
                else:
                    flags["duplicate_status"] = "conflicting_with_first"
                    metadata["conflicting_with_first_duplicates"] += 1
                    db.execute("UPDATE observations SET conflicts=conflicts+1 WHERE file_id=? AND ts=?",
                               (file_id, timestamp))
                issue.writerow(dict(house=house, channel=channel, file_id=file_id,
                                    source_line=line, timestamp=timestamp, kind=flags["duplicate_status"],
                                    detail=f"Compared numerically with first valid record at line {first_line}"))
            mask.writerow(flags)
    after = path.stat()
    metadata.update(sha256=digest.hexdigest(), size_bytes=size,
                    source_stat_unchanged=(before.st_size == after.st_size and
                                           before.st_mtime_ns == after.st_mtime_ns),
                    first_valid_timestamp=None, last_valid_timestamp=None)
    metadata["unique_timestamps"], metadata["conflicting_timestamps"] = db.execute(
        "SELECT COUNT(*),COALESCE(SUM(conflicts>0),0) FROM observations WHERE file_id=?", (file_id,)).fetchone()
    metadata["timestamped_invalid_records"], metadata["unique_invalid_timestamps"] = db.execute(
        "SELECT COALESCE(SUM(invalid_records),0),COUNT(*) FROM bad_keys WHERE file_id=?", (file_id,)).fetchone()
    metadata["unambiguous_unique_timestamps"] = db.execute(
        "SELECT COUNT(*) FROM observations o LEFT JOIN bad_keys b ON o.file_id=b.file_id AND o.ts=b.ts "
        "WHERE o.file_id=? AND o.conflicts=0 AND b.ts IS NULL", (file_id,)).fetchone()[0]
    assert (metadata["valid_rows"] == metadata["unique_timestamps"] +
            metadata["identical_to_first_duplicates"] + metadata["conflicting_with_first_duplicates"])
    assert metadata["source_rows"] == metadata["valid_rows"] + metadata["malformed_rows"]
    return metadata


def timeline(db, file_id):
    good = ((ts, 1, int(bool(conflicts))) for ts, conflicts in db.execute(
        "SELECT ts,conflicts FROM observations WHERE file_id=? ORDER BY ts", (file_id,)))
    bad = ((ts, 0, 1) for (ts,) in db.execute(
        "SELECT ts FROM bad_keys WHERE file_id=? ORDER BY ts", (file_id,)))
    for timestamp, items in itertools.groupby(heapq.merge(good, bad), key=lambda row: row[0]):
        items = list(items)  # At most two rows: one unique reading and one bad-key marker.
        yield timestamp, any(item[1] for item in items), any(item[2] for item in items)


def make_intervals(db, metadata):
    file_id = metadata["file_id"]
    frequencies = Counter()
    # Values stored as decimal TEXT also support a full signed-64-bit timestamp
    # range whose positive difference can exceed SQLite's signed integer range.
    previous = None
    tainted_between = False
    pending = {threshold: None for threshold in FIXED_THRESHOLDS}
    sequences = Counter()
    def close_span(threshold):
        if pending[threshold] is not None:
            db.execute("INSERT INTO supported_spans(file_id,threshold,seq,start,end) VALUES(?,?,?,?,?)",
                       (file_id, threshold, sequences[threshold], *pending[threshold]))
            sequences[threshold] += 1
            pending[threshold] = None
    for timestamp, valid, tainted in timeline(db, file_id):
        if not valid:
            tainted_between = True
            continue
        if previous is not None:
            old_ts, old_tainted = previous
            delta = timestamp-old_ts
            touched = bool(tainted or old_tainted or tainted_between)
            frequencies[(str(delta), int(touched))] += 1
            if len(frequencies) >= BATCH_INTERVALS:
                flush_intervals(db, file_id, frequencies)
            for threshold in FIXED_THRESHOLDS:
                if not touched and delta <= threshold:
                    pending[threshold] = ((pending[threshold][0], timestamp)
                                          if pending[threshold] is not None and pending[threshold][1] == old_ts
                                          else (old_ts, timestamp))
                else:
                    close_span(threshold)
        else:
            metadata["first_valid_timestamp"] = timestamp
        metadata["last_valid_timestamp"] = timestamp
        previous = timestamp, tainted
        tainted_between = False
    for threshold in FIXED_THRESHOLDS:
        close_span(threshold)
    flush_intervals(db, file_id, frequencies)
    first, last = metadata["first_valid_timestamp"], metadata["last_valid_timestamp"]
    metadata["elapsed_span_seconds"] = last-first if first is not None else 0
    metadata["calendar_grid_positions"] = last-first+1 if first is not None else 0
    metadata["timestamp_density_on_1s_grid"] = ratio(metadata["unique_timestamps"], metadata["calendar_grid_positions"])
    mode = db.execute(
        "SELECT delta,SUM(frequency) AS n FROM intervals WHERE file_id=? GROUP BY delta "
        "ORDER BY n DESC,LENGTH(delta),delta LIMIT 1", (file_id,)).fetchone()
    metadata["descriptive_modal_interval_seconds"] = int(mode[0]) if mode else None
    metadata["modal_interval_frequency"] = mode[1] if mode else 0
    totals = db.execute("SELECT COALESCE(SUM(frequency),0) FROM intervals WHERE file_id=?", (file_id,)).fetchone()[0]
    assert totals == max(0, metadata["unique_timestamps"]-1)
    metadata["positive_interval_count"] = totals


def flush_intervals(db, file_id, frequencies):
    db.executemany(
        "INSERT INTO intervals(file_id,delta,conflict_touched,frequency) VALUES(?,?,?,?) "
        "ON CONFLICT(file_id,delta,conflict_touched) DO UPDATE SET frequency=frequency+excluded.frequency",
        ((file_id, delta, touched, count) for (delta, touched), count in frequencies.items()))
    frequencies.clear()


def span_statistics(db, metadata, writer):
    definitions = [(f"fixed_{threshold}s", threshold) for threshold in FIXED_THRESHOLDS]
    cadence = metadata["descriptive_modal_interval_seconds"]
    if cadence is not None:
        definitions.extend((f"modal_x{float(multiplier):g}", math.ceil(cadence*multiplier))
                           for multiplier in CADENCE_MULTIPLIERS)
    metrics = {threshold: dict(connected=0, long_gap=0, long_count=0, conflict_touched=0,
                              unambiguous_connected=0) for _, threshold in definitions}
    for delta, touched, frequency in db.execute(
            "SELECT delta,conflict_touched,frequency FROM intervals WHERE file_id=?", (metadata["file_id"],)):
        delta = int(delta)
        for threshold, item in metrics.items():
            if delta <= threshold:
                item["connected"] += delta*frequency
                if not touched:
                    item["unambiguous_connected"] += delta*frequency
            else:
                item["long_gap"] += delta*frequency
                item["long_count"] += frequency
            if touched:
                item["conflict_touched"] += delta*frequency
    for name, threshold in definitions:
        item = metrics[threshold]
        assert item["connected"] + item["long_gap"] == metadata["elapsed_span_seconds"]
        writer.writerow(dict(file_id=metadata["file_id"], house=metadata["house"], channel=metadata["channel"],
                             threshold_name=name, threshold_seconds=threshold,
                             elapsed_span_seconds=metadata["elapsed_span_seconds"],
                             threshold_connected_seconds=item["connected"],
                             long_interval_seconds=item["long_gap"], long_interval_count=item["long_count"],
                             conflict_touched_interval_seconds=item["conflict_touched"],
                             unambiguous_connected_seconds=item["unambiguous_connected"],
                             connected_fraction_of_elapsed_span=ratio(item["connected"], metadata["elapsed_span_seconds"]),
                             unambiguous_connected_fraction=ratio(item["unambiguous_connected"], metadata["elapsed_span_seconds"])))


def valid_intervals(db, file_id, threshold):
    """Stream precomputed supported spans, including no tainted-key bridges."""
    yield from db.execute("SELECT start,end FROM supported_spans WHERE file_id=? AND threshold=? ORDER BY seq",
                          (file_id, threshold))


def intersect_duration(generators, clip=None):
    if not generators:
        return 0
    current = [next(generator, None) for generator in generators]
    total = 0
    while all(interval is not None for interval in current):
        left = max(interval[0] for interval in current)
        right = min(interval[1] for interval in current)
        if clip is not None:
            left, right = max(left, clip[0]), min(right, clip[1])
        total += max(0, right-left)
        end = min(interval[1] for interval in current)
        for index, interval in enumerate(current):
            if interval[1] == end:
                current[index] = next(generators[index], None)
    return total


def house_statistics(db, house, sources, row_masks, coverage_writer):
    expected_groups = IMPORTER.HOUSE_CHANNEL_GROUPS[house]
    expected = set(itertools.chain.from_iterable(expected_groups.values()))
    files = {source["channel"]: source for source in sources if source["house"] == house}
    counts = dict(union_timestamps=0, required_channels_present_timestamps=0,
                  unambiguous_mains_timestamps=0, unambiguous_complete_timestamps=0,
                  channel1_unambiguous_timestamps=0, channel1_present_timestamps=0,
                  channel1_timestamps_missing_unambiguous_main2=0,
                  legacy_any_member_row_candidates=0,
                  legacy_candidates_missing_required_constituents=0)
    group_counts = {name: 0 for name in expected_groups if name != "MAIN"}
    partial_counts = {name: Counter() for name in expected_groups}
    query = db.execute("SELECT o.ts,o.channel,o.conflicts,COALESCE(b.invalid_records,0) FROM observations o "
                       "LEFT JOIN bad_keys b ON o.file_id=b.file_id AND o.ts=b.ts "
                       "WHERE o.house=? ORDER BY o.ts,o.channel", (house,))
    for timestamp, rows in itertools.groupby(query, key=lambda row: row[0]):
        rows = list(rows)  # At most one row per original channel at this timestamp.
        observed = {row[1] for row in rows}
        unambiguous = {row[1] for row in rows if row[2] == 0 and row[3] == 0}
        mains = {1, 2} <= unambiguous
        complete = expected <= unambiguous
        channel1 = 1 in unambiguous
        legacy_candidate = channel1 and all(set(members) & unambiguous for members in expected_groups.values())
        partial_groups = [name for name, members in expected_groups.items()
                          if set(members) & unambiguous and not set(members) <= unambiguous]
        counts["union_timestamps"] += 1
        counts["required_channels_present_timestamps"] += int(expected <= observed)
        counts["unambiguous_mains_timestamps"] += int(mains)
        counts["unambiguous_complete_timestamps"] += int(complete)
        counts["channel1_present_timestamps"] += int(1 in observed)
        counts["channel1_unambiguous_timestamps"] += int(channel1)
        counts["channel1_timestamps_missing_unambiguous_main2"] += int(channel1 and 2 not in unambiguous)
        counts["legacy_any_member_row_candidates"] += int(legacy_candidate)
        counts["legacy_candidates_missing_required_constituents"] += int(legacy_candidate and not complete)
        for group, members in expected_groups.items():
            partial_counts[group]["any_member_and_channel1"] += int(channel1 and bool(set(members) & unambiguous))
            partial_counts[group]["all_members_and_channel1"] += int(channel1 and set(members) <= unambiguous)
            partial_counts[group]["partial_group_and_channel1"] += int(channel1 and group in partial_groups)
            if group != "MAIN":
                group_counts[group] += int(mains and set(members) <= unambiguous)
        row_masks.writerow(dict(house=house, timestamp=timestamp,
                                observed_channels=";".join(map(str, sorted(observed))),
                                unambiguous_channels=";".join(map(str, sorted(unambiguous))),
                                unambiguous_mains=int(mains), unambiguous_complete=int(complete),
                                legacy_any_member_candidate=int(legacy_candidate),
                                legacy_candidate_missing_constituents=int(legacy_candidate and not complete),
                                partial_groups=";".join(partial_groups)))
    counts["complete_fraction_of_union"] = ratio(counts["unambiguous_complete_timestamps"], counts["union_timestamps"])
    counts["complete_fraction_of_mains"] = ratio(counts["unambiguous_complete_timestamps"], counts["unambiguous_mains_timestamps"])
    group_stats = []
    for group, count in group_counts.items():
        group_stats.append(dict(group=group, channels=list(expected_groups[group]),
                                unambiguous_group_and_mains_timestamps=count,
                                fraction_of_unambiguous_mains=ratio(count, counts["unambiguous_mains_timestamps"])))

    # Threshold-connected spans are a sensitivity diagnostic, not measured
    # continuous uptime or an assurance of clock synchronization.
    coverage_sets = {"MAIN": (1, 2), "ALL_REQUIRED": tuple(sorted(expected)),
                     **{group: tuple(sorted({1, 2, *members}))
                        for group, members in expected_groups.items() if group != "MAIN"}}
    if all(channel in files and files[channel]["first_valid_timestamp"] is not None for channel in (1, 2)):
        mains_start = min(files[channel]["first_valid_timestamp"] for channel in (1, 2)
                          if files[channel]["first_valid_timestamp"] is not None)
        mains_end = max(files[channel]["last_valid_timestamp"] for channel in (1, 2)
                        if files[channel]["last_valid_timestamp"] is not None)
        for group, members in coverage_sets.items():
            if not all(channel in files and files[channel]["first_valid_timestamp"] is not None for channel in members):
                continue
            common_start = max(files[channel]["first_valid_timestamp"] for channel in members)
            common_end = min(files[channel]["last_valid_timestamp"] for channel in members)
            common_span = max(0, common_end-common_start)
            for threshold in FIXED_THRESHOLDS:
                generators = [valid_intervals(db, files[channel]["file_id"], threshold) for channel in members]
                connected = intersect_duration(generators)
                coverage_writer.writerow(dict(house=house, group=group, channels=";".join(map(str, members)),
                                               threshold_seconds=threshold,
                                               common_elapsed_window_seconds=common_span,
                                               mains_elapsed_window_seconds=max(0, mains_end-mains_start),
                                               simultaneous_bounded_gap_span_seconds=connected,
                                               fraction_of_common_window=ratio(connected, common_span),
                                               fraction_of_mains_window=ratio(connected, max(0, mains_end-mains_start))))
    return dict(house=house, expected_channels=sorted(expected),
                missing_channel_files=sorted(expected-files.keys()), extra_channel_files=sorted(files.keys()-expected),
                **counts, group_overlap=group_stats,
                grouped_presence_at_channel1=[dict(group=name, channels=list(expected_groups[name]), **values)
                                              for name, values in partial_counts.items()])


def audit(input_dir, output_dir, houses=(1, 2, 3, 4, 5, 6), keep_db=False):
    input_dir, output_dir = Path(input_dir).resolve(), Path(output_dir).resolve()
    if output_dir.exists():
        raise ValueError(f"Refusing existing audit output: {output_dir}")
    if input_dir == output_dir or input_dir in output_dir.parents:
        raise ValueError("Audit output must be outside the raw input directory")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".raw-audit-", dir=output_dir.parent) as temporary:
        staging = Path(temporary) / "result"
        staging.mkdir()
        db = sqlite3.connect(staging / "audit.sqlite3")
        handles = []
        try:
            db.executescript("""PRAGMA journal_mode=DELETE; PRAGMA synchronous=OFF;
                PRAGMA cache_size=-16384; PRAGMA temp_store=FILE;
                CREATE TABLE observations(file_id INTEGER,house INTEGER,channel INTEGER,ts INTEGER,
                    value TEXT,source_line INTEGER,conflicts INTEGER,PRIMARY KEY(file_id,ts)) WITHOUT ROWID;
                CREATE TABLE intervals(file_id INTEGER,delta TEXT,conflict_touched INTEGER,frequency INTEGER,
                    PRIMARY KEY(file_id,delta,conflict_touched)) WITHOUT ROWID;
                CREATE TABLE bad_keys(file_id INTEGER,ts INTEGER,invalid_records INTEGER,
                    PRIMARY KEY(file_id,ts)) WITHOUT ROWID;
                CREATE TABLE supported_spans(file_id INTEGER,threshold INTEGER,seq INTEGER,start INTEGER,end INTEGER,
                    PRIMARY KEY(file_id,threshold,seq)) WITHOUT ROWID;""")
            handle, mask = csv_output(staging / "record_masks.csv.gz",
                ["file_id", "source_line", "timestamp", "parse_valid", "duplicate_status", "out_of_order"], True)
            handles.append(handle)
            handle, issue = csv_output(staging / "issues.csv.gz",
                ["house", "channel", "file_id", "source_line", "timestamp", "kind", "detail"], True)
            handles.append(handle)
            sources, structures = [], []
            for house in houses:
                directory = input_dir / f"house_{house}"
                if not directory.is_dir():
                    structures.append(dict(house=house, issue="missing_house_directory"))
                    continue
                for path in sorted(directory.glob("channel_*.dat")):
                    match = re.fullmatch(r"channel_([1-9][0-9]*)\.dat", path.name)
                    if not match or not path.is_file():
                        structures.append(dict(house=house, issue="noncanonical_channel_file", path=path.name))
                        continue
                    channel = int(match[1])
                    with db:
                        metadata = source_audit(db, path, len(sources)+1, house, channel, mask, issue)
                        make_intervals(db, metadata)
                    sources.append(metadata)
                    print(json.dumps(dict(event="channel_complete", house=house, channel=channel,
                                          rows=metadata["source_rows"], malformed=metadata["malformed_rows"],
                                          conflicts=metadata["conflicting_with_first_duplicates"])), flush=True)
            for handle in handles:
                handle.close()
            handles.clear()
            db.execute("CREATE INDEX house_timestamp ON observations(house,ts,channel)")
            db.commit()
            handle, histogram = csv_output(staging / "interval_histogram.csv",
                ["file_id", "house", "channel", "interval_seconds", "conflict_touched", "frequency"])
            handles.append(handle)
            handle, spans = csv_output(staging / "threshold_spans.csv",
                ["file_id", "house", "channel", "threshold_name", "threshold_seconds", "elapsed_span_seconds",
                 "threshold_connected_seconds", "long_interval_seconds", "long_interval_count",
                 "conflict_touched_interval_seconds", "unambiguous_connected_seconds",
                 "connected_fraction_of_elapsed_span", "unambiguous_connected_fraction"])
            handles.append(handle)
            for metadata in sources:
                for delta, touched, frequency in db.execute(
                        "SELECT delta,conflict_touched,frequency FROM intervals WHERE file_id=? "
                        "ORDER BY LENGTH(delta),delta,conflict_touched", (metadata["file_id"],)):
                    histogram.writerow(dict(file_id=metadata["file_id"], house=metadata["house"],
                        channel=metadata["channel"], interval_seconds=delta, conflict_touched=touched, frequency=frequency))
                span_statistics(db, metadata, spans)
            handle, row_masks = csv_output(staging / "timestamp_masks.csv.gz",
                ["house", "timestamp", "observed_channels", "unambiguous_channels",
                 "unambiguous_mains", "unambiguous_complete", "legacy_any_member_candidate",
                 "legacy_candidate_missing_constituents", "partial_groups"], True)
            handles.append(handle)
            handle, coverage = csv_output(staging / "simultaneous_spans.csv",
                ["house", "group", "channels", "threshold_seconds", "common_elapsed_window_seconds",
                 "mains_elapsed_window_seconds", "simultaneous_bounded_gap_span_seconds",
                 "fraction_of_common_window", "fraction_of_mains_window"])
            handles.append(handle)
            house_summaries = [house_statistics(db, house, sources, row_masks, coverage) for house in houses]
            handle, ambiguous = csv_output(staging / "ambiguous_timestamps.csv.gz",
                ["file_id", "house", "channel", "timestamp", "first_source_line", "conflicting_records", "invalid_records"], True)
            handles.append(handle)
            for file_id, house, channel, timestamp, line, conflicts, invalid in db.execute(
                    "SELECT o.file_id,o.house,o.channel,o.ts,o.source_line,o.conflicts,COALESCE(b.invalid_records,0) "
                    "FROM observations o LEFT JOIN bad_keys b ON o.file_id=b.file_id AND o.ts=b.ts "
                    "WHERE o.conflicts>0 OR b.ts IS NOT NULL"):
                ambiguous.writerow(dict(file_id=file_id, house=house, channel=channel, timestamp=timestamp,
                                        first_source_line=line, conflicting_records=conflicts, invalid_records=invalid))
        finally:
            for handle in handles:
                handle.close()
            db.close()
        if not keep_db:
            (staging / "audit.sqlite3").unlink()
        source_handle, source_writer = csv_output(staging / "source_files.csv", list(sources[0]) if sources else ["file_id"])
        with source_handle:
            source_writer.writerows(sources)
        summary = dict(
            contract_version=1, audit_kind="raw_source_quality_diagnostic_not_strict_import",
            input_directory=str(input_dir), houses_requested=list(houses), sources=sources,
            house_summary=house_summaries, structural_issues=structures,
            code_sha256=digest_file(Path(__file__)), importer_helper_sha256=digest_file(IMPORTER_PATH),
            thresholds=dict(primary_seconds=60, fixed_seconds=list(FIXED_THRESHOLDS),
                            modal_multipliers=[str(value) for value in CADENCE_MULTIPLIERS]),
            definitions={
                "source_rows": "All physical lines, including malformed/blank lines; valid_rows+malformed_rows.",
                "duplicates": "Compared numerically against the first valid record at that file/timestamp; no values are added or repaired.",
                "conflict_mask": "Every timestamp with any conflicting or timestamped-invalid record is ambiguous; all observations at that key are excluded from primary group-validity and simultaneous-span diagnostics. Bad-only timestamps are listed in record_masks/SQLite bad_keys; ambiguous_timestamps lists keys also having valid observations.",
                "mode": "Mode of positive deltas between sorted unique parsed timestamps; ties select the smallest delta. Descriptive, not a certified acquisition cadence.",
                "elapsed_span": "Last minus first unique parsed timestamp. No extra endpoint sample duration is assumed.",
                "threshold_connected_span": "Sum of consecutive sorted-unique timestamp deltas <=T; longer deltas excluded entirely. Connected+long_interval_seconds equals elapsed span.",
                "unambiguous_connected_span": "Same, excluding intervals with a conflicting or timestamped-invalid key at either endpoint or between the endpoints; known invalid/conflicting keys never bridged. CSV conflict_touched fields include timestamped-invalid records.",
                "simultaneous_span": "Intersection of per-channel unambiguous unions of half-open bounded-gap spans [t_i,t_(i+1)) for delta<=T. This is not measured continuous uptime.",
                "mains_window": "Minimum first to maximum last timestamp across the two mains channels; common window is maximum first to minimum last across required channels.",
                "densities": "Ratios of source timestamps or bounded-gap spans, not raw sensor-loss/error rates, physical synchronization/clock offsets, energy, or NILM accuracy.",
                "mains_preservation": "Raw files are read only; no imputation, alignment repair, submeter-derived MAIN reconstruction, or physical measurand assertion occurs.",
            },
            outputs={path.name: dict(sha256=digest_file(path), size_bytes=path.stat().st_size)
                     for path in staging.iterdir() if path.is_file() and path.name != "audit.sqlite3"},
        )
        (staging / "summary.json").write_text(json.dumps(summary, indent=2)+"\n")
        if output_dir.exists():
            raise ValueError(f"Audit output appeared during run: {output_dir}")
        staging.rename(output_dir)
    return output_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--houses", nargs="+", type=int, choices=range(1, 7), default=list(range(1, 7)))
    parser.add_argument("--keep-db", action="store_true")
    args = parser.parse_args()
    destination = audit(args.input_dir, args.output_dir, tuple(dict.fromkeys(args.houses)), args.keep_db)
    print(json.dumps(dict(event="audit_complete", output_directory=str(destination))), flush=True)


if __name__ == "__main__":
    main()
