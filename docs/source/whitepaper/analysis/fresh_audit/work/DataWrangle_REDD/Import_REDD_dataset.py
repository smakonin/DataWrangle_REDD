#!/usr/bin/env python3
"""Import low-frequency REDD without rewriting independently measured mains.

Python 3.10+, standard library only. See README.md for output schemas, native
cadence, grouping caveats, and the deliberate break with historical repairs.
"""
# Copyright (C) 2013 Stephen Makonin. All Right Reserved.
# SPDX-License-Identifier: MPL-2.0

from __future__ import annotations

import argparse
import csv
import hashlib
import heapq
import itertools
import json
import platform
import re
import sqlite3
import sys
import tempfile
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path

VERSION = "2.0.0"
MAX_INTERVAL_BINS = 256

# Historical categories are retained for comparability, not certified appliance
# identities. Every original observation and channel ID is exported separately.
_LOAD_IDS = {
    1: ["MAIN", "OVEN", "REFG", "DISH", "KTCH", "LITE", "DRYR", "MICR", "BATH", "HEAT", "STOV", "DIFF"],
    2: ["MAIN", "KTCH", "LITE", "STOV", "MICR", "DRYR", "REFG", "DISH", "GARB", "DIFF"],
    3: ["MAIN", "UNKN", "LITE", "ELEC", "REFG", "GARB", "DISH", "FURN", "DRYR", "MICR", "SMOK", "BATH", "KTCH", "DIFF"],
    4: ["MAIN", "LITE", "FURN", "KTCH", "UNKN", "DRYR", "STOV", "AIRC", "MISC", "SMOK", "DISH", "BATH", "DIFF"],
    5: ["MAIN", "MICR", "LITE", "UNKN", "FURN", "DRYR", "SUBP", "HEAT", "BATH", "REFG", "DISH", "GARB", "ELEC", "KTCH", "OUTD", "DIFF"],
    6: ["MAIN", "KTCH", "DRYR", "STOV", "ELEC", "BATH", "REFG", "DISH", "UNKN", "HEAT", "LITE", "AIRC", "DIFF"],
}
_CHANNEL_INDEX = {
    1: [0, 0, 1, 1, 2, 3, 4, 4, 5, 6, 7, 8, 9, 10, 4, 4, 5, 5, 6, 6],
    2: [0, 0, 1, 2, 3, 4, 5, 1, 6, 7, 8],
    3: [0, 0, 1, 1, 2, 3, 4, 5, 6, 7, 2, 1, 8, 8, 2, 9, 2, 10, 2, 11, 12, 12],
    4: [0, 0, 1, 2, 3, 4, 5, 6, 7, 7, 8, 9, 2, 3, 10, 11, 11, 2, 2, 7],
    5: [0, 0, 1, 2, 3, 4, 3, 5, 5, 6, 6, 7, 7, 2, 3, 8, 2, 9, 2, 10, 11, 12, 2, 13, 13, 14],
    6: [0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 8, 9, 1, 10, 11, 11, 11],
}
HOUSE_CHANNEL_GROUPS = {
    house: {
        name: tuple(channel for channel, group in enumerate(_CHANNEL_INDEX[house], 1)
                    if group == index)
        for index, name in enumerate(names[:-1])
    }
    for house, names in _LOAD_IDS.items()
}
MAPPING_NOTES = {
    1: ["Historical DRYR includes channel 19, whose identity is disputed by "
        "NILMTK metadata. Inspect original channels before appliance scoring."],
    5: ["SUBP pools subpanel channels 10 and 11. Verify circuit topology and "
        "possible overlap before interpreting a balance residual."],
    6: ["AIRC pools channels 15, 16 and 17; NILMTK distinguishes an inferred "
        "air handler from the air-conditioning channels. This is an HVAC group."],
}
QUALITY_FIELDS = [
    "TimeStamp", "observed_channels", "missing_channels", "mains_complete",
    "targets_complete", "row_complete", "negative_residual", "seconds_since_previous_row",
]
ISSUE_FIELDS = ["kind", "channel", "timestamp", "source_line", "detail"]
_NUMBER = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\Z")
_TIMESTAMP = re.compile(r"[+-]?[0-9]+\Z")


class REDDImportError(ValueError):
    """An input or output contract failed; no partial house is published."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _parse_record(raw: bytes, path: Path, line: int) -> tuple[int, Decimal]:
    location = f"{path}:{line}"
    try:
        fields = raw.decode("utf-8-sig" if line == 1 else "utf-8").split()
    except UnicodeDecodeError as exc:
        raise REDDImportError(f"{location}: record is not UTF-8 text") from exc
    if len(fields) != 2:
        raise REDDImportError(f"{location}: expected timestamp and measurement, got {len(fields)} fields")
    if not _TIMESTAMP.fullmatch(fields[0]):
        raise REDDImportError(f"{location}: timestamp must be an integer")
    timestamp = int(fields[0])
    if not -(2**63) <= timestamp < 2**63:
        raise REDDImportError(f"{location}: timestamp is outside the supported 64-bit range")
    if not _NUMBER.fullmatch(fields[1]):
        raise REDDImportError(f"{location}: measurement must be a finite decimal number")
    try:
        value = Decimal(fields[1])
    except InvalidOperation as exc:
        raise REDDImportError(f"{location}: invalid measurement") from exc
    if not value.is_finite():
        raise REDDImportError(f"{location}: measurement must be finite")
    return timestamp, value


def _validate_channels(house_dir: Path, expected: set[int]) -> None:
    if not house_dir.is_dir():
        raise REDDImportError(f"House directory does not exist: {house_dir}")
    found = set()
    for path in house_dir.glob("channel_*.dat"):
        match = re.fullmatch(r"channel_([1-9][0-9]*)\.dat", path.name)
        if match is None or not path.is_file():
            raise REDDImportError(f"Unrecognized channel file: {path}")
        channel = int(match[1])
        if path.name != f"channel_{channel}.dat":
            raise REDDImportError(f"Noncanonical channel filename: {path}")
        found.add(channel)
    missing, extra = sorted(expected - found), sorted(found - expected)
    if missing or extra:
        raise REDDImportError(f"{house_dir}: missing channels {missing}; unexpected channels {extra}")


def _read_labels(path: Path, expected: set[int]) -> dict | None:
    if not path.exists():
        return None
    raw = path.read_bytes()
    labels = {}
    try:
        for line, text in enumerate(raw.decode("utf-8-sig").splitlines(), 1):
            fields = text.split(maxsplit=1)
            if len(fields) != 2 or not re.fullmatch(r"[1-9][0-9]*", fields[0]):
                raise REDDImportError(f"{path}:{line}: expected channel number and label")
            channel = int(fields[0])
            if channel in labels:
                raise REDDImportError(f"{path}:{line}: repeated channel label {channel}")
            labels[channel] = fields[1]
    except UnicodeDecodeError as exc:
        raise REDDImportError(f"{path}: labels are not UTF-8 text") from exc
    if set(labels) != expected:
        raise REDDImportError(f"{path}: labels do not match the expected channel IDs")
    return {"path": f"{path.parent.name}/{path.name}",
            "sha256": hashlib.sha256(raw).hexdigest(), "names": labels}


def _interval_statistics(connection: sqlite3.Connection, channel: int) -> dict:
    intervals = Counter()
    previous = first = last = None
    count = omitted = 0
    minimum = maximum = None
    for (timestamp,) in connection.execute(
            "SELECT timestamp FROM observations WHERE channel = ? ORDER BY timestamp", (channel,)):
        if previous is not None:
            delta = timestamp - previous
            minimum = delta if minimum is None else min(minimum, delta)
            maximum = delta if maximum is None else max(maximum, delta)
            if delta in intervals or len(intervals) < MAX_INTERVAL_BINS:
                intervals[delta] += 1
            else:
                omitted += 1
        else:
            first = timestamp
        previous = last = timestamp
        count += 1
    return {
        "unique_observations": count, "first_timestamp": first, "last_timestamp": last,
        "sample_interval_seconds": {
            "basis": "sorted unique observations, not an estimate of sensor uptime",
            "histogram": {str(delta): intervals[delta] for delta in sorted(intervals)},
            "minimum": minimum, "maximum": maximum, "interval_count": max(0, count - 1),
            "histogram_limit": MAX_INTERVAL_BINS, "histogram_complete": omitted == 0,
            "omitted_intervals": omitted,
            "histogram_selection": "first distinct intervals encountered in sorted timestamps; "
                                   "counts for retained bins cover the full channel",
        },
    }


def _ingest_channel(connection, path, channel, raw_writer, issue_writer) -> dict:
    digest = hashlib.sha256()
    size = rows = duplicates = unordered = issues = 0
    previous = None
    minimum_exponent = maximum_adjusted = 0
    with path.open("rb") as source:
        for line, raw in enumerate(source, 1):
            digest.update(raw)
            size += len(raw)
            timestamp, value = _parse_record(raw, path, line)
            raw_writer.writerow([timestamp, channel, str(value), line])
            rows += 1
            minimum_exponent = min(minimum_exponent, value.as_tuple().exponent)
            maximum_adjusted = max(maximum_adjusted, value.adjusted())
            if previous is not None and timestamp < previous:
                issue_writer.writerow(["out_of_order", channel, timestamp, line,
                                       f"Previous source timestamp was {previous}; canonical output is sorted"])
                unordered += 1
                issues += 1
            previous = timestamp
            inserted = connection.execute(
                "INSERT INTO observations(channel, timestamp, value, source_line) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(channel, timestamp) DO NOTHING",
                (channel, timestamp, str(value), line))
            if inserted.rowcount == 0:
                prior_value, prior_line = connection.execute(
                    "SELECT value, source_line FROM observations WHERE channel = ? AND timestamp = ?",
                    (channel, timestamp)).fetchone()
                if Decimal(prior_value) != value:
                    raise REDDImportError(
                        f"{path}:{line}: conflicting duplicate timestamp {timestamp}; "
                        f"first value {prior_value} at line {prior_line}, later value {value}")
                duplicates += 1
                issues += 1
                issue_writer.writerow(["identical_duplicate", channel, timestamp, line,
                                       f"Same value as source line {prior_line}; retained in channels.csv, "
                                       "counted once in loads.csv"])
    if rows == 0:
        raise REDDImportError(f"{path}: channel file contains no observations")
    return {
        "channel": channel, "path": f"{path.parent.name}/{path.name}",
        "sha256": digest.hexdigest(), "size_bytes": size, "source_rows": rows,
        "identical_duplicates": duplicates, "out_of_order_records": unordered,
        "_minimum_exponent": minimum_exponent, "_maximum_adjusted": maximum_adjusted,
        "_issue_rows": issues,
    }


def _channel_rows(connection, channel):
    for timestamp, value in connection.execute(
            "SELECT timestamp, value FROM observations WHERE channel = ? ORDER BY timestamp", (channel,)):
        yield timestamp, channel, Decimal(value)


def _write_loads(connection, staging, groups, precision, maximum_adjusted, minimum_exponent):
    expected = {channel for members in groups.values() for channel in members}
    counts = dict.fromkeys([
        "timestamp_rows", "complete_rows", "mains_complete_rows",
        "targets_complete_rows", "negative_residual_rows",
    ], 0)
    first = previous = last = None
    with (staging / "loads.csv").open("w", encoding="utf-8", newline="") as loads, (
            staging / "quality.csv").open("w", encoding="utf-8", newline="") as quality:
        load_writer, quality_writer = csv.writer(loads), csv.writer(quality)
        load_writer.writerow(["TimeStamp", *groups, "DIFF", "MAIN_1", "MAIN_2"])
        quality_writer.writerow(QUALITY_FIELDS)
        # Cover the observed decimal exponent range and the carry from summing
        # every channel. Source measurements themselves are never rounded.
        with localcontext() as context:
            context.prec = precision
            context.Emax = max(context.Emax, maximum_adjusted + len(str(len(expected))) + 4)
            context.Emin = min(context.Emin, minimum_exponent)
            ordered = heapq.merge(*(_channel_rows(connection, channel)
                                   for channel in sorted(expected)))
            for timestamp, rows in itertools.groupby(ordered, key=lambda row: row[0]):
                values = {row[1]: row[2] for row in rows}
                grouped = {
                    name: sum((values[channel] for channel in members), Decimal(0))
                    if all(channel in values for channel in members) else None
                    for name, members in groups.items()
                }
                mains_complete = grouped["MAIN"] is not None
                targets_complete = all(value is not None for name, value in grouped.items() if name != "MAIN")
                complete = mains_complete and targets_complete
                residual = (grouped["MAIN"] - sum((value for name, value in grouped.items()
                                                  if name != "MAIN"), Decimal(0))
                            if complete else None)
                negative = residual < 0 if residual is not None else None
                load_writer.writerow([timestamp, *grouped.values(), residual, values.get(1), values.get(2)])
                quality_writer.writerow([
                    timestamp, ";".join(map(str, sorted(values))),
                    ";".join(map(str, sorted(expected - values.keys()))),
                    int(mains_complete), int(targets_complete), int(complete),
                    int(negative) if negative is not None else "",
                    timestamp - previous if previous is not None else "",
                ])
                counts["timestamp_rows"] += 1
                counts["complete_rows"] += int(complete)
                counts["mains_complete_rows"] += int(mains_complete)
                counts["targets_complete_rows"] += int(targets_complete)
                counts["negative_residual_rows"] += int(bool(negative))
                if first is None:
                    first = timestamp
                previous = last = timestamp
    return counts, {
        "first_timestamp": first, "last_timestamp": last,
        "calendar_span_seconds": last - first + 1 if first is not None else 0,
        "note": "Timestamp union at native observation times; calendar span is not continuous uptime. "
                "No unobserved grid positions are invented.",
    }


def import_house(input_dir: Path, output_dir: Path, house_id: int) -> Path:
    """Validate, export, and atomically publish one house; never edit inputs.

    input_dir contains house_1 through house_6. Existing output house directories
    are refused. A failed house leaves no published CSVs; already successful
    houses from a multi-house invocation remain available.
    """
    input_dir, output_dir = Path(input_dir), Path(output_dir)
    if house_id not in HOUSE_CHANNEL_GROUPS:
        raise REDDImportError(f"Unsupported house {house_id}; choose 1 through 6")
    house_dir = input_dir / f"house_{house_id}"
    destination = output_dir / house_dir.name
    if destination.exists() or destination.is_symlink():
        raise REDDImportError(f"Output already exists; choose a new destination: {destination}")
    groups = HOUSE_CHANNEL_GROUPS[house_id]
    expected = {channel for members in groups.values() for channel in members}
    _validate_channels(house_dir, expected)
    label_paths = [house_dir / name for name in ("labels.dat", "labels.txt")
                   if (house_dir / name).exists() or (house_dir / name).is_symlink()]
    if len(label_paths) > 1:
        raise REDDImportError(f"{house_dir}: both labels.dat and labels.txt exist; "
                              "retain one unambiguous label source")
    labels = _read_labels(label_paths[0], expected) if label_paths else None
    output_dir.mkdir(parents=True, exist_ok=True)

    # SQLite staging stays on disk; no list is allocated per calendar second.
    # Only complete, validated exports enter the public house folder.
    with tempfile.TemporaryDirectory(prefix=f".redd-house-{house_id}-", dir=output_dir) as temporary:
        working = Path(temporary)
        staging = working / house_dir.name
        staging.mkdir()
        connection = sqlite3.connect(working / "observations.sqlite3")
        try:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=OFF")
            connection.execute("PRAGMA cache_size=-16384")
            connection.execute("PRAGMA temp_store=FILE")
            connection.execute(
                "CREATE TABLE observations (channel INTEGER NOT NULL, timestamp INTEGER NOT NULL, "
                "value TEXT NOT NULL, source_line INTEGER NOT NULL, "
                "PRIMARY KEY(channel, timestamp)) WITHOUT ROWID")
            source_files = []
            with (staging / "channels.csv").open("w", encoding="utf-8", newline="") as raw_file, (
                    staging / "issues.csv").open("w", encoding="utf-8", newline="") as issues_file:
                raw_writer, issue_writer = csv.writer(raw_file), csv.writer(issues_file)
                raw_writer.writerow(["TimeStamp", "Channel", "Value", "SourceLine"])
                issue_writer.writerow(ISSUE_FIELDS)
                with connection:
                    for channel in sorted(expected):
                        metadata = _ingest_channel(
                            connection, house_dir / f"channel_{channel}.dat", channel, raw_writer, issue_writer)
                        source_files.append(metadata)
            for metadata in source_files:
                metadata.update(_interval_statistics(connection, metadata["channel"]))
            minimum_exponent = min(item.pop("_minimum_exponent") for item in source_files)
            maximum_adjusted = max(item.pop("_maximum_adjusted") for item in source_files)
            issue_rows = sum(item.pop("_issue_rows") for item in source_files)
            precision = max(28, maximum_adjusted - minimum_exponent + len(str(len(expected))) + 4)
            counts, coverage = _write_loads(connection, staging, groups, precision,
                                             maximum_adjusted, minimum_exponent)
        finally:
            connection.close()

        counts["source_observations"] = sum(item["source_rows"] for item in source_files)
        counts["identical_duplicate_records"] = sum(item["identical_duplicates"] for item in source_files)
        output_rows = {"channels.csv": counts["source_observations"], "issues.csv": issue_rows,
                       "loads.csv": counts["timestamp_rows"], "quality.csv": counts["timestamp_rows"]}
        manifest = {
            "format_version": 2, "house_id": house_id,
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "importer": {"name": "DataWrangle_REDD", "version": VERSION,
                         "python_version": platform.python_version(), "source_sha256": _sha256(Path(__file__))},
            "policies": {
                "alignment": "exact timestamp union; no resampling or filling",
                "groups": "require every original constituent at the same timestamp",
                "mains": "preserve observed legs; MAIN is their sum only when both are observed",
                "negative_residuals": "retain as diagnostic values; never replace mains",
                "duplicates": "equivalent values counted once in loads; all source rows retained in channels; "
                              "conflicting duplicate values reject the house",
                "ordering": "channels.csv retains per-channel source order; loads.csv and quality.csv are sorted",
                "precision": "decimal arithmetic without per-channel truncation",
            },
            "measurement_caveat": "Power measurands, units, calibration, circuit topology and electrical "
                                  "additivity are not certified by this importer. DIFF is a diagnostic, "
                                  "not verified unknown-load power or energy.",
            "groups": groups, "mapping_notes": MAPPING_NOTES.get(house_id, []),
            "source_files": source_files, "labels": labels, "counts": counts, "coverage": coverage,
            "outputs": {name: {"sha256": _sha256(staging / name), "rows": rows}
                        for name, rows in output_rows.items()},
        }
        (staging / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        if destination.exists() or destination.is_symlink():
            raise REDDImportError(f"Output already exists; choose a new destination: {destination}")
        staging.rename(destination)
    return destination


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path,
                        help="REDD low_freq directory containing house_1 through house_6")
    parser.add_argument("--output-dir", required=True, type=Path,
                        help="Destination root; existing house folders are never overwritten")
    parser.add_argument("--houses", nargs="+", type=int, choices=range(1, 7),
                        default=list(range(1, 7)), help="House IDs to import (default: all six)")
    parser.add_argument("--version", action="version", version=f"DataWrangle_REDD {VERSION}")
    args = parser.parse_args(argv)
    try:
        for house in dict.fromkeys(args.houses):
            destination = import_house(args.input_dir, args.output_dir, house)
            print(f"House {house}: {destination}")
    except (REDDImportError, OSError, sqlite3.Error) as exc:
        print(f"Import failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
