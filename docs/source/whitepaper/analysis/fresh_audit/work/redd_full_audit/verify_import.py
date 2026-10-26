#!/usr/bin/env python3
"""Independently check numerical preservation of every REDD mains source row.

This checker does not import the importer or trust its values. It reads source
channels 1/2, compares original records with channels.csv in per-channel source
order (including duplicate rows and source line numbers), and independently
checks MAIN_1, MAIN_2 and MAIN in every loads.csv row. A disk SQLite index sorts
the source timestamps and deduplicates equivalent values for this comparison.
No source, export, or repository file is modified.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import csv
import hashlib
import itertools
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time


HERE = Path(__file__).resolve().parent
DEFAULT_INPUT = Path('/Volumes/bigass/Stephen/Research/Datasets/REDD/low_freq')
MAX_EXAMPLES = 12


def numeric_key(value):
    """Exact numeric identity, without Decimal.normalize's context rounding."""
    sign, digits, exponent = value.as_tuple()
    if not any(digits):
        return '0'
    digits = list(digits)
    while digits[-1] == 0:
        digits.pop()
        exponent += 1
    return f'{sign}:{"".join(map(str, digits))}:{exponent}'


def number(text):
    try:
        value = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError(f'not a decimal number: {text!r}') from exc
    if not value.is_finite():
        raise ValueError(f'nonfinite number: {text!r}')
    return value


def raw_records(path, digest=None):
    with path.open('rb') as source:
        for line, raw in enumerate(source, 1):
            if digest is not None:
                digest.update(raw)
            fields = raw.decode('utf-8-sig' if line == 1 else 'utf-8').split()
            if len(fields) != 2:
                raise ValueError(f'{path}:{line}: expected two fields')
            timestamp = int(fields[0])
            yield line, timestamp, number(fields[1])


class HashedCSVLines:
    """Hash original CSV bytes while the csv module consumes decoded lines."""
    def __init__(self, path):
        self.source = path.open('rb')
        self.digest = hashlib.sha256()
        self.bytes = 0

    def __iter__(self):
        return self

    def __next__(self):
        raw = next(self.source)
        self.digest.update(raw)
        self.bytes += len(raw)
        return raw.decode('utf-8')

    def close(self):
        self.source.close()


def add_record_digest(digest, *fields):
    digest.update(('\t'.join(map(str, fields)) + '\n').encode('ascii'))


def canonical_rows(connection):
    rows = connection.execute('SELECT timestamp, channel, value FROM mains ORDER BY timestamp, channel')
    for timestamp, records in itertools.groupby(rows, key=lambda r: r[0]):
        yield timestamp, {channel: number(value) for _, channel, value in records}


def write_json(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=path.parent,
                                     prefix=path.name + '.', suffix='.tmp', delete=False) as handle:
        tmp = Path(handle.name)
        json.dump(content, handle, indent=2)
        handle.write('\n')
    tmp.replace(path)


def verify_house(house, input_dir, output_dir, archive_provenance, temporary_parent):
    started = time.monotonic()
    source_dir = input_dir / f'house_{house}'
    export_dir = output_dir / f'house_{house}'
    manifest_path = export_dir / 'manifest.json'
    if not manifest_path.is_file():
        raise ValueError(f'house_{house} is not published: {manifest_path}')
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    result = {
        'house': house,
        'scope': 'Every mains source record, all derived mains fields, and importer source hashes.',
        'export_manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(),
        'importer_recorded_source_sha256': manifest.get('importer', {}).get('source_sha256'),
        'mismatch_count': 0,
        'mismatch_examples': [],
        'sources': {},
    }

    def mismatch(detail):
        result['mismatch_count'] += 1
        if len(result['mismatch_examples']) < MAX_EXAMPLES:
            result['mismatch_examples'].append(detail)

    if manifest.get('house_id') != house:
        mismatch('Manifest house_id differs from selected house.')
    expected_archive = {r['archive_member'].removeprefix('low_freq/'): r
                        for r in archive_provenance['canonical_files'] if r['house'] == house}
    manifest_sources = manifest.get('source_files', [])
    manifest_source_paths = [r['path'] for r in manifest_sources]
    wanted_channels = {p for p in expected_archive if '/channel_' in p}
    if len(manifest_source_paths) != len(set(manifest_source_paths)) or set(manifest_source_paths) != wanted_channels:
        mismatch('Manifest source channel inventory differs from verified archive inventory.')
    manifest_source_by_path = {r['path']: r for r in manifest_sources}
    matching_source_hashes = 0
    for path, item in manifest_source_by_path.items():
        prior = expected_archive.get(path)
        if prior is not None and item.get('sha256') == prior['archive_sha256'] and item.get('size_bytes') == prior['archive_size_bytes']:
            matching_source_hashes += 1
        else:
            mismatch(f'Manifest input hash/size differs from archived bytes: {path}')
    label = manifest.get('labels')
    expected_label = expected_archive.get(f'house_{house}/labels.dat')
    labels_match = bool(label and expected_label and label.get('sha256') == expected_label['archive_sha256'])
    if not labels_match:
        mismatch('Manifest labels hash differs from archived labels.dat.')
    result['input_manifest_hashes_vs_verified_archive'] = {
        'channel_files': len(wanted_channels),
        'matching_channel_hashes_and_sizes': matching_source_hashes,
        'label_hash_matches': labels_match,
    }

    with tempfile.TemporaryDirectory(prefix=f'verify-house-{house}-', dir=temporary_parent) as temporary:
        connection = sqlite3.connect(Path(temporary) / 'mains.sqlite3')
        connection.execute('PRAGMA journal_mode=OFF')
        connection.execute('PRAGMA synchronous=OFF')
        connection.execute('PRAGMA cache_size=-16384')
        connection.execute('PRAGMA temp_store=FILE')
        connection.execute('CREATE TABLE mains (timestamp INTEGER, channel INTEGER, value TEXT, '
                           'PRIMARY KEY(timestamp, channel)) WITHOUT ROWID')
        source_record_hashes = {}
        max_adjusted = min_exponent = 0
        try:
            for channel in (1, 2):
                path = source_dir / f'channel_{channel}.dat'
                byte_digest, semantic_digest = hashlib.sha256(), hashlib.sha256()
                count = duplicates = conflicting_duplicates = unordered = 0
                previous = None
                with connection:
                    for line, timestamp, value in raw_records(path, byte_digest):
                        count += 1
                        max_adjusted = max(max_adjusted, value.adjusted())
                        min_exponent = min(min_exponent, value.as_tuple().exponent)
                        unordered += previous is not None and timestamp < previous
                        previous = timestamp
                        add_record_digest(semantic_digest, line, timestamp, numeric_key(value))
                        cursor = connection.execute('INSERT INTO mains VALUES (?, ?, ?) '
                                                    'ON CONFLICT(timestamp, channel) DO NOTHING',
                                                    (timestamp, channel, str(value)))
                        if cursor.rowcount == 0:
                            prior, = connection.execute('SELECT value FROM mains WHERE timestamp=? AND channel=?',
                                                        (timestamp, channel)).fetchone()
                            if number(prior) == value:
                                duplicates += 1
                            else:
                                conflicting_duplicates += 1
                                mismatch(f'Conflicting raw mains duplicate, channel {channel}, timestamp {timestamp}.')
                archive_hash = expected_archive[f'house_{house}/channel_{channel}.dat']['archive_sha256']
                source_meta = manifest_source_by_path.get(f'house_{house}/channel_{channel}.dat', {})
                source_hash_matches = byte_digest.hexdigest() == archive_hash == source_meta.get('sha256')
                if not source_hash_matches:
                    mismatch(f'Reread source channel {channel} hash differs from archive/import manifest.')
                source_record_hashes[channel] = semantic_digest.hexdigest()
                result['sources'][str(channel)] = {
                    'source_rows': count, 'unique_timestamps': count - duplicates - conflicting_duplicates,
                    'identical_duplicate_rows': duplicates, 'conflicting_duplicate_rows': conflicting_duplicates,
                    'out_of_order_rows': unordered, 'source_sha256': byte_digest.hexdigest(),
                    'source_hash_matches_archive_and_import_manifest': source_hash_matches,
                    'record_semantic_sha256': semantic_digest.hexdigest(),
                }
                print(f'house_{house} channel_{channel}: indexed {count:,} original rows ({duplicates:,} equal duplicates).', flush=True)

            # All channels.csv rows are scanned so extra mains rows cannot hide later.
            expected_iters = {c: iter(raw_records(source_dir / f'channel_{c}.dat')) for c in (1, 2)}
            exported_counts = Counter()
            exported_hashes = {c: hashlib.sha256() for c in (1, 2)}
            channel_lines = HashedCSVLines(export_dir / 'channels.csv')
            try:
                rows = csv.DictReader(channel_lines)
                if rows.fieldnames != ['TimeStamp', 'Channel', 'Value', 'SourceLine']:
                    raise ValueError(f'Unexpected channels.csv schema: {rows.fieldnames!r}')
                all_channel_rows = 0
                for row in rows:
                    all_channel_rows += 1
                    channel = int(row['Channel'])
                    if channel not in (1, 2):
                        continue
                    line, timestamp, value = int(row['SourceLine']), int(row['TimeStamp']), number(row['Value'])
                    exported_counts[channel] += 1
                    add_record_digest(exported_hashes[channel], line, timestamp, numeric_key(value))
                    expected = next(expected_iters[channel], None)
                    if expected is None or (line, timestamp, value) != expected:
                        mismatch(f'channels.csv mains record differs, channel {channel}, exported row {exported_counts[channel]}.')
            finally:
                channel_lines.close()
                for iterator in expected_iters.values():
                    iterator.close()
            for channel in (1, 2):
                expected_count = result['sources'][str(channel)]['source_rows']
                if exported_counts[channel] != expected_count:
                    mismatch(f'channels.csv channel {channel} count {exported_counts[channel]} != original {expected_count}.')
                semantic_match = exported_hashes[channel].hexdigest() == source_record_hashes[channel]
                if not semantic_match:
                    mismatch(f'channels.csv channel {channel} record digest differs from source digest.')
                result['sources'][str(channel)].update({
                    'channels_csv_rows': exported_counts[channel],
                    'channels_csv_record_semantic_sha256': exported_hashes[channel].hexdigest(),
                    'channels_csv_all_records_and_source_lines_match': semantic_match,
                })
            print(f'house_{house}: scanned all {all_channel_rows:,} channels.csv rows; original mains records compared.', flush=True)

            expected = iter(canonical_rows(connection))
            next_expected = next(expected, None)
            loaded_main_leg_counts = Counter()
            expected_leg_hashes = {c: hashlib.sha256() for c in (1, 2)}
            loaded_leg_hashes = {c: hashlib.sha256() for c in (1, 2)}
            counts = Counter()
            previous = None
            load_lines = HashedCSVLines(export_dir / 'loads.csv')
            try:
                reader = csv.DictReader(load_lines)
                if not {'TimeStamp', 'MAIN_1', 'MAIN_2', 'MAIN', 'DIFF'}.issubset(reader.fieldnames or []):
                    raise ValueError('loads.csv lacks required original/derived mains columns.')
                with localcontext() as context:
                    context.prec = max(50, max_adjusted - min_exponent + 10)
                    for row in reader:
                        timestamp = int(row['TimeStamp'])
                        counts['loads_rows'] += 1
                        if previous is not None and timestamp <= previous:
                            mismatch(f'loads.csv timestamps not strictly increasing at {timestamp}.')
                        previous = timestamp
                        while next_expected is not None and next_expected[0] < timestamp:
                            mismatch(f'Original mains timestamp missing from loads.csv: {next_expected[0]}.')
                            counts['missing_original_mains_timestamps'] += 1
                            next_expected = next(expected, None)
                        values = next_expected[1] if next_expected is not None and next_expected[0] == timestamp else {}
                        if values:
                            counts['original_mains_union_timestamps'] += 1
                            next_expected = next(expected, None)
                        for channel in (1, 2):
                            text = row[f'MAIN_{channel}']
                            actual = number(text) if text != '' else None
                            wanted = values.get(channel)
                            if actual != wanted:
                                mismatch(f'MAIN_{channel} differs from original source at {timestamp}.')
                            if wanted is not None:
                                add_record_digest(expected_leg_hashes[channel], timestamp, numeric_key(wanted))
                            if actual is not None:
                                loaded_main_leg_counts[channel] += 1
                                add_record_digest(loaded_leg_hashes[channel], timestamp, numeric_key(actual))
                        wanted_main = values[1] + values[2] if len(values) == 2 else None
                        actual_main = number(row['MAIN']) if row['MAIN'] != '' else None
                        if actual_main != wanted_main:
                            mismatch(f'MAIN is not the original legs exact sum/blank at {timestamp}.')
                        counts[f'rows_with_{len(values)}_original_mains_legs'] += 1
                        if len(values) == 2:
                            counts['exact_original_leg_sums_checked'] += 1
                        if row['DIFF'] != '' and number(row['DIFF']) < 0:
                            counts['negative_residual_rows_with_original_mains_checked'] += 1
            finally:
                load_lines.close()
            while next_expected is not None:
                mismatch(f'Original mains timestamp missing at end of loads.csv: {next_expected[0]}.')
                counts['missing_original_mains_timestamps'] += 1
                next_expected = next(expected, None)
            for channel in (1, 2):
                wanted_count = result['sources'][str(channel)]['unique_timestamps']
                canonical_match = (loaded_leg_hashes[channel].hexdigest() == expected_leg_hashes[channel].hexdigest()
                                   and loaded_main_leg_counts[channel] == wanted_count)
                if not canonical_match:
                    mismatch(f'Canonical MAIN_{channel} count/numeric digest differs from original source.')
                result['sources'][str(channel)].update({
                    'canonical_loads_leg_rows': loaded_main_leg_counts[channel],
                    'expected_canonical_numeric_sha256': expected_leg_hashes[channel].hexdigest(),
                    'loads_canonical_numeric_sha256': loaded_leg_hashes[channel].hexdigest(),
                    'all_canonical_leg_readings_match': canonical_match,
                })
            result['counts'] = dict(counts)
            result['all_channels_csv_rows_scanned'] = all_channel_rows
            result['output_hash_checks'] = {}
            for name, lines in (('channels.csv', channel_lines), ('loads.csv', load_lines)):
                expected_hash = manifest.get('outputs', {}).get(name, {}).get('sha256')
                matches = lines.digest.hexdigest() == expected_hash
                if not matches:
                    mismatch(f'{name} bytes differ from importer manifest output hash.')
                result['output_hash_checks'][name] = {
                    'sha256': lines.digest.hexdigest(), 'size_bytes': lines.bytes,
                    'matches_import_manifest': matches,
                }
            print(f"house_{house}: checked {counts['loads_rows']:,} loads rows and {counts['exact_original_leg_sums_checked']:,} exact original leg sums.", flush=True)
        finally:
            connection.close()
    result['verified'] = result['mismatch_count'] == 0
    result['elapsed_seconds'] = round(time.monotonic() - started, 3)
    result['finished_utc'] = datetime.now(timezone.utc).isoformat()
    print(f"house_{house}: {'PASS' if result['verified'] else 'FAIL'} ({result['mismatch_count']} mismatches).", flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', type=Path, default=DEFAULT_INPUT)
    parser.add_argument('--output-dir', type=Path, default=HERE / 'imported')
    parser.add_argument('--archive-provenance', type=Path, default=HERE / 'provenance.json')
    parser.add_argument('--result', type=Path, default=HERE / 'import_verification.json')
    parser.add_argument('--houses', type=int, nargs='+', choices=range(1, 7), default=list(range(1, 7)))
    args = parser.parse_args()
    provenance = json.loads(args.archive_provenance.read_text())
    if provenance.get('verification_passed') is not True:
        parser.error('Archive-to-local provenance verification must first pass.')
    result = json.loads(args.result.read_text()) if args.result.exists() else {
        'schema_version': 1,
        'scope': 'Actual-data numerical preservation check of all original REDD mains readings.',
        'record_digest_definition': 'SHA-256 over tab-separated source line/timestamp/exact normalized decimal identity; source line omitted for canonical loads digests.',
        'comparison_arithmetic': 'Decimal, with precision sufficient for all source mains exponent ranges.',
        'source_data_redistributed': False,
        'houses': {},
    }
    result['archive_sha256'] = provenance['archive']['sha256']
    result['archive_provenance_sha256'] = hashlib.sha256(args.archive_provenance.read_bytes()).hexdigest()
    result['validator_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    for house in args.houses:
        result['houses'][str(house)] = verify_house(house, args.input_dir, args.output_dir,
                                                   provenance, args.result.parent)
        result['all_verified_houses_passed'] = all(v['verified'] for v in result['houses'].values())
        result['all_six_houses_verified'] = set(result['houses']) == set(map(str, range(1, 7)))
        write_json(args.result, result)
    print(f'Results saved to {args.result}', flush=True)
    return 0 if result['all_verified_houses_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
