#!/usr/bin/env python3
"""Read-only byte comparison of Stephen's archived REDD release and local data.

No archive member is extracted. Links, special files, unsafe member paths, and
duplicate member paths invalidate verification. Only the six conventional
house directories and their channel/label files are compared with local data.
The archive checksum records this copy's identity; it is not an independently
published official checksum or proof of the original download's authenticity.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tarfile
import tempfile
import time


DEFAULT_ROOT = Path('/Volumes/bigass/Stephen/Research/Datasets/REDD')
EXPECTED_CHANNEL_COUNTS = {1: 20, 2: 11, 3: 22, 4: 20, 5: 26, 6: 17}
CANONICAL = re.compile(r'^low_freq/house_([1-6])/(labels\.dat|channel_([1-9][0-9]*)\.dat)$')
CHUNK_SIZE = 8 * 1024 * 1024


def hash_stream(stream):
    digest = hashlib.sha256()
    size = 0
    while chunk := stream.read(CHUNK_SIZE):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def regular_file_identity(path):
    """Reject symlinks and detect changes during the read without modifying it."""
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode):
        raise ValueError(f'not a regular, non-symlink file: {path}')
    with path.open('rb') as handle:
        opened = os.fstat(handle.fileno())
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise ValueError(f'file changed before opening: {path}')
        digest, size = hash_stream(handle)
        after_fd = os.fstat(handle.fileno())
    after = path.lstat()
    identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns)
    if identity(before) != identity(after_fd) or identity(before) != identity(after):
        raise ValueError(f'file changed while reading: {path}')
    return {
        'path': str(path),
        'size_bytes': size,
        'sha256': digest,
        'filesystem_mtime_utc': datetime.fromtimestamp(before.st_mtime, timezone.utc).isoformat(),
    }


def member_type(member):
    if member.isdir():
        return 'directory'
    if member.isreg():
        return 'regular_file'
    if member.issym():
        return 'symbolic_link'
    if member.islnk():
        return 'hard_link'
    return f'special_{member.type!r}'


def normalized_safe_name(name):
    path = PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts or '\\' in name or '\x00' in name:
        raise ValueError(f'unsafe archive member name: {name!r}')
    normalized = str(path)
    if normalized in ('', '.'):
        raise ValueError(f'empty archive member name: {name!r}')
    return normalized


def local_canonical_inventory(low_freq):
    inventory = {}
    ignored = []
    for item in sorted(low_freq.iterdir(), key=lambda p: p.name):
        if item.name not in {f'house_{i}' for i in range(1, 7)}:
            ignored.append(item.name)
    for house in range(1, 7):
        house_path = low_freq / f'house_{house}'
        if not house_path.is_dir() or house_path.is_symlink():
            raise ValueError(f'expected non-symlink house directory missing: {house_path}')
        for item in sorted(house_path.iterdir(), key=lambda p: p.name):
            name = f'low_freq/house_{house}/{item.name}'
            if CANONICAL.fullmatch(name):
                if not item.is_file() or item.is_symlink():
                    raise ValueError(f'expected non-symlink canonical file: {item}')
                inventory[name] = item
            else:
                ignored.append(f'house_{house}/{item.name}')
    return inventory, ignored


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile('w', encoding='utf-8', dir=path.parent,
                                     prefix=path.name + '.', suffix='.tmp', delete=False) as handle:
        temporary = Path(handle.name)
        json.dump(value, handle, indent=2, ensure_ascii=False)
        handle.write('\n')
    temporary.replace(path)


def verify(dataset_root, output):
    started = time.monotonic()
    manifest = {
        'schema_version': 1,
        'verification_started_utc': datetime.now(timezone.utc).isoformat(),
        'dataset_root': str(dataset_root),
        'verification_scope': 'Archive-to-local byte comparison, not independent download authentication.',
        'read_only_sources': True,
        'extracted_members': 0,
        'hash_algorithm': 'SHA-256',
        'errors': [],
        'archive_members': [],
        'canonical_files': [],
        'houses': {},
        'independent_official_archive_checksum_available': False,
    }
    errors = manifest['errors']
    archive_path = dataset_root / 'low_freq.tar.bz2'
    low_freq = dataset_root / 'low_freq'
    try:
        print('Hashing original compressed archive...', flush=True)
        archive_identity = regular_file_identity(archive_path)
        manifest['archive'] = archive_identity
        print(f"Archive: {archive_identity['size_bytes']:,} bytes; SHA-256 {archive_identity['sha256']}", flush=True)
        local_inventory, ignored = local_canonical_inventory(low_freq)
        manifest['ignored_local_entries'] = ignored
        print(f'Comparing {len(local_inventory)} canonical local files; ignoring {len(ignored)} noncanonical entries.', flush=True)
        manifest['supporting_files'] = {}
        for name in ('readme.txt', 'kolter-kddsust11.pdf'):
            manifest['supporting_files'][name] = regular_file_identity(dataset_root / name)
        readme = (dataset_root / 'readme.txt').read_text(encoding='utf-8')
        version_match = re.search(r'^REDD:.*Version\s+([^\s]+)', readme, re.MULTILINE)
        manifest['readme_evidence'] = {
            'release_version_as_documented': version_match.group(1) if version_match else None,
            'original_site_as_documented': 'http://redd.csail.mit.edu' if 'http://redd.csail.mit.edu' in readme else None,
            'native_cadence_as_documented': 'About once a second for mains and once every three seconds for circuits.',
            'timestamp_format_as_documented': 'UTC timestamps as integers.',
            'measurement_type_as_documented': 'Apparent power of the circuit (README wording; not independently validated).',
            'labels_as_documented': 'General category or main appliance type on the circuit, potentially multiple device types.',
            'citation_requested': 'Kolter and Johnson, SustKDD 2011.',
            'formal_license_identified_in_readme': False,
            'measurement_type_caveat': 'NILMTK assigns apparent power to mains and active power to other channels, unlike this README.',
        }
        seen = set()
        selected = set()
        count = 0
        by_house = Counter()
        with tarfile.open(archive_path, mode='r|bz2') as archive:
            for member in archive:
                entry = {'name': member.name, 'type': member_type(member), 'size_bytes': member.size}
                manifest['archive_members'].append(entry)
                try:
                    name = normalized_safe_name(member.name)
                    entry['normalized_name'] = name
                    if name in seen:
                        raise ValueError(f'duplicate archive member path: {name}')
                    seen.add(name)
                    if not member.isdir() and not member.isreg():
                        raise ValueError(f'archive links/special files rejected: {name} ({entry["type"]})')
                    match = CANONICAL.fullmatch(name)
                    entry['canonical'] = bool(match and member.isreg())
                    if not match or not member.isreg():
                        continue
                    selected.add(name)
                    house = int(match.group(1))
                    record = {'archive_member': name, 'house': house, 'archive_size_bytes': member.size}
                    source = archive.extractfile(member)
                    if source is None:
                        raise ValueError(f'could not read regular archive member: {name}')
                    with source:
                        digest, size = hash_stream(source)
                    record['archive_sha256'] = digest
                    if size != member.size:
                        raise ValueError(f'archive member size mismatch: {name}')
                    local = local_inventory.get(name)
                    if local is None:
                        record['status'] = 'missing_local_file'
                        errors.append(f'archive member absent locally: {name}')
                    else:
                        local_identity = regular_file_identity(local)
                        record['local'] = local_identity
                        record['status'] = ('matched' if local_identity['sha256'] == digest and
                                            local_identity['size_bytes'] == size else 'mismatch')
                        if record['status'] != 'matched':
                            errors.append(f'archive/local bytes mismatch: {name}')
                    manifest['canonical_files'].append(record)
                    count += 1
                    by_house[house] += 1
                    if by_house[house] == 1 or by_house[house] % 10 == 0 or record['status'] != 'matched':
                        print(f"[{count:03d}] house_{house}/{match.group(2)}: {record['status']} ({size:,} bytes)", flush=True)
                except (ValueError, OSError) as exc:
                    entry['error'] = str(exc)
                    errors.append(str(exc))
        archive_after = archive_path.lstat()
        if (archive_after.st_size != archive_identity['size_bytes'] or
            datetime.fromtimestamp(archive_after.st_mtime, timezone.utc).isoformat() != archive_identity['filesystem_mtime_utc']):
            errors.append('archive changed during member verification')
        for extra in sorted(set(local_inventory) - selected):
            errors.append(f'canonical local file absent from archive: {extra}')
        for house in range(1, 7):
            prefix = f'low_freq/house_{house}/'
            members = [r for r in manifest['canonical_files'] if r['house'] == house]
            channel_ids = sorted(int(CANONICAL.fullmatch(r['archive_member']).group(3))
                                 for r in members if '/channel_' in r['archive_member'])
            expected = list(range(1, EXPECTED_CHANNEL_COUNTS[house] + 1))
            labels_present = prefix + 'labels.dat' in selected
            if channel_ids != expected or not labels_present:
                errors.append(f'house_{house}: unexpected canonical channel/label inventory')
            summary = {
                'channel_file_count': len(channel_ids),
                'channel_ids': channel_ids,
                'mains_channels': [1, 2],
                'circuit_channel_count': len(channel_ids) - 2,
                'labels_present': labels_present,
                'canonical_file_count': len(members),
                'canonical_uncompressed_size_bytes': sum(r['archive_size_bytes'] for r in members),
                'matched_file_count': sum(r['status'] == 'matched' for r in members),
            }
            manifest['houses'][str(house)] = summary
            print(f"house_{house}: {summary['matched_file_count']}/{len(members)} files matched; {len(channel_ids)} channels, labels={labels_present}.", flush=True)
        manifest['totals'] = {
            'archive_member_count': len(manifest['archive_members']),
            'archive_regular_file_count': sum(e['type'] == 'regular_file' for e in manifest['archive_members']),
            'archive_directory_count': sum(e['type'] == 'directory' for e in manifest['archive_members']),
            'canonical_file_count': len(manifest['canonical_files']),
            'channel_file_count': sum(h['channel_file_count'] for h in manifest['houses'].values()),
            'label_file_count': sum(h['labels_present'] for h in manifest['houses'].values()),
            'canonical_uncompressed_size_bytes': sum(r['archive_size_bytes'] for r in manifest['canonical_files']),
            'matched_file_count': sum(r['status'] == 'matched' for r in manifest['canonical_files']),
        }
    except (OSError, ValueError, tarfile.TarError) as exc:
        errors.append(str(exc))
    manifest['verification_passed'] = not errors
    manifest['verification_finished_utc'] = datetime.now(timezone.utc).isoformat()
    manifest['elapsed_seconds'] = round(time.monotonic() - started, 3)
    write_json(output, manifest)
    print(f"Verification {'PASSED' if manifest['verification_passed'] else 'FAILED'}; manifest: {output}", flush=True)
    for error in errors:
        print(f'ERROR: {error}', flush=True)
    return 0 if manifest['verification_passed'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset-root', type=Path, default=DEFAULT_ROOT)
    parser.add_argument('--output', type=Path, default=Path(__file__).with_name('provenance.json'))
    args = parser.parse_args()
    return verify(args.dataset_root.absolute(), args.output.absolute())


if __name__ == '__main__':
    raise SystemExit(main())
