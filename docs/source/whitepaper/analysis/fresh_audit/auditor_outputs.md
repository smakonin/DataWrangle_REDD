# Full-release REDD audit: implementation and metric contract

This local audit reads the canonical `house_1` through `house_6` directories in
the original low-frequency release. It is separate from the merged, strict
DataWrangle_REDD importer. The auditor continues after malformed records and
conflicting duplicates so those defects can be counted rather than hidden by a
failed import. It does not change, fill, realign, or reconstruct measurements.

The completed run examined **116 channel files, 932,604,931 source bytes, and
56,341,629 physical source records**. Archive-to-file verification is recorded
separately in `provenance.json`; that verification, rather than filenames alone,
establishes the relationship to the original archive. Only the six canonical
house directories were selected. A duplicate `house_2 copy` directory and any
cleaned exports were excluded.

## Reproduce the audit

The script requires Python 3.10+ and the standard library. It imports the parsing
helper and historical category definitions from the sibling checkout
`../DataWrangle_REDD/Import_REDD_dataset.py`. Both code hashes are recorded in
`raw_results/summary.json`.

From the workspace root, run:

```bash
python3 work/redd_full_audit/audit_raw.py \
  --input-dir /path/to/original/REDD/low_freq \
  --output-dir work/redd_full_audit/raw_results_new \
  --houses 1 2 3 4 5 6 \
  --keep-db
```

Existing output is refused. Output must be outside the raw directory. A complete
result directory is published only after the audit finishes. `--keep-db` retains
the diagnostic SQLite database for independent checks; omitting it removes that
database after exporting the tables. Raw files are opened for reading only.
SHA-256 is accumulated over the exact source bytes, including line endings, in
the same pass used to parse the records. Size and modification-time checks also
detect an observed source-file change during reading.

The synthetic definition checks can be run with:

```bash
python3 -m unittest discover -s work/redd_full_audit -p test_audit_raw.py -v
```

The five tests cover conflict/malformed/order reporting, invalid-key interval
breaks, exact histograms beyond 256 distinct intervals, a wholly malformed
channel, and the legacy ANY-member rule hiding missing constituents. Passing
these checks validates those computations, not REDD's metrological quality.

## Outputs and schema

| File | Grain and purpose |
| --- | --- |
| `raw_results/summary.json` | Code hashes, source-file metadata, house-level counts, definitions, thresholds, and output hashes. |
| `raw_results/source_files.csv` | One row per channel file: exact byte/row counts and hash; valid/malformed records; duplicate/conflict/order flags; sorted-unique boundaries, descriptive modal interval, and timestamp density. |
| `raw_results/interval_histogram.csv` | Exact frequency by `file_id`, `house`, `channel`, positive `interval_seconds`, and `conflict_touched`. There is no 256-bin cap. `conflict_touched` also includes known timestamped-invalid keys. |
| `raw_results/threshold_spans.csv` | One row per file and named threshold; connected and excluded durations, long-interval counts, and the additional unambiguous span. |
| `raw_results/simultaneous_spans.csv` | One row per house, group, and fixed threshold: simultaneous bounded-gap duration and its two explicit window denominators. |
| `raw_results/record_masks.csv.gz` | One row per physical source record, keyed by file ID and source line: known timestamp, parsing validity, duplicate status, and descending-adjacency flag. |
| `raw_results/timestamp_masks.csv.gz` | One row per successfully parsed timestamp in a house union: observed/unambiguous channel sets, strict completeness, and legacy ANY-member/partial-group flags. |
| `raw_results/ambiguous_timestamps.csv.gz` | Keys having parsed observations and any conflicting or timestamped-invalid record. Bad-only keys remain in `record_masks` and SQLite `bad_keys`. |
| `raw_results/issues.csv.gz` | Individual malformed, duplicate, and source-order diagnostics. |
| `raw_results/audit.sqlite3` | Local diagnostic observations, invalid-key markers, exact interval frequencies, and supported spans. Contains source values; do not redistribute it. |
| `house_metrics.csv` | Small derived house-level table for figures and document integration. |
| `sixty_second_windows.csv` | The 12 MAIN/ALL_REQUIRED rows at the primary 60-second threshold, with exact numerators and denominators. |
| `source_order_summary.json` | Descending-adjacency counts and distinct house/timestamp counts; derived from the issue records. |
| `boundary_verification.json` | Independent read-only first/last source-record comparison with per-file sorted extrema. |

The compressed masks, detailed issues, extracted source values, and SQLite
files stay local. Only reviewed aggregate tables and figures should enter the
public technical notes. The auditor does not calculate power-balance or energy
metrics. Those calculations use the strict importer's preserved observations
and have a separate measurement-compatibility qualification.

## Observation and conflict definitions

- `source_rows = valid_rows + malformed_rows` counts physical lines, including
  blank lines if present. Successfully parsed numbers must be finite and their
  timestamps must fit the signed 64-bit range.
- A diagnostic representative retains the first valid reading at each
  file/timestamp. Subsequent readings are compared numerically with that first
  reading. They are counted as `identical_to_first_duplicates` or
  `conflicting_with_first_duplicates`; values are never added or repaired.
- Every key with any conflicting or known timestamped-invalid record is excluded
  from primary group-validity and simultaneous-span diagnostics. All source
  records remain locatable through file hash and source-line masks.
- An invalid record with an unreadable timestamp is reported but cannot be
  placed on a timeline. No clock position is invented for it.
- The house union contains timestamps having at least one successfully parsed
  observation. MAIN completeness requires both unambiguous mains channels;
  complete-row validity requires every expected original channel. Group
  completeness requires both mains and every constituent of that group.
- A legacy ANY-member candidate requires a valid channel-1 timestamp and at
  least one observed constituent for every historical group. The separate
  missing-constituent count identifies candidates that fail the stricter rule.
  These are input-eligibility diagnostics, not a replay of old numerical repairs.

## Temporal definitions, fixed before the scan

The primary threshold is **60 seconds**, with fixed sensitivities at **30, 120,
and 300 seconds**. Per-channel sensitivities also use the ceiling of **1.5, 2,
3, and 5 times** that channel's descriptive modal positive interval. Ties in
the mode select the smallest interval. This is a descriptor of the observed
timestamps, not a certified acquisition cadence.

For sorted unique parsed timestamps, let `delta = next_timestamp - timestamp`.
The per-channel elapsed denominator is **last minus first**, with no extrapolated
duration after the final sample. Then:

```text
threshold_connected_seconds = sum(delta for delta <= threshold)
long_interval_seconds       = sum(delta for delta > threshold)
threshold_connected_seconds + long_interval_seconds = elapsed_span_seconds
```

The additional unambiguous duration excludes an interval if a conflicting or
known timestamped-invalid key occurs at either endpoint or between its endpoints.
Such keys are never bridged. Each supported span is represented as a half-open
interval `[timestamp, next_timestamp)`, and adjacent supported spans are merged.
Simultaneous coverage is the intersection of these unions across the required
channels.

Two denominators are reported for each simultaneous duration:

1. **Mains envelope:** minimum first to maximum last timestamp across mains
   channels 1 and 2.
2. **Common endpoint window:** maximum first to minimum last timestamp across
   every channel required by that group, clipped to zero if there is no overlap.

These are **threshold-connected or bounded-gap durations**, not measurements of
continuous uptime, true missing energy, or sensor failure. Endpoints do not
certify that every intervening measurement existed. No power samples are filled
when these descriptive spans are calculated. Density on a one-second grid uses
`last-first+1` integer positions and is a separate row-count measure.

## Ordering and endpoint checks

An `out_of_order` flag means the current valid source timestamp is smaller than
the previous valid source timestamp in that file. It counts a **descending
adjacency**, not every inverted pair and not every record that would move during
a full sort. For example, `1,4,2,3` has one such descent although both `2` and `3`
arrive after `4`.

The scan found **47,957 flagged source rows**, corresponding to **6,528 distinct
house/timestamp pairs**. Several channel files can flag the same timestamp;
those are not independent physical clock events. Neither count proves a clock
offset or identifies its cause. The boundary check found the first and last
source timestamps equal the sorted minimum and maximum in all 116 files, so
internal disorder did not invalidate the old endpoint assumption on this release.
First/last line samples used LF endings; that endpoint sample is not a census
of every line's ending or a calculation of historical parser effects.

## Resource and provenance notes

Unique timestamp/value representatives and exact interval histograms are staged
in SQLite on disk. Python interval-frequency batches have at most 4,096 entries;
group merging retains only one timestamp's fixed channel set. Supported spans
are precomputed once per channel/threshold so each group comparison reads span
segments rather than rescanning the original observations. Detailed record
masks are streamed to gzip at compression level 1. Memory is not allocated per
calendar second.

`summary.json` hashes the aggregate exports and masks but does not hash itself
or the retained SQLite database. Its source hashes are the audit's byte-level
provenance; the independent archive comparison remains the controlling release
verification. Statistical masks do not certify electrical quantity, calibration,
physical additivity, synchronization, or appliance identity.
