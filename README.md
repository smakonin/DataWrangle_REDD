# DataWrangleREDD

Import the REDD low-frequency files with Python 3 while preserving observations,
missing values, original timestamps, and the provenance of derived load groups.
The importer uses only the Python standard library and requires **Python 3.10 or
newer**.

## Run an import

Download and extract the original REDD low-frequency archive separately. Point
`--input-dir` at the directory containing `house_1`, `house_2`, and so on:

```text
/path/REDD/low_freq/
  house_1/
    channel_1.dat
    channel_2.dat
    ...
    labels.dat
  house_2/
    ...
```

From this repository, run:

```bash
python3 Import_REDD_dataset.py \
  --input-dir /path/REDD/low_freq \
  --output-dir ./datasets \
  --houses 1 2 3
```

Omit `--houses` to import all six houses. Use `--help` for the command-line
options or `--version` to identify the importer release. There is no need to
edit paths inside the script.

`labels.dat` is optional. When present, its labels and file hash are recorded in
the manifest. `labels.txt` is accepted only when `labels.dat` is absent; having
both is an error because the metadata source would be ambiguous. Label files
must contain each expected channel exactly once. Recorded labels provide
provenance; they do not silently replace the explicit historical group mapping.

Each selected house produces its own directory, such as `datasets/house_1`.
The importer refuses to overwrite an existing house directory; use a new output
directory for another run. Invalid input stops the import instead of silently
dropping or repairing the offending observation. Missing or extra channel files,
malformed or nonfinite readings, and conflicting duplicates are errors. Preserve
the reported input location when investigating a failure.

Publication is atomic for each house: a failed house leaves no partial house
directory. Houses successfully imported earlier in the same command remain
available if a later house fails.

The importer stages observations in a temporary SQLite database on disk inside
the output root. Allow space for that spool and the CSV exports. The spool is
removed after success or failure; the input files are never modified.

## Output files

| File | Contents |
| --- | --- |
| `channels.csv` | All parsed source observations, including identical duplicate rows, with `TimeStamp`, `Channel`, `Value`, and `SourceLine`. This is the reference for inspecting or regrouping the original channels. |
| `loads.csv` | Derived groups on the union of observed timestamps. Both mains legs remain available as `MAIN_1` and `MAIN_2`. Missing derived values are blank. Negative `DIFF` values are retained. |
| `quality.csv` | Observed and missing channel IDs, mains/target completeness flags, and residual and timing diagnostics for each derived row. |
| `issues.csv` | Detected duplicate and source-order events, with locations for inspection. |
| `manifest.json` | Channel-file and optional label-file hashes, importer version and source hash, channel-to-group mappings, counts, timestamp-interval diagnostics, and hashes of the four CSV exports. |

`loads.csv` and `quality.csv` share one row per timestamp in the sorted union of
source observations. `channels.csv` retains per-channel source order and includes
the original source line number; it is not a globally sorted time series.

The `quality.csv` fields are:

| Field | Meaning |
| --- | --- |
| `TimeStamp` | The same observed timestamp as the corresponding `loads.csv` row. |
| `observed_channels`, `missing_channels` | Semicolon-separated channel IDs present or absent at that exact timestamp. |
| `mains_complete` | `1` when both mains legs are observed; otherwise `0`. |
| `targets_complete` | `1` when every non-main historical group has all its constituent observations; otherwise `0`. |
| `row_complete` | `1` when both mains and all target groups are complete; otherwise `0`. |
| `negative_residual` | `1` for a negative `DIFF`, `0` for a nonnegative `DIFF`, and blank when `DIFF` cannot be calculated. |
| `seconds_since_previous_row` | Difference from the previous timestamp in the union; blank for the first row. This is not a per-channel sample interval. |

Use these flags and the manifest's constituent mappings to choose a quality mask
for the particular channels being evaluated. Requiring `row_complete` also
requires every historical target group, which may be stricter than the needs of
a single-target experiment.

`issues.csv` has fields `kind`, `channel`, `timestamp`, `source_line`, and
`detail`. Its event kinds are `identical_duplicate` and `out_of_order`.
Malformed input and conflicting duplicates cause an error rather than a
published issue-only export.

Identical readings at the same timestamp in one channel remain separate source
rows in `channels.csv`, but contribute only once to the canonical grouped value.
Conflicting values for that channel and timestamp are rejected. Observations
that arrive out of order are recorded as issues; timestamp alignment does not
depend on source row order.

The importer does **not** allocate a dense one-second calendar grid, resample,
fill missing readings, replace a mains measurement, or reconstruct aggregate
input from target channels. A blank value is unobserved, not an appliance being
off. Retained timestamps can be irregular; do not treat consecutive CSV rows as
consecutive seconds.

## Understand the groups and residual

The historical category mapping is retained for comparison, but a category is
not necessarily one physical appliance. A group is calculated only when **all
of its constituent channels** have observations at exactly the same timestamp.
In particular, `MAIN` requires both original mains legs. A missing constituent
leaves that group blank instead of silently summing the available subset.

`DIFF` is `MAIN` minus the sum of the non-main groups when every required group
is available. It is a diagnostic residual, not certified unknown-load power.
The script does not certify electrical units, calibration, power-factor
compatibility, or physical additivity. A negative residual is preserved for
investigation; it is not automatically classified as a clock error.

Review the mapping before interpreting an output as appliance ground truth:

- House 1 `DRYR` includes channels 10, 19, and 20. Official NILMTK metadata treats
  channel 19 as uncertain, so this group needs particular scrutiny.
- House 6 `AIRC` combines channels 15, 16, and 17, pooling HVAC components.
- House 5 `SUBP` combines subpanel channels 10 and 11. Confirm the circuit topology
  before assuming there is no overlap with other measurements.
- Kitchen, lighting, unknown-outlet, and similar categories may contain several
  circuits or devices.

The source-channel export and manifest allow a different mapping to be built
without losing the original channel identities. REDD's mains and circuit files
have different nominal sampling intervals; exact-time availability is not a
measurement of physical synchronization accuracy. Use the quality records and
interval diagnostics to choose and disclose an evaluation mask.
The manifest's interval diagnostics use sorted unique observations within each
channel. To bound memory, a histogram retains at most 256 distinct interval
values. Full minimum, maximum, and interval count remain available; the
`histogram_complete` and `omitted_intervals` fields disclose truncation. A
truncated histogram is not the complete gap distribution. Calendar span is not
continuous recording uptime.

## Compatibility with the historical importer

This Python 3 version changes the output contract. It replaces the old single
`REDDhouseN_*_VA.csv` export with a per-house directory of source, derived,
quality, issue, and provenance files. It preserves missingness and negative
residuals instead of excluding incomplete rows and modifying aggregate readings.
Update downstream scripts to read `loads.csv` together with its quality records
and manifest; do not compare its row counts directly with a historical export.

`REDD_import.log` is retained as a historical record. Its `3sec` export names
differ from the previous script's `lowf` names. It is not a fresh execution log
for this version, and its exclusion and repair counts do not describe the new
importer. Its dense-grid shrinkage percentages are not sensor-failure rates.

## Evaluation notes

REDD is a foundational NILM benchmark and remains useful for historical
comparison, reproduction, and focused method development. Two companion
technical notes discuss how measurement conditions and preprocessing affect the
scope of an evaluation:

- [One-page summary](docs/redd_nilm_tldr.pdf)
- [Detailed evaluation notes](docs/redd_nilm_validation_white_paper.pdf)

Both are dated **25 October 2026, version 1.1**. They review published descriptions
and archived processing records; they do not report a full replay of the raw
REDD archive. The notes distinguish dataset limitations from transformations in
Stephen Makonin's archived 2015 importer. Their quantitative log results describe
that historical pipeline, not raw REDD error rates or sensor data loss, and do
not establish that any REDD publication used those repairs. The current Python 3
importer preserves the observed mains legs and does not repair or reconstruct
them from target channels.

**Disclosure:** Stephen Makonin coauthored AMPds2, RAE, and R1Hz. The comparisons
discuss complementary validation roles, not a universal ranking of dataset
accuracy. Each dataset's actual release, channels, and measurement assumptions
still require inspection. Editable LaTeX sources and supporting data are
described in [the documentation guide](docs/README.md).

## Run the tests

The tests use small synthetic files and require no REDD download or third-party
packages:

```bash
python3 -m unittest discover -s tests -v
```

GitHub Actions runs the same command on Python 3.10, 3.12, and 3.14. Passing these
tests checks the importer's behavior; it does not independently validate REDD's
physical measurements or reproduce results from the full archive.

## Citation and license

If you use this code, please cite the original thesis:

> Makonin, S. (2014). *Real-Time Embedded Low-Frequency Load Disaggregation*.
> PhD thesis, Simon Fraser University, School of Computing Science.

Record the code revision and the input and output hashes from the manifest when
reporting an import. Cite the original REDD dataset separately when using its
data: J. Zico Kolter and Matthew J. Johnson, *REDD: A Public Data Set for Energy
Disaggregation Research*, SustKDD, 2011.

The repository's [Mozilla Public License 2.0](LICENSE) is retained. The software
license does not replace the terms applying to the REDD source data.

Copyright (C) 2013 Stephen Makonin. All Right Reserved.
