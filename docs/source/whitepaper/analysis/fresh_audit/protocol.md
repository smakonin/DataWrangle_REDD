# Predeclared evaluation protocol for the full REDD audit

Protocol v1.0, frozen 2026-10-26 01:55:20 UTC (25 October 2026 in Vancouver).
Prepared before any new full-file statistical scan. At declaration, only the
archive directory inventory, Version 1.0 readme, house-1 labels, and four initial
records from two channels had been inspected. Earlier historical-log results
are not fresh raw-data results.

## Scope and controlling evidence

- Audit canonical `low_freq/house_1` through `house_6`, after root verifies them
  against `low_freq.tar.bz2`. Exclude `house_2 copy`, `cleaned`, converted ZIPs,
  high-frequency files, and prior derived exports from the controlling source.
- Preserve original bytes and record archive/file hashes, source revision,
  parser/auditor revision, configuration, date, and exclusions. The readme and
  labels are source metadata, not evidence that the meters were calibrated.
- Unit of raw acquisition evidence: a source line in a particular channel file.
  Unit of canonical observation: `(house, channel, UTC integer timestamp)`.
  Unit of exact shared analysis: a timestamp with all explicitly required,
  unambiguous finite observations. Household is the unit for household coverage;
  a large number of timestamps is not a large independent household sample.
- This is a descriptive acquisition/processing audit, not a NILM accuracy test.
  No new model experiment or generalization improvement can be inferred.

## What the local readme establishes, and what it does not

The Version 1.0 readme says mains are logged about once a second, circuits about
every three seconds, timestamps are UTC integers, and labels identify the main
device category on a circuit that can contain several devices. It calls the
low-frequency values “apparent power of the circuit,” despite also using
“time/wattage” language. NILMTK's active-circuit/apparent-mains convention is
therefore a conflicting metadata convention, not a resolved physical fact.

Use **source power units** for numerical balance diagnostics unless the actual
measurement quantity is independently established. Do not call a signed
residual calibrated watts, unknown-load energy, or verified sensor error.
The initial circuit sample contains both three- and four-second spacings;
nominal three seconds is not a certified uniform slot clock. No timestamp-only
audit establishes physical simultaneity, averaging windows, or clock accuracy.

## Raw validity and uniqueness

For each channel report source rows, parse-valid finite rows, malformed rows,
valid timestamps with invalid measurements, unique timestamp keys, identical
repeats, conflicting keys, and source-order reversals. Give each count's
denominator explicitly; a zero denominator produces `null`/N/A, not zero.

- Reject nonfinite measurement tokens as usable observations; retain their
  source locations in the audit. Zero and finite negative values are not
  missing markers. `-999` has no special meaning in the raw-data audit.
- Distinguish repeated rows from repeated keys. A duplicated key can have two
  or more identical records or two or more different values. Count ambiguous
  keys separately from the number of records involved.
- A deterministic first valid record may represent a key in examples or a
  diagnostic spool, but **exclude conflicting keys from primary numerical
  balance and usable-reference analyses**. Do not silently pick a value.
- If counts compare each later value only with the first, name them
  `repeats_equal_to_first` and `records_disagreeing_with_first`; those are not
  necessarily all duplicate-value classes for keys with three or more records.
- Count source-order reversals using the declared rule (e.g. `t_i < t_(i-1)` in
  file order). Do not call every record after the first disorder event bad.

The strict merged importer may refuse a malformed/conflicting house. Such a
failure is a result to report, not permission to modify the raw files so that
the importer completes. The raw auditor may continue profiling with masks.

## Exact-time denominators: never substitute one for another

Let `T_c` be unique timestamps with a finite unambiguous value in channel `c`.
Define each set for a stated house and stated interval; publish both numerator
and denominator alongside any percentage.

| Symbol | Definition | Interpretation |
| --- | --- | --- |
| `R_c` | Number of source rows in channel c | Acquisition/export rows, including repeated keys |
| `N_c` | `|T_c|` | Distinct usable timestamp keys, not elapsed seconds |
| `D_c` | `last(T_c) - first(T_c)` | Elapsed endpoint span; no added cadence |
| `G_c` | `D_c + 1` for integer timestamps | Number of integer-second grid positions between endpoints |
| `U_all` | Union of all required channel timestamp sets | Native exact-time union; the new importer emits these keys |
| `M` | `T_1 intersection T_2` | Both mains legs observed at the same timestamp |
| `C_g` | `M intersection` all original constituent sets for target g | Exact usable mains/target-g pairing |
| `C_all` | Intersection of all required channel sets | Exact completeness for every historical group |

Report `|M|`, unilateral mains-leg counts, `|C_all| / |U_all|`,
`|C_all| / |M|`, and each `|C_g| / |M|`. If comparing with old channel-1
denominators, also give `|C_all| / |T_1|` and name that denominator. Require all
constituents before declaring a group observed; a partial sum is not complete.

`N_c / G_c` is integer-grid timestamp density. For submeter cadence near three
seconds, sparsity on a one-second grid is expected. It is not sensor loss,
continuous uptime, energy coverage, or the fraction of useful appliance cycles.
All-target completeness can be much stricter than the mask needed for one
target; report per-target pairing rather than presenting the strictest mask as
the only usable data.

Report channel starts/ends and common endpoint overlap separately. Delayed
recording starts are boundary coverage differences, not proven clock offsets.

## Interval quality and preregistered sensitivity thresholds

Sort unique timestamps and form all positive consecutive spacings `delta_i`.
Publish exact spacing counts on disk (not an unlabelled truncated histogram),
minimum, median, mode, p90/p95/p99, maximum, and endpoint span. Quantiles use
linear interpolation at zero-based rank `(n - 1) p` (type 7); an empty set has
undefined quantiles. A descriptive mode uses the smallest spacing in a tie.

Absolute gap thresholds are **30, 60, 120, and 300 seconds**, with **60 seconds
the primary descriptive threshold**. Cadence-relative sensitivity thresholds
are `ceil(k * c)` for `k = 1.5, 2, 3, 5`, where `c` is the channel's descriptive
modal positive spacing. Report `c` and the resulting thresholds. These choices
are operational definitions, not instrument specifications or validated outage
boundaries; do not choose a preferred threshold after seeing which looks best.

For every threshold `T` and channel:

- `connected_seconds(T) = sum(delta_i for delta_i <= T)`.
- `excluded_gap_seconds(T) = sum(delta_i for delta_i > T)`.
- Their exact sum is `D_c`; count long-gap intervals separately.
- Give `connected_seconds(T) / D_c`, the number of connected blocks, their
  length distribution, and the longest excluded intervals with source context.

Call this **threshold-connected duration**, not observed continuous uptime.
Two endpoint observations do not certify the values between them. The statistic
neither fills those values nor estimates events/energy during missing periods.
Use “long acquisition gap” or “outage candidate”; timestamps alone cannot
distinguish radio loss, logger downtime, intentional unplugging, or a clock
issue. Spacings above the modal cadence but below `T` are short spacing
exceedances, not a verified count of missing packets. Do not infer missing
sample counts by dividing every spacing by an assumed three seconds.

## Cadence-aware shared-duration proxy

Exact timestamp intersections penalize different native cadences and timestamp
phases even when the streams cover similar periods. Complement them with a
duration analysis that does not pretend to create new observations.

For each required stream, create half-open intervals `[t_i, t_(i+1))` only when
the two adjacent usable keys are separated by at most `T`. Do not extend past
the last observation. Do not bridge across a known ambiguous key or a
timestamped invalid measurement in the usable-reference version. Intersect
these interval unions for the two mains legs and for each required target
constituent. Use an interval sweep, not a dense calendar-second allocation.

Report intersection duration on two clearly named windows:

1. The house mains endpoint envelope (earliest mains start to latest mains end),
   showing boundary and internal availability limitations together.
2. The common endpoint overlap of the required streams (latest required start
   to earliest required end), separating boundary mismatch from internal gaps.

Each fraction is shared threshold-connected duration divided by that window's
elapsed duration. Empty/nonpositive common windows yield N/A. Repeat the
absolute-threshold grid. If cadence-relative thresholds are applied per stream,
state that they differ by stream; do not label that one common threshold.

This is an **availability proxy under a gap rule**, not simultaneous measured
sample coverage, a correction of physical synchronization, or independent
ground truth between observations. Do not score interpolated values as new
event labels or select an offset using test target values. No lag correction is
part of the primary audit.

## Signed balance diagnostics and sensitivity

At exact fully usable keys, retain `M = MAIN_1 + MAIN_2`, selected complete
target sums `S`, and `R = M - S`, without changing any source value. Run a
strict all-required-channel diagnostic and identify any separate selected-target
diagnostic explicitly. Exclude ambiguous keys from the primary calculation.

Report number of eligible keys; negative/zero/positive residual counts and
fractions; signed and absolute residual summaries; per-house distributions;
and deterministic examples linked to source timestamps and line numbers.
The primary negative test is exactly `R < 0`, preserving decimal precision.
Absolute sensitivity thresholds are `R < -a` for `a = 1, 5, 10, 50` in source
power units. Optional relative sensitivities are `R < -q * abs(M)` for
`q = 0.01, 0.05, 0.10`, restricted to `abs(M) >= 10` source units; report both
the restriction and the eligible denominator. These are sensitivity cutoffs,
not claimed meter-error tolerances.

If reported, `sum(abs(R)) / sum(abs(M))` on these exact keys is a
**sample-weighted absolute residual ratio**, not an energy-weighted noise ratio.
Irregular spacing and cadence selection mean raw sums are not time integrals.
Do not clip negative residuals, infer a physical clock error from their sign,
or automatically reconstruct mains. Meter quantities, calibration, averaging
windows, unknown loads, overlapping circuits, and timestamp mismatch can all
affect the diagnostic.

Do not report Wh, unmetered-energy percentages, or energy changes from sparse
rows without establishing compatible active-power units and a declared
integration rule on common valid intervals. Missing-interval energy remains
unknown. Demonstrate preservation by comparing every original source value and
each observed mains leg with the exports, then checking the derived two-leg
sum where available; do not substitute a purported zero-energy-error claim.

## Independent crosschecks and interpretation gate

Before updating publications:

- Reconcile raw rows, valid keys, duplicate classes, masks, union/intersections,
  and importer manifest counts. Match source and output hashes. Confirm no
  accepted source measurement was changed and no group is complete without
  all its required constituents.
- Check interval partitions exactly; verify common-duration intersections are
  bounded by every contributing stream/window. Recompute the headline fractions
  independently from saved counts, with N/A for zero denominators.
- Distinguish new raw-archive findings from old log results. Do not assume that
  the new archive is the identical archive processed in 2015 unless byte-level
  identity is established. Keep historical counter definitions separate.
- Report sensitivity materially changing a conclusion rather than promoting
  one threshold as a definitive quality score. No universal dataset accuracy
  rank or effect on a NILM model follows from this audit alone.

This protocol can be revised for an implementation or source constraint, but
record the version, time, reason, and whether results had been inspected before
changing a threshold or denominator.
