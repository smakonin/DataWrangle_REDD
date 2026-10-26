Limits of REDD-Only Evidence for NILM Deployment

Stephen Makonin. Companion technical note, version 1.2, 25 October 2026.

REDD remains valuable for historical reproduction and focused benchmark tests.
This note discusses the scope of deployment claims and distinguishes properties
of REDD from transformations in the author's archived 2015 importer. Version 1.2
adds an actual-data audit and a full current-importer run: 56,341,629 records,
116 channels, and six homes. All 122 canonical channel/label files matched the
author's local Version 1.0 archive byte for byte. The archive SHA-256 identifies
that retained copy; it is not a claimed independently published official hash.

The audit found zero malformed records or within-channel duplicate timestamps and 47,957
descending timestamp adjacencies. A separate validator verified preservation of
all 14,113,792 original mains records, including source line numbers; 14,851
independent arithmetic checks of the diagnostic results also passed. The old
partial-group and endpoint defects had zero observed triggering cases in this
copy. Gaps, cadence, exact-time selection, and measurement compatibility remain
evaluation concerns without demonstrating general numerical corruption.
No NILM model accuracy, calibration, or other metrology experiment was performed.

The archived log is a separate historical case study. Its 2,858 mains
substitutions or rebuilds were actions of Stephen Makonin's 2015 importer,
not REDD's acquisition pipeline. The current importer preserves measured mains
and performs no such repairs. These records establish no use of the old repairs
by any other publication.

Disclosure: Stephen Makonin wrote the archived importer and coauthored the
AMPds2, RAE, and R1Hz dataset publications. The comparisons identify complementary
evaluation roles, without a universal accuracy ranking; equal quality and
provenance scrutiny applies to all datasets.

Compile main.tex twice with XeLaTeX or Tectonic. The three actual-data plots and
three retained historical-log plots are editable PGFPlots sources in figures/.
The figure_*.tex wrappers compile each plot separately.
Times New Roman must be installed. fontsettings.tex uses local fonts/ when
present, or the installed font. Proprietary font binaries are not distributed.

data/fresh/ contains reviewed actual-data aggregates, hashes, archive provenance,
execution records, interval/coverage tables, and preservation checks. The other
data/ files retain the historical six-house log summaries, all 476 detailed
fallback records, distributions, and definitions. analysis/ contains the log
parser and exact pinned Git copies of the archived log, importer, README, and
license. The historical Python 2 importer is inspection material and was not
executed for the fresh audit.

analysis/fresh_audit/ contains the fresh-audit scripts, protocol guidance,
review_results.ipynb, and the executed importer snapshot. Nested directories
work/redd_full_audit/ and work/DataWrangle_REDD/ preserve the original relative
paths. The reviewer's notebook checks published aggregates; repeating the raw
scan and mains-value comparisons requires a separately held original release.
Some executed scripts retain local defaults and layout assumptions: read the
analysis guidance and provide explicit input paths and new output directories.

Original raw files, channel values, record-level masks, SQLite databases, and
generated import exports are not distributed with these notes.

To reproduce the log calculations with Python 3, from this directory run:
python3 analysis/analyze_log.py --outdir data

The completeness percentages describe exact timestamp coincidence; the shared
duration measures use explicitly bounded gaps. Neither is sensor uptime or a
sensor-loss rate. The statistics do not measure physical clock offsets, missing
energy, calibration error, or NILM accuracy. The historical log/source
output-name mismatch is documented in the note. Historical pinned commit:
66ea7e01e499be2bb6b88dbaf8f3cee3445cbe0d.

references.bib retains the supplied R1Hz journal-paper entry. main.tex contains
the complete inline bibliography used by the compiled edition.
