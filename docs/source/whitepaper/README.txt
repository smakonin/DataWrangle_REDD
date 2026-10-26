Limits of REDD-Only Evidence for NILM Deployment

Stephen Makonin. Companion technical note, version 1.1, 25 October 2026.

REDD remains valuable for historical reproduction and focused benchmark tests.
This note discusses the scope of deployment claims and distinguishes published
dataset limitations from transformations in the author's archived 2015 importer.
It reports an archived-log analysis, not a fresh replay of raw REDD or a model
accuracy experiment. The current Python 3 importer preserves measured mains.

Disclosure: Stephen Makonin wrote the archived importer and coauthored the
AMPds2, RAE, and R1Hz dataset publications. The comparisons identify complementary
evaluation roles, without a universal accuracy ranking; equal quality and
provenance scrutiny applies to all datasets.

Compile main.tex twice with XeLaTeX or Tectonic. The three plots are editable
PGFPlots sources in figures/. figure_*.tex compiles each plot separately.
Times New Roman must be installed. fontsettings.tex uses local fonts/ when
present, or the installed font. Proprietary font binaries are not distributed.

data/ contains six-house summaries, all 476 detailed fallback records,
distributions, and definitions. analysis/ contains the parser and exact pinned
Git copies of the archived log, importer, README, and license. The historical
Python 2 importer is inspection material and is not run by this project.

To reproduce the log calculations with Python 3, from this directory run:
python3 analysis/analyze_log.py --outdir data

The statistics do not measure raw sensor missingness, physical clock offsets,
energy changes, or NILM accuracy. The historical log/source output-name mismatch
is documented in the note. Pinned commit:
66ea7e01e499be2bb6b88dbaf8f3cee3445cbe0d.

references.bib retains the supplied R1Hz journal-paper entry. main.tex contains
the complete inline bibliography used by the compiled edition.
