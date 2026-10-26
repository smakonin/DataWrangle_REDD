# Companion evaluation notes

These technical notes accompany the importer documentation. REDD is a
foundational benchmark with continuing value for historical reproduction and
method development. The notes explain how measurement conditions, task
definitions, and preprocessing limit the conclusions an experiment supports.
They distinguish properties of REDD from transformations in this repository's
archived importer.

| Document | Edition | Editable source |
| --- | --- | --- |
| [One-page summary](redd_nilm_tldr.pdf) | 25 October 2026, v1.2 | [LaTeX](source/tldr/main.tex) |
| [Detailed evaluation notes](redd_nilm_validation_white_paper.pdf) | 25 October 2026, v1.2 | [LaTeX](source/whitepaper/main.tex) |

Version 1.2 adds a full six-house low-frequency audit and current-importer execution:
**56,341,629 source records, 116 channels, and six homes**. All 122 canonical
channel and label files matched the author's locally retained Version 1.0
archive byte for byte. Its SHA-256 identifies that archive copy; no independent
official archive checksum is asserted.

The audit found zero malformed records or within-channel duplicate timestamps and 47,957
descending timestamp adjacencies. Independent checks verified preservation of
all 14,113,792 original mains records, including source line numbers, and 14,851
independent consistency checks of the reviewed diagnostics passed. The documented
latent partial-group and endpoint defects of the historical importer had zero
observed triggering cases in this copy. Gaps, cadence, exact-time selection, and
measurement compatibility still constrain interpretation. Counts and
threshold-defined shared durations are not measurements of sensor loss, physical
clock offsets, missing energy, calibration error, or effects on NILM accuracy.
No model accuracy or metrology experiment was performed.

The archived 2015 log remains a distinct case study: its 2,858 mains
substitutions or rebuilds were actions of Stephen Makonin's importer, not REDD's
acquisition pipeline. They do not establish that another publication used those
repairs. The current importer preserves observed mains and records missingness
and diagnostics separately. Archived source copies retain their exact
historical Git bytes and whitespace.

Stephen Makonin coauthored AMPds2, RAE, and R1Hz, which are discussed among the
complementary datasets. Their inclusion is not a claim that they are universally
more accurate than REDD. The notes recommend choosing and auditing reference
data for the particular validation question.

## Inspect the evidence

- [Fresh aggregate data and provenance](source/whitepaper/data/fresh/) contain
  the per-house summaries, exact interval histograms, threshold protocols,
  archive/source hashes, execution records, and preservation-check results.
- [Fresh-audit analysis material](source/whitepaper/analysis/fresh_audit/)
  preserves the executed scripts and the importer snapshot. Its nested
  `work/redd_full_audit/` and `work/DataWrangle_REDD/` layout retains the original
  scripts' relative paths.
- [Reviewer's notebook](source/whitepaper/analysis/fresh_audit/review_results.ipynb)
  provides an inspectable companion to the aggregate results. Reproducing the
  full source scan requires a separately obtained original REDD release; the
  public aggregate tables alone do not reproduce the raw-value comparisons.

The full audit does not redistribute the raw archive, channel files, per-record
masks, diagnostic databases, or generated observation exports. Previously public
historical-log excerpts, including logged before/after readings, remain included
as evidence of the old importer's behavior. The three
new actual-data figures complement the retained historical-log figures. Consult
the supplied analysis guidance before running the executed scripts: some retain
the original local defaults and expect the documented nested layout. Use new
output directories and explicit paths to a separately held raw copy.

## Build the LaTeX sources

Compile `main.tex` from its own directory, using XeLaTeX or another compatible
engine that supports `fontspec`. For example, from `docs/source/whitepaper`:

```bash
xelatex -interaction=nonstopmode -halt-on-error main.tex
xelatex -interaction=nonstopmode -halt-on-error main.tex
```

Use the same commands from `docs/source/tldr` to build the summary. A complete
LaTeX installation must provide the packages used by each source, including
`fontspec` and, for the detailed notes, `pgfplots`. **Times New Roman must be
installed locally; proprietary font files are not distributed here.** Keep the
supporting source files and figure inputs in their existing relative locations.
The resulting `main.pdf` can be compared with the committed edition; compiler
and font versions can affect pagination and PDF metadata.
