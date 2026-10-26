# Companion evaluation notes

These technical notes accompany the importer documentation. REDD is a
foundational benchmark with continuing value for historical reproduction and
method development. The notes explain how measurement conditions, task
definitions, and preprocessing limit the conclusions an experiment supports.
They distinguish properties of REDD from transformations in this repository's
archived importer.

| Document | Edition | Editable source |
| --- | --- | --- |
| [One-page summary](redd_nilm_tldr.pdf) | 25 October 2026, v1.1 | [LaTeX](source/tldr/main.tex) |
| [Detailed evaluation notes](redd_nilm_validation_white_paper.pdf) | 25 October 2026, v1.1 | [LaTeX](source/whitepaper/main.tex) |

The historical log statistics concern one archived processing record, not a new
execution of the raw REDD archive. They are not measurements of raw sensor loss,
clock faults, or effects on NILM accuracy, and do not establish that another
publication used this repository's repairs. Supporting tables and figure data
retain that limited provenance. The current Python 3 importer preserves observed
mains measurements and records missingness and diagnostics separately.
Archived source copies retain their exact historical Git bytes and whitespace.

Stephen Makonin coauthored AMPds2, RAE, and R1Hz, which are discussed among the
complementary datasets. Their inclusion is not a claim that they are universally
more accurate than REDD. The notes recommend choosing and auditing reference
data for the particular validation question.

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
