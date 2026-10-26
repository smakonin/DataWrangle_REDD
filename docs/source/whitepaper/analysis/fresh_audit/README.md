# Fresh full-archive audit: version 1.2

This folder preserves the executed analysis code and a snapshot of the merged
importer. The nested work/ layout preserves the executed scripts' relative
helper imports. The importer SHA-256 identifies the implementation independently
of an enclosing checkout's current Git revision.

Read protocol.md for grains, denominators, and predeclared thresholds.
Published aggregates and portable provenance are in ../../data/fresh/.
Machine-local result paths use placeholders; source hashes and numbers are
unchanged. import_verification.json keeps the original execution provenance
fingerprint and links to the published provenance representation.

No raw archive, measurements, row masks, importer observation exports, or SQLite
database is redistributed. Obtain REDD separately under its applicable terms.
The archive checksum identifies the local copy, not an independently published
official checksum.

## Reproduce

Python 3.10+ and its standard library are sufficient. From this folder, replace
/path/REDD with the source root containing low_freq.tar.bz2 and low_freq/:

    python3 work/redd_full_audit/verify_archive.py --dataset-root /path/REDD
    python3 work/DataWrangle_REDD/Import_REDD_dataset.py --input-dir /path/REDD/low_freq --output-dir work/redd_full_audit/imported
    python3 work/redd_full_audit/audit_raw.py --input-dir /path/REDD/low_freq --output-dir work/redd_full_audit/raw_results --keep-db
    python3 work/redd_full_audit/verify_import.py --input-dir /path/REDD/low_freq --output-dir work/redd_full_audit/imported
    python3 work/redd_full_audit/summarize_results.py

Use fresh output folders for another run. Allow disk space for large derived
CSV/mask files and SQLite staging. Generated raw-derived files are local working
material and are excluded from publication. The original run_importer.py is
retained for execution traceability; it queries Git and records the enclosing
checkout revision, so the direct importer command above is more portable.

The executed source snapshots retain their original local default locations;
the explicit command-line paths above override those defaults. The audit does
not calibrate meters, infer true clock offsets, reconstruct missing energy, or
measure NILM accuracy.

## Inspect without the raw dataset

review_results.ipynb reads small published aggregates and recomputes headline
ratios. The auditor's synthetic metric checks run without REDD:

    python3 -m unittest discover -s work/redd_full_audit -p 'test_audit_raw.py' -v

The independent SQL/interval crosscheck script is also retained; its report is
in ../../data/fresh/independent_checks.json. Re-execution requires the retained
local database/outputs and adjustment of captured paths. It is independent
verification material, not part of the importer.
