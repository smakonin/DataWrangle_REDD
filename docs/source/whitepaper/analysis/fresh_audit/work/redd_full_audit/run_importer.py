"""Run the merged importer on each verified house and retain an execution record."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import resource
import subprocess
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parents[2]
REPO = ROOT / "work/DataWrangle_REDD"
SOURCE = REPO / "Import_REDD_dataset.py"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("merged_redd_importer", SOURCE)
    importer = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = importer
    spec.loader.exec_module(importer)
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO).decode().strip()
    result = {
        "scope": "Full canonical low-frequency REDD Version 1.0; six houses, 116 channels.",
        "importer_commit": commit,
        "importer_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
        "python_version": platform.python_version(),
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "houses": [], "status": "running",
    }
    record = Path(__file__).parent / "import_execution.json"
    start = time.monotonic()
    for house in [2, 1, 3, 4, 5, 6]:
        print(f"Starting house {house}", flush=True)
        before = time.monotonic()
        entry = {"house": house}
        try:
            destination = importer.import_house(args.input_dir, args.output_dir, house)
            manifest = json.loads((destination / "manifest.json").read_text())
            entry.update(status="success", counts=manifest["counts"],
                         manifest_sha256=hashlib.sha256((destination / "manifest.json").read_bytes()).hexdigest())
        except Exception as exc:
            entry.update(status="failed", error_type=type(exc).__name__, error=str(exc))
            (Path(__file__).parent / f"house_{house}_failure.txt").write_text(traceback.format_exc())
        entry["elapsed_seconds"] = round(time.monotonic() - before, 3)
        result["houses"].append(entry)
        result["elapsed_seconds"] = round(time.monotonic() - start, 3)
        record.write_text(json.dumps(result, indent=2) + "\n")
        print(json.dumps(entry), flush=True)
    result["status"] = "success" if all(x["status"] == "success" for x in result["houses"]) else "completed_with_failures"
    result["completed_utc"] = datetime.now(timezone.utc).isoformat()
    result["peak_resident_mib"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (
        1024 * 1024 if sys.platform == "darwin" else 1024)
    record.write_text(json.dumps(result, indent=2) + "\n")
    print("Completed: " + result["status"], flush=True)

if __name__ == "__main__":
    main()
