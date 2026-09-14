#!/usr/bin/env python3
"""Export a resident query run without overwriting any existing result set."""
import argparse
import json
from pathlib import Path
import shutil
import statistics

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("run", type=Path)
p.add_argument("--export", type=Path, required=True)
p.add_argument("--verification", type=Path)
a = p.parse_args()
a.export.mkdir(parents=True, exist_ok=False)
metadata = json.loads((a.run / "metadata.json").read_text())
samples = json.loads((a.run / "results.json").read_text())
for name in ["metadata.json", "results.json"]: shutil.copy2(a.run / name, a.export / name)
for manifest in a.run.glob("*.u32.json"): shutil.copy2(manifest, a.export / manifest.name)
verification = None
if a.verification:
    shutil.copy2(a.verification, a.export / "verification.json")
    verification = {r["dataset"]: r for r in json.loads(a.verification.read_text())}
summary = []
lines = ["# Resident independent-record queries", "", f"{metadata['settings']['runs']}-run medians of per-run means, not latency percentiles. Setup/CSV/JSON excluded. A one-run result is diagnostic, not stability evidence.", "",
         "| Dataset | Uniform C++ ns | Uniform Rust ns | Ratio | Zipf C++ ns | Zipf Rust ns | Ratio | Size change |", "|---|---:|---:|---:|---:|---:|---:|---:|"]
for dataset, archive in metadata["archives"].items():
    row = {"dataset": dataset, "timings": {}}
    valid = True
    for distribution in metadata["settings"]["distributions"]:
        times = {}
        for backend in ["cpp", "rust"]:
            values = [r for r in samples if r["dataset"] == dataset and r["distribution"] == distribution and r["backend"] == backend]
            success = len(values) == metadata["settings"]["runs"] and all(r["returncode"] == 0 and r["queries"] == metadata["settings"]["queries"] for r in values)
            valid &= success
            if success:
                ns = [r["mean_ns_per_row"] for r in values]
                times[backend] = {"mean_ns_samples": ns, "median_mean_ns": statistics.median(ns), "median_process_peak_rss_kib": statistics.median(r["process_peak_rss_kib"] for r in values)}
        if len(times) == 2: times["speedup"] = times["cpp"]["median_mean_ns"] / times["rust"]["median_mean_ns"]
        row["timings"][distribution] = times
    old_size = sum(v["bytes"] for v in archive["cpp"].values())
    row.update({"old_total_bytes": old_size, "new_total_bytes": archive["rust"]["bytes"], "size_change": archive["rust"]["bytes"] / old_size - 1, "complete_timing_samples": valid})
    if verification:
        v = verification.get(dataset, {})
        row["full_new_validation"] = bool(v.get("byte_exact") and v.get("typed_exit") == 0 and v.get("typed", {}).get("verified_rows") == archive["rows"])
    row["both_distributions_at_least_4x"] = valid and set(row["timings"]) == {"uniform", "zipf"} and all(t.get("speedup", 0) >= 4 for t in row["timings"].values())
    summary.append(row)
    if valid and set(row["timings"]) == {"uniform", "zipf"}:
        u, z = row["timings"]["uniform"], row["timings"]["zipf"]
        lines.append(f"| {dataset} | {u['cpp']['median_mean_ns']:.1f} | {u['rust']['median_mean_ns']:.1f} | {u['speedup']:.2f}x | {z['cpp']['median_mean_ns']:.1f} | {z['rust']['median_mean_ns']:.1f} | {z['speedup']:.2f}x | {row['size_change']:+.1%} |")
(a.export / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
(a.export / "SUMMARY.md").write_text("\n".join(lines) + "\n")
print("\n".join(lines))
