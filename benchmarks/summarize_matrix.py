#!/usr/bin/env python3
"""Export a completed matrix's audit artifacts and summarize only valid pairs."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import shutil
import statistics

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("scratch", type=Path)
p.add_argument("--export", type=Path)
args = p.parse_args()
metadata = json.loads((args.scratch / "metadata.json").read_text())
results = json.loads((args.scratch / "results.json").read_text())
groups = defaultdict(list)
for row in results:
    groups[(row["dataset"], row["mode"], row["backend"])].append(row)

lines = ["| Dataset / mode | Runs C++ / Rust | Encode speedup | Decode speedup | Rust size vs C++ | Status |",
         "|---|---:|---:|---:|---:|---|"]
summary = []
for dataset, mode in dict.fromkeys((r["dataset"], r["mode"]) for r in results):
    a, b = groups[(dataset, mode, "cpp")], groups[(dataset, mode, "rust")]
    item = {"dataset": dataset, "mode": mode, "counts": {"cpp": len(a), "rust": len(b)}}
    valid = len(a) == len(b) == metadata["runs"] and all(r["status"] == "passed" for r in a + b)
    # Randomized hash maps must not make compressed representation nondeterministic.
    deterministic = all(len({json.dumps(r.get("artifacts"), sort_keys=True) for r in group}) == 1 for group in (a, b))
    if valid and deterministic:
        item["status"] = "passed"
        item["timings"] = {}
        for backend, group in [("cpp", a), ("rust", b)]:
            item["timings"][backend] = {}
            for phase in ["encode", "decode"]:
                values = [r["phases"][phase]["wall_s"] for r in group]
                item["timings"][backend][phase] = {"min_s": min(values), "median_s": statistics.median(values), "max_s": max(values),
                    "median_max_rss_kib": statistics.median(r["phases"][phase]["max_rss_kib"] for r in group)}
        speedups = {phase: item["timings"]["cpp"][phase]["median_s"] / item["timings"]["rust"][phase]["median_s"] for phase in ["encode", "decode"]}
        size = b[0]["total_compressed_bytes"] / a[0]["total_compressed_bytes"] - 1
        item.update(speedups=speedups, size_change_fraction=size, total_bytes={"cpp": a[0]["total_compressed_bytes"], "rust": b[0]["total_compressed_bytes"]})
        lines.append(f"| {dataset} / {mode} | {len(a)} / {len(b)} | {speedups['encode']:.2f}x | {speedups['decode']:.2f}x | {size:+.1%} | verified |")
    else:
        item["status"] = "incomplete_or_failed"
        item["failures"] = [r.get("error", "unknown") for r in a + b if r["status"] != "passed"]
        lines.append(f"| {dataset} / {mode} | {len(a)} / {len(b)} | — | — | — | incomplete / invalid pair |")
    summary.append(item)
report = "\n".join(lines) + "\n"
print(report)
if args.export:
    # New directory only: don't accidentally overwrite another experiment.
    args.export.mkdir(parents=True, exist_ok=False)
    for name in ["metadata.json", "results.json"]:
        shutil.copy2(args.scratch / name, args.export / name)
    (args.export / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    (args.export / "SUMMARY.md").write_text(report)
