#!/usr/bin/env python3
"""Conditional 10x budget, not a hardware/impossibility proof.

Optimistically remove the entire model/entropy/CRC stage while holding parsing,
output and other measured process work fixed. This is even more generous than
a zero-cost DC kernel. The estimate uses coarse process wall times, not cycles.
"""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import statistics

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("scratch", type=Path)
p.add_argument("--output", type=Path)
a = p.parse_args()
results = json.loads((a.scratch / "results.json").read_text())
groups = defaultdict(list)
for row in results:
    groups[(row["dataset"], row["mode"], row["backend"])].append(row)
out = []
for dataset, mode in dict.fromkeys((r["dataset"], r["mode"]) for r in results):
    old, new = groups[(dataset, mode, "cpp")], groups[(dataset, mode, "rust")]
    if not old or not new or any(r["status"] != "passed" for r in old + new):
        continue
    old_s = statistics.median(r["phases"]["encode"]["wall_s"] for r in old)
    stages = [json.loads((Path(r["work"]) / "encode.log").read_text())["result"]["timings_s"] for r in new]
    parses = [s["parse_count"] for s in stages]
    # Exclude all measured model, entropy and CRC work, but not teardown.
    residuals = [max(s["parse_count"] + s["write_commit"], r["phases"]["encode"]["wall_s"] - s["model_encode_crc"]) for r,s in zip(new, stages)]
    floor = statistics.median(residuals)
    out.append({"dataset": dataset, "mode": mode, "old_encode_median_s": old_s,
                "target_10x_budget_s": old_s / 10, "parse_median_s": statistics.median(parses),
                "residual_if_model_encode_crc_free_s": floor,
                "optimistic_speedup_with_other_stages_unchanged": old_s / floor,
                "scope": "conditional current-stage budget; not a physical lower bound or proof against future algorithms"})
print(json.dumps(out, indent=2))
if a.output:
    with a.output.open("x") as dest:
        json.dump(out, dest, indent=2)
