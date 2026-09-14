#!/usr/bin/env python3
"""Alternate matched resident-query helpers; traces must already be generated."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--cpp", type=Path, required=True)
p.add_argument("--rust", type=Path, required=True)
p.add_argument("--cpp-dir", type=Path, required=True)
p.add_argument("--schema", default="schema")
p.add_argument("--delimiter-flag", choices=["0", "1"], default="0")
p.add_argument("--archive", type=Path, required=True)
p.add_argument("--traces", type=Path, nargs="+", required=True)
p.add_argument("--cpu", type=int, default=2)
a = p.parse_args()
scratch = Path(tempfile.mkdtemp(prefix="blitzcrank-query-matrix."))
print(scratch, flush=True)
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
exes = {}
for name in ["cpp", "rust"]:
    exes[name] = scratch / name
    shutil.copy2(getattr(a, name), exes[name])
metadata = {"cpu": a.cpu, "executables": {k: sha(v) for k,v in exes.items()},
            "archive": {"path": str(a.archive.resolve()), "sha256": sha(a.archive)},
            "cpp_dir": str(a.cpp_dir.resolve()), "traces": {str(t.resolve()): sha(t) for t in a.traces},
            "cpp_artifacts": {f: sha(a.cpp_dir / f) for f in ["payload.bin", "_enum.dat", "_temp.index"]}}
(scratch / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
rows = []
for trace in a.traces:
    for run in range(1,4):
        for backend in ["cpp", "rust"] if run % 2 else ["rust", "cpp"]:
            if backend == "cpp":
                command = [str(exes[backend]), str(trace.resolve()), a.schema, a.delimiter_flag]
                cwd = a.cpp_dir
            else:
                command = [str(exes[backend]), str(a.archive.resolve()), str(trace.resolve())]
                cwd = scratch
            result = subprocess.run(["taskset", "-c", str(a.cpu), *command], cwd=cwd, capture_output=True, text=True, check=True)
            (scratch / f"{trace.stem}-{backend}-{run}.log").write_text(result.stdout + result.stderr)
            if backend == "rust":
                parsed = json.loads(result.stdout)
                mean, queries = parsed["mean_ns_per_row"], parsed["queries"]
            else:
                mean = float(re.search(r"mean_ns_per_row=([0-9.e+-]+)", result.stdout)[1])
                queries = int(re.search(r"queries=([0-9]+)", result.stdout)[1])
            row = {"trace": trace.name, "backend": backend, "run": run, "queries": queries, "mean_ns_per_row": mean}
            rows.append(row)
            (scratch / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
            print(json.dumps(row), flush=True)
