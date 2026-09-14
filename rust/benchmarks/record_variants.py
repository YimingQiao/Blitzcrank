#!/usr/bin/env python3
"""Build independent four-state variants from existing validated source inputs."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--root", type=Path, required=True)
p.add_argument("--encoder", type=Path, required=True)
p.add_argument("--datasets", nargs="+", required=True)
p.add_argument("--precision", type=int)
p.add_argument("--lanes", type=int, choices=[1, 4], default=4)
p.add_argument("--chunks", action="store_true")
a = p.parse_args()
scratch = Path(tempfile.mkdtemp(prefix="blitzcrank-record4."))
print(scratch, flush=True)
encoder = scratch / "encoder"
shutil.copy2(a.encoder, encoder)
for dataset in a.datasets:
    if not dataset.replace("_", "").isalnum(): p.error("invalid dataset")
    original = a.root.resolve() / f"{dataset}-record-rust-1"
    target = scratch / f"{dataset}-record-rust-1"
    target.mkdir()
    for name in ["input.csv", "schema"]: (target / name).symlink_to((original / name).resolve())
    (scratch / f"{dataset}-record-cpp-1").symlink_to(a.root.resolve() / f"{dataset}-record-cpp-1", target_is_directory=True)
    if a.precision is None:
        command = [str(encoder), "compress", "input.csv", "schema", "payload.bin", "1", str(a.lanes), "--json"]
        if dataset.startswith("Medicare"): command.append("--delimiter=pipe")
    else:
        command = [str(encoder), "input.csv", "schema", "payload.bin", str(a.lanes), str(a.precision), "pipe" if dataset.startswith("Medicare") else "comma"]
        if a.chunks: command.append("chunks")
    result = subprocess.run(command, cwd=target, text=True, capture_output=True, check=True)
    (target / "encode.json").write_text(result.stdout)
    print(json.dumps({"dataset": dataset, "bytes": (target / "payload.bin").stat().st_size}), flush=True)
