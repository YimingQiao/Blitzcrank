#!/usr/bin/env python3
"""Full exact CSV hash + every typed row; no decoding benchmark is timed here."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--root", type=Path, required=True)
p.add_argument("--reader", type=Path, required=True)
p.add_argument("--csv", type=Path, required=True)
p.add_argument("--datasets", nargs="+", required=True)
p.add_argument("--reader-args", nargs="*", default=[])
a = p.parse_args()
scratch = Path(tempfile.mkdtemp(prefix="blitzcrank-record-verify."))
print(scratch, flush=True)
trace = scratch / "row-zero.u32"
trace.write_bytes(bytes(4))
results = []
for dataset in a.datasets:
    directory = a.root.resolve() / f"{dataset}-record-rust-1"
    archive = directory / "payload.bin"
    result = subprocess.run([str(a.reader.resolve()), str(archive), str(trace), "verify-all", *a.reader_args], text=True, capture_output=True, timeout=1800)
    record = {"dataset": dataset, "typed_exit": result.returncode, "typed": json.loads(result.stdout) if result.returncode == 0 else result.stderr}
    decode = subprocess.Popen([str(a.csv.resolve()), str(archive)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    h = hashlib.sha256()
    for block in iter(lambda: decode.stdout.read(1 << 20), b""): h.update(block)
    decode.stdout.close()
    error = decode.stderr.read().decode(); decode.stderr.close()
    record["csv_exit"] = decode.wait()
    source = hashlib.sha256()
    with (directory / "input.csv").open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""): source.update(block)
    record.update({"source_sha256": source.hexdigest(), "decoded_sha256": h.hexdigest(), "byte_exact": h.digest() == source.digest() and decode.returncode == 0, "decode_error": error})
    results.append(record)
    (scratch / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(record), flush=True)
    if not record["byte_exact"] or result.returncode != 0: raise SystemExit(1)
