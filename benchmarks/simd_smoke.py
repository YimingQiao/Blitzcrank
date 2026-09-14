#!/usr/bin/env python3
"""Explicit bulk profile interoperability; one-run wall times are diagnostic.

Read complete source files. Compare portable and AVX-enabled binaries on the
same cumulative/64 format, plus ordinary general-mode bulk at the same block
size. No random-access claim. Never overwrite source or previous artifacts.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as src:
        for data in iter(lambda: src.read(1 << 20), b""):
            h.update(data)
    return h.hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-root", type=Path, required=True)
    p.add_argument("--portable", type=Path, required=True)
    p.add_argument("--avx512", type=Path, required=True)
    p.add_argument("--datasets", nargs="+", required=True)
    p.add_argument("--cpu", type=int, default=2)
    a = p.parse_args()
    if a.cpu < 0 or len(set(a.datasets)) != len(a.datasets) or any(not re.fullmatch(r"[A-Za-z0-9_]+", s) for s in a.datasets):
        p.error("invalid settings")
    scratch = Path(tempfile.mkdtemp(prefix="blitzcrank-simd-release."))
    print(scratch, flush=True)
    helpers = {}
    for name in ["portable", "avx512"]:
        helpers[name] = scratch / name
        shutil.copy2(getattr(a, name), helpers[name])
    metadata = {"cpu": a.cpu, "runs": 1, "block_rows": 4096, "precision": 16,
                "measurement": "diagnostic full-process wall times, not resident queries or acceptance ratios",
                "helpers": {k: {"sha256": sha(v), "capabilities": json.loads(subprocess.check_output([str(v), "capabilities", "--json"], text=True))} for k, v in helpers.items()}}
    (scratch / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rows = []
    for dataset in a.datasets:
        source = (a.data_root / "tables" / f"{dataset}.dat").resolve()
        schema = (a.data_root / "config" / f"{dataset}.config").resolve()
        source_hash = sha(source)
        encoded_hashes = {}
        for backend, command in [("portable", "compress-simd"), ("avx512", "compress-simd"), ("avx512", "compress")]:
            work = scratch / f"{dataset}-{backend}-{command}"
            work.mkdir()
            archive, restored = work / "data.bcr", work / "restored.csv"
            encoder = helpers[backend]
            decoder = helpers["avx512" if backend == "portable" else "portable"] if command == "compress-simd" else encoder
            delimiter = "--delimiter=pipe" if dataset.startswith("Medicare") else "--delimiter=comma"
            args = [str(encoder), command, str(source), str(schema), str(archive), "4096", delimiter, "--general", "--json"]
            begin = time.perf_counter()
            encoded = subprocess.check_output(["taskset", "-c", str(a.cpu), *args], text=True)
            encode_s = time.perf_counter() - begin
            begin = time.perf_counter()
            decoded = subprocess.check_output(["taskset", "-c", str(a.cpu), str(decoder), "decompress", str(archive), str(restored), "--json"], text=True)
            decode_s = time.perf_counter() - begin
            row = {"dataset": dataset, "backend": backend, "profile": command, "decoder": decoder.name,
                   "source_sha256": source_hash, "schema_sha256": sha(schema), "archive_sha256": sha(archive),
                   "restored_sha256": sha(restored), "bytes": archive.stat().st_size, "encode_s": encode_s,
                   "decode_s": decode_s, "encode": json.loads(encoded), "decode": json.loads(decoded)}
            rows.append(row)
            (scratch / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
            print(json.dumps(row), flush=True)
            if row["restored_sha256"] != source_hash:
                raise SystemExit("roundtrip mismatch")
            if command == "compress-simd":
                encoded_hashes[backend] = row["archive_sha256"]
        if encoded_hashes["portable"] != encoded_hashes["avx512"]:
            raise SystemExit("portable/SIMD file mismatch")


if __name__ == "__main__":
    main()
