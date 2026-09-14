#!/usr/bin/env python3
"""Full-file, single-core CLI comparison; not the paper's resident YCSB test.

All timings include process start, parsing/model training, serialization and I/O.
Validation runs outside timing. Rust must restore exact input bytes. C++ may
normalize CSV spelling and quantize DOUBLE within the supplied schema tolerance.
Failures are retained, never converted into speedups. No source data is modified.
"""
import argparse
import csv
from decimal import Decimal, InvalidOperation
import hashlib
import itertools
import json
import math
import os
import re
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        while data := f.read(8 << 20):
            h.update(data)
    return h.hexdigest()


def unquoted_lf_digest(path):
    """Fast C++ line-ending-only check; never normalize quoted field contents."""
    h = hashlib.sha256()
    pending = b""
    with path.open("rb") as f:
        while chunk := f.read(8 << 20):
            if b'"' in chunk:
                return None
            chunk = pending + chunk
            pending = chunk[-1:] if chunk.endswith(b"\r") else b""
            if pending:
                chunk = chunk[:-1]
            h.update(chunk.replace(b"\r\n", b"\n"))
    h.update(pending)
    return h.hexdigest()


def semantic(source, restored, config, delimiter):
    schema = [s.split() for s in config.read_text().splitlines() if s.strip()]
    maximum = [0.0] * len(schema)
    changed = [0] * len(schema)
    sentinel = object()
    rows = 0
    # surrogateescape retains arbitrary non-UTF8 bytes rather than replacing them.
    with source.open(newline="", errors="surrogateescape") as a, restored.open(newline="", errors="surrogateescape") as b:
        for rows, (left, right) in enumerate(itertools.zip_longest(csv.reader(a, delimiter=delimiter), csv.reader(b, delimiter=delimiter), fillvalue=sentinel), 1):
            if left is sentinel or right is sentinel or len(left) != len(schema) or len(right) != len(schema):
                raise ValueError(f"row {rows}: row/column count mismatch")
            for c, (x, y, field) in enumerate(zip(left, right, schema)):
                if x == y:
                    continue
                changed[c] += 1
                if field[0] == "DOUBLE":
                    error = abs(float(x) - float(y))
                    # Historical CLI std::to_string renders six decimal digits.
                    allowed = float(field[1]) + 0.500001e-6
                    if not math.isfinite(error) or error > allowed:
                        raise ValueError(f"row {rows}, column {c}: numeric error {error} > {allowed}")
                    maximum[c] = max(maximum[c], error)
                elif field[0] == "INTEGER" and Decimal(x) == Decimal(y) and Decimal(x) == Decimal(x).to_integral_value():
                    pass
                else:
                    raise ValueError(f"row {rows}, column {c}: nonnumeric value mismatch")
    return {"kind": "schema_semantic", "rows": rows, "changed_fields_by_column": changed, "max_abs_error_by_column": maximum,
            "double_allowance": "schema tolerance + 0.500001e-6 for historical six-decimal CSV rendering"}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--data-root", type=Path, required=True)
    p.add_argument("--cpp", type=Path, required=True)
    p.add_argument("--rust", type=Path, required=True)
    p.add_argument("--datasets", nargs="+", required=True)
    p.add_argument("--modes", nargs="+", choices=["bulk", "record"], default=["bulk", "record"])
    p.add_argument("--backends", nargs="+", choices=["cpp", "rust"], default=["cpp", "rust"])
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--cpu", type=int, default=2)
    p.add_argument("--states", type=int, choices=[1, 4], default=1)
    p.add_argument("--timeout", type=int, default=1800)
    args = p.parse_args()
    if not 1 <= args.runs <= 100 or args.cpu < 0 or args.timeout <= 0:
        p.error("expected 1..100 runs, nonnegative CPU and positive timeout")
    if any(not re.fullmatch(r"[A-Za-z0-9_]+", name) for name in args.datasets):
        p.error("datasets must be simple file stems (letters, digits and underscores)")
    if any(len(values) != len(set(values)) for values in [args.datasets, args.modes, args.backends]):
        p.error("duplicate dataset/mode/backend")
    scratch = Path(tempfile.mkdtemp(prefix="blitzcrank-paper-matrix."))
    print(f"Results: {scratch}", flush=True)
    source = Path(__file__).resolve().parents[1]
    shutil.copytree(source, scratch / "source" / "Blitzcrank", ignore=shutil.ignore_patterns(".git", "target", "__pycache__"))
    cargo = json.loads(subprocess.check_output(["cargo", "metadata", "--manifest-path", str(source / "Cargo.toml"), "--format-version", "1", "--locked"], text=True))
    core = Path(next(p["manifest_path"] for p in cargo["packages"] if p["name"] == "delayed-coding")).parent
    shutil.copytree(core, scratch / "source" / "delayed-coding",
                    ignore=shutil.ignore_patterns("target", "target-*", "build", "build-*", ".git", "benchmarks", "__pycache__"))
    # Freeze executables: an ongoing optimization must not alter this matrix.
    executables = {}
    for name in args.backends:
        src = getattr(args, name).resolve()
        dest = scratch / name
        shutil.copy2(src, dest)
        executables[name] = dest
    metadata = {"cpu": args.cpu, "runs": args.runs, "states": args.states,
                "rust_source_snapshot": str(scratch / "source"),
                "measurement": "single-core full CLI wall time; warm page cache; no fsync; verification outside timing",
                "cpp_skip_conditional_learning": True,
                "executables": {k: {"original": str(getattr(args, k).resolve()), "sha256": digest(v)} for k, v in executables.items()}, "datasets": {}}
    if "rust" in executables:
        try:
            metadata["rust_capabilities"] = json.loads(subprocess.check_output([str(executables["rust"]), "capabilities", "--json"], text=True, stderr=subprocess.DEVNULL))["result"]
        except (subprocess.CalledProcessError, ValueError, KeyError):
            metadata["rust_capabilities"] = "unavailable in this executable"
    results = []
    for stem in args.datasets:
        data = (args.data_root / "tables" / f"{stem}.dat").resolve()
        config = (args.data_root / "config" / f"{stem}.config").resolve()
        delimiter = "|" if stem == "Medicare1_1" else ","
        metadata["datasets"][stem] = {"source": str(data), "sha256": digest(data), "bytes": data.stat().st_size,
                                       "unquoted_lf_sha256": unquoted_lf_digest(data),
                                       "config": str(config), "config_sha256": digest(config), "delimiter": delimiter,
                                       "paper_identity": "UNCONFIRMED" if stem == "cps" else "see paper_inventory.json"}
        (scratch / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        for mode in args.modes:
            for run in range(1, args.runs + 1):
                for backend in args.backends if run % 2 else reversed(args.backends):
                    work = scratch / f"{stem}-{mode}-{backend}-{run}"
                    work.mkdir()
                    (work / "input.csv").symlink_to(data)
                    (work / "schema").symlink_to(config)
                    exe = str(executables[backend])
                    if backend == "cpp":
                        block = "20000" if mode == "bulk" else "1"
                        flag = "1" if delimiter == "|" else "0"
                        commands = {"encode": [exe, "-c", "input.csv", "payload.bin", "schema", flag, "1", block],
                                    "decode": [exe, "-d", "payload.bin", "restored.csv", "schema", flag, block]}
                    else:
                        block = "256" if mode == "bulk" else "1"
                        flag = "--delimiter=pipe" if delimiter == "|" else "--delimiter=comma"
                        commands = {"encode": [exe, "compress", "input.csv", "schema", "payload.bin", block, str(args.states), flag, "--json"],
                                    "decode": [exe, "decompress", "payload.bin", "restored.csv", "--json"]}
                    result = {"dataset": stem, "mode": mode, "backend": backend, "run": run, "status": "failed", "work": str(work), "phases": {}}
                    try:
                        for phase, command in commands.items():
                            timefile = work / f"{phase}.time"
                            with (work / f"{phase}.log").open("wb") as log:
                                proc = subprocess.Popen(["/usr/bin/time", "-f", "%e,%U,%S,%M", "-o", str(timefile), "taskset", "-c", str(args.cpu), *command], cwd=work, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                                try:
                                    proc.wait(timeout=args.timeout)
                                except subprocess.TimeoutExpired:
                                    # Kill only this invocation's process group,
                                    # including the timed child, not just `time`.
                                    os.killpg(proc.pid, signal.SIGKILL)
                                    proc.wait()
                                    raise
                            if proc.returncode:
                                raise ValueError(f"{phase} exit {proc.returncode}; see {phase}.log")
                            wall, user, system, rss = timefile.read_text().strip().split(",")
                            result["phases"][phase] = {"wall_s": float(wall), "user_s": float(user), "system_s": float(system), "max_rss_kib": int(rss)}
                        restored = work / "restored.csv"
                        restored_hash = digest(restored)
                        result["restored_sha256"] = restored_hash
                        artifacts = ["payload.bin"] + (["_enum.dat", "_temp.index"] if backend == "cpp" else [])
                        result["artifacts"] = {f: {"bytes": (work / f).stat().st_size, "sha256": digest(work / f)} for f in artifacts}
                        result["total_compressed_bytes"] = sum(v["bytes"] for v in result["artifacts"].values())
                        if restored_hash == metadata["datasets"][stem]["sha256"]:
                            result["validation"] = {"kind": "byte_exact", "sha256": restored_hash}
                        elif backend == "rust":
                            raise ValueError("Rust output is not byte-exact")
                        elif metadata["datasets"][stem]["unquoted_lf_sha256"] and restored_hash == metadata["datasets"][stem]["unquoted_lf_sha256"]:
                            result["validation"] = {"kind": "CRLF_to_LF_only_unquoted", "sha256": restored_hash}
                        else:
                            result["validation"] = semantic(data, restored, config, delimiter)
                        result["status"] = "passed"
                    except (ValueError, InvalidOperation, OSError, subprocess.TimeoutExpired) as error:
                        result["error"] = str(error)
                    results.append(result)
                    (scratch / "results.json").write_text(json.dumps(results, indent=2) + "\n")
                    print(json.dumps({k: result[k] for k in ("dataset", "mode", "backend", "run", "status", "phases")}, separators=(",", ":")), flush=True)
                    if "error" in result:
                        print(result["error"], flush=True)
    print(f"Complete: {scratch / 'results.json'}", flush=True)


if __name__ == "__main__":
    main()
