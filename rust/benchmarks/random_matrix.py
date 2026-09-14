#!/usr/bin/env python3
"""Resident independent-record queries. Freeze helpers; never overwrite a run.

Archive directories must have passed full-file roundtrip validation beforehand.
This runner is not itself that validation. All query IDs are generated before
timing. Setup, CRC and trace loading are excluded from helper query timers.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, required=True)
    p.add_argument("--cpp", type=Path, required=True)
    p.add_argument("--rust", type=Path, required=True)
    p.add_argument("--datasets", nargs="+", default=["jena_climate", "covtype", "USCensus1990", "Food", "Bimbo_1", "YaleLanguages", "Arade_1", "cps"])
    p.add_argument("--queries", type=int, default=1000000)
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--cpu", type=int, default=2)
    p.add_argument("--distributions", nargs="+", choices=["uniform", "zipf"], default=["uniform", "zipf"])
    p.add_argument("--rust-args", nargs="*", default=[])
    a = p.parse_args()
    if not 1 <= a.runs <= 100 or a.cpu < 0 or not 1 <= a.queries <= 10000000 or any(not re.fullmatch(r"[A-Za-z0-9_]+", d) for d in a.datasets):
        p.error("invalid run settings")
    if len(set(a.datasets)) != len(a.datasets) or len(set(a.distributions)) != len(a.distributions):
        p.error("duplicate datasets or distributions")
    root = a.root.resolve()
    scratch = Path(tempfile.mkdtemp(prefix="blitzcrank-random-matrix."))
    print(scratch, flush=True)
    helpers = {}
    for name in ["cpp", "rust"]:
        helpers[name] = scratch / name
        shutil.copy2(getattr(a, name), helpers[name])
    metadata = {"settings": {k: str(v) if isinstance(v, Path) else v for k, v in vars(a).items()}, "executables": {k: sha(v) for k, v in helpers.items()}, "archives": {}, "host": subprocess.check_output(["lscpu"], text=True)}
    traces = {}
    for dataset in a.datasets:
        rd = root / f"{dataset}-record-rust-1"
        cd = root / f"{dataset}-record-cpp-1"
        with (rd / "payload.bin").open("rb") as f:
            header = f.read(30)
        n = int.from_bytes(header[15:19] if header[:8] == b"BLTZRS02" else header[14:18], "little")
        metadata["archives"][dataset] = {"rows": n, "rust": {"sha256": sha(rd / "payload.bin"), "bytes": (rd / "payload.bin").stat().st_size}, "cpp": {f: {"sha256": sha(cd / f), "bytes": (cd / f).stat().st_size} for f in ["payload.bin", "_enum.dat", "_temp.index"]}, "schema_sha256": sha(cd / "schema")}
        for distribution in a.distributions:
            trace = scratch / f"{dataset}-{distribution}.u32"
            subprocess.run(["python3", str(Path(__file__).with_name("query_trace.py")), str(n), str(trace), "--queries", str(a.queries), "--distribution", distribution], check=True, stdout=subprocess.DEVNULL)
            traces[dataset, distribution] = trace
    (scratch / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    results = []
    for dataset in a.datasets:
        for distribution in a.distributions:
            trace = traces[dataset, distribution]
            for run in range(1, a.runs + 1):
                for backend in ["cpp", "rust"] if run % 2 else ["rust", "cpp"]:
                    directory = root / f"{dataset}-record-{backend}-1"
                    if backend == "cpp":
                        cmd = [str(helpers[backend]), str(trace), "schema", "1" if dataset.startswith("Medicare") else "0"]
                    else:
                        cmd = [str(helpers[backend]), str(directory / "payload.bin"), str(trace), *a.rust_args]
                    log = scratch / f"{dataset}-{distribution}-{backend}-{run}"
                    result = subprocess.run(["/usr/bin/time", "-f", "%M", "-o", str(log) + ".rss", "taskset", "-c", str(a.cpu), *cmd], cwd=directory, text=True, capture_output=True, timeout=600)
                    Path(str(log) + ".log").write_text(result.stdout + result.stderr)
                    row = {"dataset": dataset, "distribution": distribution, "run": run, "backend": backend, "returncode": result.returncode}
                    if result.returncode == 0:
                        if backend == "rust":
                            row.update(json.loads(result.stdout))
                        else:
                            row["mean_ns_per_row"] = float(re.search(r"mean_ns_per_row=([0-9.e+-]+)", result.stdout)[1])
                            row["queries"] = int(re.search(r"queries=([0-9]+)", result.stdout)[1])
                        row["process_peak_rss_kib"] = int(Path(str(log) + ".rss").read_text().strip())
                    else:
                        row["error"] = result.stderr
                    results.append(row)
                    (scratch / "results.json").write_text(json.dumps(results, indent=2) + "\n")
                    print(json.dumps(row), flush=True)
    if any(row["returncode"] != 0 for row in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
