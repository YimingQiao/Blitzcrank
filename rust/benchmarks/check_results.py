"""Check the >=2x throughput target for both full-file phases and both modes.

Usage: python3 check_results.py /path/to/results.csv
No third-party packages. Input comes from e2e.sh (which checks restored bytes).
"""
import csv
import statistics
import sys

with open(sys.argv[1], newline="", encoding="utf-8") as source:
    rows = list(csv.DictReader(source))
assert len(rows) == 24, "expected all 24 phase measurements"
for mode in ("bulk", "record"):
    for phase in ("encode", "decode"):
        medians = {}
        for backend in ("cpp", "rust"):
            selected = [r for r in rows if (r["mode"], r["phase"], r["backend"]) == (mode, phase, backend)]
            assert sorted(r["run"] for r in selected) == ["1", "2", "3"]
            samples = [float(r["wall_s"]) for r in selected]
            assert all(t > 0 for t in samples)
            medians[backend] = statistics.median(samples)
        speedup = medians["cpp"] / medians["rust"]
        print(f'{mode:6} {phase:6}: C++={medians["cpp"]:.2f}s Rust={medians["rust"]:.2f}s speedup={speedup:.3f}x')
        assert speedup >= 2, f"2x target not met for {mode}/{phase}"
print("PASS: >=2x throughput in all four end-to-end comparisons")
