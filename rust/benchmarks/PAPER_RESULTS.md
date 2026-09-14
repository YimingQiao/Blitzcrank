# Paper-data audit and performance expansion — 2026-09-14

Status: experimental implementation and ongoing acceptance, **not a claim of
universal 10x, hardware saturation, or a production-ready default branch**.
The earlier [Census acceptance](RESULTS.md) remains a separate checkpoint.

The user subsequently made independent-record random reads the primary target,
requesting at least 4x across datasets. See the newer
[typed random-read report](RANDOM_ACCESS.md); CSV export figures below do not
satisfy that target. The earlier Census query result is retained, not rewritten
to incorporate the new joint-symbol profile or matched warm-up protocol.

## Coverage and provenance

The paper's [Table 1](https://www.vldb.org/pvldb/vol17/p2528-zhang.pdf) lists:
Corel, Jena Climate, Cars, Forest Cover, US Census 1990, Food, Bimbo, Yale
Languages, Medicare and Arade. The [machine-readable inventory](2026-09-14-paper-inventory.json)
records local file/config SHA-256, bytes, field types and dimensions.

Eight local named datasets match the published row/column dimensions. This is
not independent proof of byte-identical paper inputs: no original paper hashes
were supplied. Two cases remain unresolved:

- `cps.dat` matches Cars' 344,287 x 155 dimensions, but its identity is not
  established. It is reported as **cps, unconfirmed**, not silently renamed Cars.
- Corel's four components have 93 columns in total but LayoutHistogram has
  66,615 rows instead of 68,040. No truncation, padding or positional join was
  performed to manufacture a matching table. The actual merged input is needed.

Medicare uses pipes, not commas. Many tables use CRLF. Numeric-looking INTEGER
fields in Medicare include integral decimal spellings (`19300.000000`), which
the new lexical backend preserves exactly. Source datasets and the original
user-dirty checkout were never modified.

## Comparison contract

- Full files; no training/test row subsampling or favorable row filtering.
- New standalone Rust pipeline versus a freshly built pristine historical
  C++ revision `0ed9c97908c51440b30a2eef3c1b90325dd2c87c`.
  The fresh C++ binary has SHA-256
  `a099892b0be401caa1387cfcf8466b214e9c22430240644a0e3266aa9965baeb`,
  **identical** to the earlier OFF-backend executable. This additionally
  verifies that the earlier control binary did not change its execution code.
- Single process/thread on CPU 2 (Xeon Platinum 8474C), GCC 12.2 O3/NDEBUG,
  Rust 1.92 thin LTO. Shared host, no exclusive CPU/SMT isolation. No AVX-512 in
  this application. Input is page-cache-backed; no fsync/durable-I/O guarantee.
- Three alternating C++/Rust runs per dataset/mode; frozen executables and
  Rust source snapshots. Whole-process encode includes parsing, model work,
  encoding, framing, checksum, output and teardown. Whole-process decode
  includes file/model loading, reconstruction and writing CSV.
- Bulk is C++'s 20,000-interval threshold versus Rust's 256 rows/one state;
  these do **not** have identical block sizes or lookup amplification. Single-
  row mode must be reported separately. All dictionaries, models and indexes
  count toward size (C++ includes `_enum.dat` and `_temp.index`).
- Both use independent columns: C++ `skip_learning=1` still fits parameters;
  conditional structure learning is omitted. The new fixed-scale/string
  codecs are not a port of the original learned numerical/string models.
- Rust must restore every source byte. Historical outputs are checked bytewise,
  then (only for unquoted data) for CRLF-to-LF-only changes, otherwise with CSV
  field comparisons. DOUBLE allowance is configured tolerance plus 0.500001e-6
  for six-decimal CSV rendering; INTEGER values must be exactly integral and
  equal. Changed fields and maximum errors are recorded by the semantic check.
- Validation is outside timing. A failed validation never produces a speedup
  claim. Successful compressed artifacts must also match across repeat runs.

This compares complete implementations, not DC versus rANS or Rust versus C++
in isolation. Changes to modeling, data layout, formatting and indexing matter.
The paper measures resident compressed tuples and OLTP workloads, not this CSV
pipeline. The [shared query traces](QUERY_TRACES.md) cover a separate resident
lookup experiment; they do not reproduce Silo/TPC-C or online insertions.

## A baseline correctness issue that must not be hidden

Historical Medicare decoding fails the supplied tolerance on row 23, column 24
(one-based): `25531.930000` becomes `25531.940000`, error 0.01 against configured
0.0025. This repeats across all three runs. The new backend's full source hash
matches after decoding. No tolerance was relaxed to make the old run pass, and
Medicare is excluded from valid speedup pairs.

The old numerical code stores model quantities in float32 and rounds the
reconstructed result to an inferred number of decimal places. Those are avenues
for diagnosis, **not a proven complete root cause** of this particular error.
The historical control itself has not been patched during this comparison.

## What the current evidence can and cannot establish

The repeated [checkpoint B matrix](results/2026-09-14-matrix-b/SUMMARY.md)
contains all eight valid pairs (seven paper-named datasets plus unconfirmed cps),
with [raw samples](results/2026-09-14-matrix-b/results.json) and
[metadata](results/2026-09-14-matrix-b/metadata.json). It includes fixed-scale
packing, prefix sharing and cached numeric validation, but not the subsequently
added opt-in table builds. The later batch-row agent command does not appear
in that frozen executable. Source snapshots and exact executable hashes are
retained; do not assume rebuilding a moving working tree gives the same hash.

| Dataset | Encode ratio | Decode ratio | Total size change |
|---|---:|---:|---:|
| Jena Climate | 2.37x | 10.56x | -17.7% |
| US Census 1990 | 4.44x | 3.44x | -1.1% |
| Forest Cover | 5.34x | 3.37x | +20.3% |
| cps (identity unconfirmed) | 5.63x | 3.87x | +21.0% |
| Food | 3.90x | 3.03x | +28.9% |
| Yale Languages | 2.63x | 3.24x | +23.3% |
| Arade | 3.08x | 6.51x | +27.4% |
| Bimbo | 3.21x | 4.07x | +15.2% |

All 48 roundtrips in this matrix passed their declared validation, and artifacts
were deterministic across repetitions. A separate
[Medicare run](results/2026-09-14-medicare-b/results.json) with the later default
build passes three byte-exact roundtrips; no valid historical speedup is claimed
there. New encode median 17.20 s, decode 6.60 s, 375,420,308 total bytes.

These are ratios of three-run medians, not statistical confidence bounds. For
Jena, the 0.16–0.17 s decoder range and 0.01 s process-timer resolution straddle
a strict every-run 10x threshold; **10.56x is a median result, not a guaranteed
minimum**. Background development/tests/diagnostics ran on other cores during
parts of the broader session; this was not a quiescent, dedicated host. C++
system time also changes materially between checkpoints. Preserve the raw
user/system/wall samples and reproduce on isolated hardware before publication.

The [earlier checkpoint A](results/2026-09-14-matrix-a/SUMMARY.md) records the
larger pre-prefix Arade result; it is retained rather than overwritten.

### Final serial cross-check of the current default executable

After development tests and other performance diagnostics stopped, the current
default binary was tested again on Jena, Census and Arade, three alternating
runs each. No other benchmark/test process was launched concurrently by this
agent; the host itself was still not exclusively reserved. All 18 full-file
roundtrips passed. [Serial results](results/2026-09-14-serial-check/SUMMARY.md)
and [exact current-binary metadata](results/2026-09-14-serial-check/metadata.json):

| Dataset | Encode ratio | Decode ratio | Size change |
|---|---:|---:|---:|
| Jena Climate | 2.43x | 11.20x | -17.7% |
| US Census 1990 | 4.60x | 3.40x | -1.1% |
| Arade | 3.23x | 6.36x | +27.4% |

Ratios remain three-run medians with the same coarse wall timer. This supports
the operation-specific gain and the continuing shortfall in general 10x, not
a hardware saturation claim. The full eight-pair table above remains checkpoint
B rather than silently replacing its three overlapping rows with newer trials.

Jena's CSV gain includes representation/formatting changes: the new backend
returns exact decimal lexemes, while the old pipeline reconstructs doubles and
formats them. Numeric consumers that need typed floating-point values require
a separate benchmark including that conversion. CSV roundtrip equality does
not establish 11x typed OLTP numerical query speed or a DC-over-rANS advantage.

The first repeated expanded checkpoint crosses 10x for **Jena CSV decoding**:
1.67 s versus 0.16 s (all three new runs 0.16 s at the timer's 0.01 s resolution),
with 17.7% fewer total bytes. Encoding at that checkpoint is only 1.64x faster.
It is one favorable operation, not general 10x acceptance or entropy-only speed.

Other datasets expose size regressions and smaller speedups. The initial
Arade lexical-byte prototype used 313,127,802 bytes; exact fixed-scale packing
reduced this to 255,329,235, then block-local prefix sharing to 173,879,366 in a
full byte-exact diagnostic. The latter still exceeds historical 136,488,236.
Do not combine timings from these distinct checkpoints as if one binary ran
the whole matrix. Final repeated results and raw manifests must identify the
specific checkpoint/configuration used.

There is **no proof that general 10x is impossible**. A useful narrower bound is:
if parsing/output stay unchanged, even a zero-cost entropy kernel cannot reduce
elapsed time below those stages. For example, the prefix Arade diagnostic spends
about 5.22 s in parsing/counting/conversion alone, already above a roughly 2 s
budget for 10x historical compression. Achieving that target therefore requires
further outer-pipeline changes, not just SIMD in DC. This conditional bound is
not a hardware limit; the profile still contains substantial avoidable work.
The [per-dataset 10x budgets](results/2026-09-14-matrix-b/budget.json) explicitly
remove the **whole model/encode/CRC stage**, an even more generous assumption
than a free entropy kernel. It still leaves most datasets below 10x. This tells
us which stages must change, not that future algorithms cannot reach the target.

## Independent records and resident queries

The [full record-mode smoke matrix](results/2026-09-14-record-smoke/SUMMARY.md)
uses the later default build, with one full run per backend on all eight valid
pair candidates. All 16 roundtrips passed. This is correctness/coverage smoke
testing, **not** three-run stability evidence; the table's ratios must be read
with that limitation. Medicare additionally passes a
[byte-exact record roundtrip](results/2026-09-14-medicare-record/results.json).
Food's record archive is 4.2% smaller than historical, whereas Arade's is
65.7% larger and Forest's 31.5% larger. Bulk prefix-sharing gains must not be
advertised as if they applied to independently encoded records.

On resident Census record archives, each backend received the same one-million-
row trace, with three alternating runs, on CPU 2:

| Trace | C++ median mean ns/row | Rust median mean ns/row | Ratio |
|---|---:|---:|---:|
| Uniform xorshift64 | 1496.12 | 710.116 | 2.107x |
| Finite Zipf, theta 0.99, unscrambled | 1291.06 | 647.626 | 1.993x |

[Raw query samples](results/2026-09-14-query-traces/results.json),
[executable/archive hashes](results/2026-09-14-query-traces/metadata.json), and
trace manifests preserve the exact comparison. The tested helper predates the
later CLI batch-row command; its model/index setup, CRC, file loading and query
generation are outside the timer. The data archives are the earlier verified
Census record files, readable by the new code. This confirms a resident-query
gain, **not 10x query latency**, and includes indexing changes as well as DC.

## Optional lookup-table experiment

Direct tables are **not enabled in the acceptance defaults**. A full Census
diagnostic on CPU 4 produced an archive byte-identical to checkpoint B, but was
slower: approximately 6.99 s to parse/encode/write with encode tables and
7.17 s to open/decode/write with both tables. The compact-path reference was
measured separately on CPU 2, so this is not a calibrated speedup comparison;
it is a clear reason not to promote the option without proper workload-specific
tests. Models add 128 KiB for encoding and 512 KiB for decoding, each. More
lookup memory is not automatically faster for a many-model workload.

## Reproduction and release gates

The current default and all-feature Release suites pass 18 tests; strict Clippy
on all targets/features passes. The declared Rust 1.88 toolchain passes the
full Release suite before the final additive CLI command, checks all features,
and passes the final CLI tests (including batch-row bounds and response quota).
Debug also passed the full suite at the preceding checkpoint. Coverage includes
lexical decimals/negative zero, quotes/multiline/pipes, high-cardinality fallback,
fixed-scale extremes, prefix reconstruction, corrupt/truncated containers,
checksum-repaired mutations, entropy final states and atomic output failures.
This is not a completed security audit or a production storage-engine release.

Use `paper_matrix.py --help` and `summarize_matrix.py SCRATCH --export NEW_DIR`.
The scripts retain raw failures, source/binary hashes, timings, RSS and sizes.
Do not infer acceptance from a single fastest sample. Published summaries should
show the size tradeoff and operation-specific speedups, not only a geometric mean.

Remaining release gates include confirmed Cars/Corel inputs, robust record-mode
coverage, broader performance/ratio work, portable pinned dependency checkout,
and agreement on the embryo destination. No repository was pushed, transferred,
or made the default branch as part of this still-incomplete acceptance work.
