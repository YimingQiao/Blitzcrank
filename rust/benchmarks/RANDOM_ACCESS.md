# Typed independent-record random access — 2026-09-14

For the new **balanced default**, start with the
[release measurements](results/2026-09-14-balanced-release/SUMMARY.md).
The historical joint-profile report below is a separate optional tradeoff.

**Release-policy update:** the table below measures the explicit **joint**
profile, not the new balanced default. Reproduce its CLI representation with
`compress-records INPUT SCHEMA OUTPUT 1 16 joint`. The preview default preserves
independent fields; it does not inherit these joint-profile speedup claims.
The optional SIMD profile is bulk-only and is not part of these timings.

**The requested general 4x target is not yet met.** Census and Forest exceed
4x for both tested distributions; the unconfirmed cps dataset also does. Food
exceeds 4x for uniform queries but not Zipf. Jena, Bimbo, Yale and Arade remain
below target. This is not a proof of a hardware limit or of impossibility.

The comparison is new Blitzcrank versus **historical Blitzcrank, which also uses
Delayed Coding**. It is not DC versus rANS. Joint-symbol modeling, materialized
output layout and indexing contribute to these gains.

## Repeated result: one state, full precision, generic record profile

One common configuration for all datasets: `compress-records` with one state,
full 16-bit probability precision, joint ENUM symbols and eligible STRING chunks.
Compact prepared alias models; no direct tables, numeric-slot fusion, native
CPU target flag, handwritten SIMD, decoded-row cache or batched queries.

| Dataset | Old uniform ns/row | New uniform ns/row | Uniform gain | Zipf gain | Total bytes change |
|---|---:|---:|---:|---:|---:|
| Jena Climate | 767.2 | 341.4 | 2.25x | 2.82x | -19.1% |
| Forest Cover | 1091.0 | 186.4 | **5.85x** | **6.57x** | +31.1% |
| US Census 1990 | 1492.2 | 280.8 | **5.31x** | **5.63x** | **-10.2%** |
| Food | 537.2 | 130.0 | **4.13x** | 3.83x | -4.1% |
| Bimbo | 955.2 | 338.6 | 2.82x | 2.74x | +7.1% |
| Yale Languages | 1115.0 | 374.9 | 2.97x | 3.58x | +8.5% |
| Arade | 765.2 | 259.3 | 2.95x | 3.22x | +39.6% |
| cps — identity unconfirmed | 3804.3 | 550.6 | **6.91x** | **7.39x** | +23.5% |

These are ratios of three-run medians of **per-run means**, not percentiles,
confidence bounds or guaranteed minimum speedups. Size includes C++ payload,
dictionary and index sidecars versus the complete self-contained Rust file.
In particular, Forest's speed comes with a substantial size regression; Census
is the clearest speed-and-size improvement in this set.

[All timings, hashes, trace manifests and validation](results/2026-09-14-random-records/SUMMARY.md)
are retained, including [raw samples](results/2026-09-14-random-records/results.json).
No rows from a different build or configuration were substituted into this table.

### Memory accounting

Median process peak RSS from the three uniform runs, including setup and the
one-million-ID trace, not just steady-state decoder storage:

| Dataset | Old MiB | New MiB | Additional prepared plan bytes |
|---|---:|---:|---:|
| Jena | 38.00 | 36.83 | 3,606,176 |
| Forest | 37.33 | 29.88 | 5,888 |
| Census | 89.28 | 67.54 | 450,320 |
| Food | 129.75 | 93.53 | 476,688 |
| Bimbo | 580.29 | 412.94 | 3,345,376 |
| Yale | 198.20 | 185.31 | 3,262,894 |
| Arade | 301.50 | 333.80 | 18,756 |
| cps | 48.11 | 44.08 | 669,488 |

Prepared plan bytes exclude the archive, original model storage and mutable row
scratch. They must not be presented as total memory use. The table does not
demonstrate out-of-core operation or memory-usage bounds under hostile input.

## Measurement and correctness contract

- Full datasets, one independently compressed row per indexed record. No
  decoding of a neighboring row or a 256-row block to answer a query.
- Both helpers locate and materialize the whole row. Integers are i64, decimals
  are f64, dictionary enums are IDs, and logical strings are copied. Rust
  numerical dictionaries are prepared once; fixed numbers are decoded directly,
  and numeric byte fallbacks are parsed during the query. This is **not** the
  earlier Jena CSV-formatting shortcut. The physical C++/Rust output structures
  still differ; they are implementations of the same logical operation.
- Every new row was compared against its lexical decoder, including bitwise
  f64 comparison against decimal parsing. Every complete CSV was SHA-256 checked
  against its source: **44,930,864 rows across eight files**, all passed.
  See [full verification](results/2026-09-14-random-records/verification.json).
- Historical files come from the previously validated independent-record
  [control matrix](results/2026-09-14-record-smoke/summary.json). The old numeric
  backend uses its supplied error tolerances; the new archive preserves source
  bytes and produces correctly rounded f64 values. Archive sizes are therefore
  not a pure comparison of identical lossy numeric modeling.
- One million identical IDs per backend, separately for uniform xorshift64 and
  finite unscrambled Zipf theta=0.99. Traces use seed 123456 and are generated
  outside timing. Both helpers warm row zero and the first 1,000 trace queries.
  New typed verification is outside timing and followed by that warm-up.
- Three alternating runs per distribution, serial processes on CPU 2 of an
  Intel Xeon Platinum 8474C. No concurrent benchmark, build or test was launched
  by this agent during the repeated matrix. The host was **not exclusively
  reserved**; background users, frequency and cache effects are not eliminated.
- File loading, CRC, model/index preparation, trace generation, validation and
  CSV/JSON formatting are outside query timers. This is not disk random-read
  latency, CLI startup time, p50/p99, online insertion, or transaction latency.
- C++: pristine `0ed9c97908c51440b30a2eef3c1b90325dd2c87c`, GCC 12.2 Release
  `-O3 -DNDEBUG`, original static library. Rust: 1.92.0 Release, thin LTO,
  codegen-units=1, default features, portable target. Compiler/LTO differences
  are part of this implementation comparison, not an isolated language test.

The exact tested helper hashes are in
[metadata](results/2026-09-14-random-records/metadata.json):

- Rust `cc67e3d741f9dce90e707b4c2a078a9234906ed0455877cb6ff47dcdd975f9f2`
- C++ `da6953ecb94306354210df7dd3012a6ecbb086128c8879007223fb9753fbc4cb`

The frozen local source snapshot is
`/tmp/blitzcrank-random-opt.TclzFY/final-source`, preserving the sibling crate
layout. It is not a published/pinned remote dependency. Working trees remain
uncommitted; no branch, push, transfer or default-branch change was performed.
[Source hashes and build provenance](results/2026-09-14-random-records/provenance.json)
are recorded; the relocated snapshot was successfully rebuilt with `--locked`.

## What changed

The [record API](../RECORD_API.md) describes the implementation and new format
flags. The material changes are:

1. Prepared, reusable typed single-row output rather than per-column dynamic
   arrays holding one value; direct numeric reconstruction and compact numeric
   dictionaries; u32 resident offsets.
2. Greedy adjacent categorical groups, at most four fields and a Cartesian
   alphabet of 65,536. A joint DC symbol expands to several original fields.
   This removes many entropy-state updates and can capture field correlations.
   It does not cache decoded rows or skip output fields.
3. Four-byte STRING substring dictionaries for eligible byte fallbacks, reducing
   DC event count while preserving exact strings and record independence.
4. Packed decode-only alias metadata in the standalone DC library, plus explicit
   four-state/narrow-state/table experiments. Supported APIs remain safe Rust.

The default record profile retains integer/fixed-decimal varints: those fields
do not use DC. The wins are not all attributable to the entropy kernel. The
joint-symbol and substring transformations could also be used with rANS; a
same-model/container rANS ablation remains necessary for a DC-specific claim.

## Four states and failed experiments

After changing symbol granularity, four states were tested again on all eight
files. Both full CSV and every typed row were verified. The subsequent timing
is a **one-run diagnostic**, not repeated acceptance:
[four-state results](results/2026-09-14-random-records-four-state/SUMMARY.md).

Census reached about 245.5 ns uniform / 194.4 ns Zipf, but total size rose to
64,260,398 bytes, +27.1% versus historical (one-state profile: 45,423,956).
Food and Arade regressed in both query time and size. Yale's one Zipf sample
crossed 4x while uniform remained 3.26x. These do not establish general 4x or
justify replacing the one-state default everywhere.

Earlier single-run screens, retained in local scratch, include:

- `/tmp/blitzcrank-random-matrix._asq96nz`: previous block-output reader.
- `.slcyf4nv`: initial typed reader; `.pmfgf75u`: packed alias slots.
- `.ory2jiy2`: initial four-state path, often slower.
- `.o1xmj1m9`: 8-bit small-alphabet probabilities / four states, before final
  joint categorical models. This is a different representation, not final data.
- `.eh6qorsx`: v1 joint groups before general-table groups and matched warm-up.

Full 16-bit direct tables were particularly bad for the many-model Census
screen (~2.8 us single-state). Smaller aligned tables, native CPU compilation
and numeric/alias-slot fusion also failed to provide a general win. They remain
explicit experiments, not default settings. Screens overlapped some development
work and are explanatory diagnostics, not calibrated causal ablations.

## Remaining work

The gap is real: Jena needs another ~1.78x reduction relative to this Rust
uniform result to reach old/4; Bimbo ~1.42x, Yale ~1.35x and Arade ~1.36x.
These arithmetic budgets are **not hardware lower bounds**.

Next measurements should isolate typed numeric dictionary lookup, record-index
dependency, entropy work and output materialization. Promising directions are
better numeric/string symbol representations and denser record-index/layout
designs that still decode only one row. Any padding, wider tables or probability
changes must have explicit size/RAM budgets. Add per-query latency distributions
and same-pipeline rANS controls before claiming an extreme-latency limit or
algorithmic superiority. Do not use cross-query batching to replace this metric.

Cars/cps identity and the correct merged Corel input remain unresolved; see the
[paper inventory](PAPER_RESULTS.md). Medicare's historical control has a known
numeric tolerance violation, so it cannot supply a valid speedup denominator.
This is not the complete paper's YCSB/Silo/TPC-C workload or a production release.

### Medicare: new backend only

The new record profile passes all 8,645,072 typed rows and exact CSV SHA-256.
Three one-million-query runs give median means of **676.3 ns uniform** and
**420.3 ns Zipf**, with a 473,628,799-byte archive. The old numeric tolerance
failure prevents a valid speedup ratio. This is separate from the eight-pair
table above: [samples](results/2026-09-14-random-medicare.json),
[archive/trace hashes and verification](results/2026-09-14-random-medicare-verification.json).

## Reproduce

```sh
cargo build --release --example record_pack --example trace_records --example archive_csv
python3 benchmarks/record_variants.py --root VALIDATED_RECORD_CONTROL_ROOT \
  --encoder target/release/examples/record_pack --precision 16 --lanes 1 --chunks \
  --datasets USCensus1990 covtype cps jena_climate Food Bimbo_1 YaleLanguages Arade_1
python3 benchmarks/verify_record_profile.py --root NEW_RECORD_ROOT \
  --reader target/release/examples/trace_records --csv target/release/examples/archive_csv \
  --datasets USCensus1990 covtype cps jena_climate Food Bimbo_1 YaleLanguages Arade_1
python3 benchmarks/random_matrix.py --root NEW_RECORD_ROOT --cpp ORIGINAL_CPP_HELPER \
  --rust target/release/examples/trace_records --queries 1000000 --runs 3
```

Use `QUERY_TRACES.md` to build the historical helper against the exact original
source/library. Each runner creates fresh scratch and freezes executables; the
summary exporter refuses to overwrite existing result directories. The current
Rust default and all-feature Release suites pass 23 tests, including new joint
models, typed values, chunk padding, event ordering and repaired-checksum
mutations. The core all-feature suite, strict Clippy and debug prepared-decoder
tests pass. Rust 1.88 compatibility is checked. This is not a security audit.
