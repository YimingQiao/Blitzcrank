# Rust tabular end-to-end acceptance, 2026-09-14

**The >=2x throughput target passes for compression and decompression, both
bulk and independently compressed records, on the full Census dataset.** This
is the implemented INTEGER/ENUM subset, not a completed migration of all
Blitzcrank features, a hardware-limit claim or a comparison against rANS.

## Final default and record-mode results

Median of three full-process wall times. CSV parsing, fitting/normalization,
file/model/index output and teardown are included in compression. File reading,
model initialization, semantic reconstruction, CSV formatting and output are
included in decompression. Rust additionally computes/verifies its container CRC.
No timer is narrowed to make the target pass.

| Configuration | Phase | C++ seconds | Rust seconds | Throughput speedup |
| --- | --- | ---: | ---: | ---: |
| Bulk | Compress | 16.39 | 4.16 | **3.940×** |
| Bulk | Decompress to CSV | 8.27 | 2.42 | **3.417×** |
| Independent records | Compress | 16.19 | 4.50 | **3.598×** |
| Independent records | Decompress to CSV | 8.33 | 2.71 | **3.074×** |

The default bulk throughput improvement is **294% encoding / 242% decoding**;
equivalently, elapsed time falls about 74.6% / 70.7%. Rust bulk throughput is
86.27 MB/s encoding and 148.30 MB/s decoding, measured in original CSV bytes
(decimal MB). These are not ns/entropy-symbol measurements.

| Configuration | C++ total bytes | Rust total bytes | Change | C++ / Rust compress peak RSS, MiB |
| --- | ---: | ---: | ---: | ---: |
| Bulk | 31,067,142 | 30,721,873 | **−1.11%** | 6600.8 / 379.6 |
| Independent records | 50,559,092 | 51,353,345 | **+1.57%** | 6551.3 / 431.4 |

Sizes include all model/dictionary/index bytes, not just entropy payload. Rust
stores everything plus CRC in one file. C++ totals include `_enum.dat` and
`_temp.index`. Bulk peak RSS falls about 94%; median per-process maximum RSS is
reported, not just entropy workspace. Decoder RSS is about 36.9 / 32.4 MiB
(bulk C++ / Rust), and 79.4 / 70.7 MiB (record C++ / Rust).

All **12 full-data roundtrips** passed byte-for-byte comparison. Per-backend
compressed artifacts were also identical across the three runs. Raw timings:
[2026-09-14-e2e.csv](2026-09-14-e2e.csv). Machine-check the acceptance criterion:

```sh
python3 benchmarks/check_results.py benchmarks/2026-09-14-e2e.csv
```

## Comparison contract and important differences

- Full USCensus1990: **2,458,285 rows, 69 fields, 358,885,350 bytes**.
  One integer field and 68 enum fields, unquoted CSV, exact reconstruction.
- CPU: Intel Xeon Platinum 8474C, one process/thread on logical CPU 2. No
  AVX-512 in this Rust application. OS Linux 6.1.0-45-amd64; Rust 1.92.0,
  GCC 12.2.0, Release builds, C++ `-O3 -DNDEBUG`, Rust thin LTO/codegen-units=1.
  The shared host and SMT sibling were not isolated.
- Both backends freshly timed, alternating C++/Rust, Rust/C++, C++/Rust.
  Input was page-cache-backed; no forced cache flushes or `fsync`. This is not
  cold/durable-storage throughput or a transactional database service test.
- C++ is the existing OFF backend from integration revision
  `903fdde93cde71e80eee5f519daf17f9abb6742d`, with its original C++ encoder and
  decoder. This is **not** a newly compiled pristine legacy `0ed9c979` control.
- Bulk: C++ threshold 20,000 probability intervals (about 286 rows/block),
  Rust **256 rows/block, one DC state per column**. Rust does not use larger
  blocks to obtain this acceptance result. It decodes a whole block on random
  lookup; C++ can stop after the target row, so lookup amplification is not
  identical even though Rust has a smaller maximum block row count.
- Record: both reset per individual row. Rust uses one mixed-model DC state
  across enum fields, not one state per field. Integer fields use varints.
- C++ `skip_learning=1` skips dependency/structure search but still fits model
  parameters. Rust also fits independent per-column models, fusing counts into
  parsing. The exact frequency fitting/normalization policies and numerical
  codec differ: bulk integers use reversible delta bit packing, not C++'s
  numerical model. Models and compressed streams are **not bit-identical across
  implementations**; source CSV reconstruction is identical.
- Rust uses DC16 rather than the C++ delay-24 format. New layout, dictionary
  preparation, integer coding, buffer ownership, indexing and CSV processing
  all contribute. This is not a controlled experiment isolating language choice
  or a new entropy-algorithm claim. It does not establish speedups for arbitrary
  strings, conditional models, JSON, floats or time-series data.

## Matched random-record query sequence

The resident single-row mode also improves: median of three per-run mean
latencies is **1507.80 ns C++ / 717.215 ns Rust**, a **2.102×** throughput ratio.
Each process issues the **same 300,000 row IDs** (xorshift64 seed 123456, modulo
2,458,285), with one initial row-zero warm-up, on CPU 2. Model/file/index setup,
CRC scanning and query generation are outside the timer. Both implementations
locate and reconstruct the typed row; neither formats CSV during this test.
This is not p50/p99 latency or a transactional query benchmark.

[Raw query samples](2026-09-14-seeks.csv). The control
[cpp_seek.cpp](cpp_seek.cpp) links the same OFF C++ decoder library as the CLI;
it only replaces the CLI's query generator/timer for an exactly matched sequence.
The measured C++ `LocateTuple` path uses a binary search through block tuple
counts even for single-row blocks. Rust uses direct fixed-block offset lookup.
Thus the gain includes indexing/layout changes, **not just entropy decoding**.
Both C++ and Rust could benefit from further targeted engineering.

Build the C++ helper from `rust/`, using the same Release library and headers:

```sh
c++ -O3 -DNDEBUG -std=gnu++17 -I../delayed_coding/include -I../rapidjson \
  benchmarks/cpp_seek.cpp /absolute/cpp-build/delayed_coding/libdb_compress.a \
  -o /absolute/acceptance-scratch/cpp_seek
# Run cpp_seek in acceptance-scratch/record-cpp-1 (has sidecars and ../input.config).
# Compare with Rust seek-bench on acceptance-scratch/record-rust-1/payload.bin.
```

## Optional four-state speed/size point

The preceding three-run diagnostic used 256-row blocks and four states. It
measured 3.88 s compression / 1.58 s CSV decompression, versus its contemporary
C++ control at 16.33 / 8.25 s. Its **36,518,271-byte** archive is **17.55% larger**
than C++: it is not the balanced default. This demonstrates why a fast reset-heavy
multi-state configuration should not be advertised without its size cost.

That diagnostic's executable SHA-256 was
`bca0baed293c68a0b19e634ed29f4190f66a2f2919c4e9f29ac13e65e1c5a6ac`;
raw logs and timing CSV remain in `/tmp/blitzcrank-rust-acceptance.DKY1mr`.
The final acceptance executable below adds the typed-input/dictionary-access
APIs and selects the one-state default. Do not confuse the earlier optional
configuration with the final default's exact reproducibility checkpoint.

## Validation

- Nine tests pass in Debug, Release, and Rust **1.88.0** Release (declared MSRV).
  Strict Clippy on all targets passes; production library forbids unsafe code.
- Integer extremes/wrapping deltas and bit widths; constant/skewed/high-cardinality
  enum models; single integer/enum columns; empty enum tokens; LF/CRLF; missing
  final newline; block/tail boundaries; one/four states; typed input validation.
- Every truncation of the small fixtures, checksum mutations, plus **8,000
  checksum-repaired mutations** exercising structural/model/block checks. This
  is bounded testing, not a security audit or exhaustive proof.
- CLI roundtrip, query command and refusal to overwrite input/existing output.
- `examples/verify_seeks.rs` verifies **2,114 shuffled/boundary queries × 69
  fields** against independently parsed full Census data for each Rust mode:
  **4,228 full-data queries** total, including first/last rows and block edges.

## Reproduce and identify the measured build

Use [e2e.sh](e2e.sh) and the [build instructions](../README.md). The final run's
raw files and generated artifacts are retained at
`/tmp/blitzcrank-rust-acceptance.nJKCDW`. The source is the new `rust/` directory
on the integration checkout; no C++ production source or standalone DC core
was changed for this rewrite. No repository was pushed as part of this run.

- Core dependency revision: `7e6c5a2b216967682ad33b045cd277f2dea549a1`.
- CSV SHA-256: `7832d5412b8304ae23b697bcf7226a073326a672ef9b187117579bfe0eae6021`.
- Config SHA-256: `d72d29c8a7f43103d746991edce72a60c219352906735f83408c6bea9990d744`.
- C++ executable SHA-256: `a099892b0be401caa1387cfcf8466b214e9c22430240644a0e3266aa9965baeb`.
- Final Rust executable SHA-256: `129f89c801dab321d0a1146ef99a52e3acdcb26b2641aa36b7bdf85cff408525`.

The earlier 1024-row exploratory run was faster/smaller than C++ too, but is
not the acceptance configuration: its larger blocks would change random-access
amplification, and its provisional index layout predates this final v1 layout.
