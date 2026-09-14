# Rust preview review — 2026-09-14

Release intent: a stronger, reusable implementation with conservative defaults,
explicit experimental profiles and reproducible measurements. The new
`rust-preview` branch does not replace historical `main`. This is not a complete
port of the paper's storage engine or a claim of universal 4x/10x improvement.

## Review scope

Source review covers every Rust runtime module: `archive`, `general`, `table`,
`fixed`, `record`, `joint`, `cli`, `view`, library/binary entry points; tests and
examples; benchmark helpers/runners; dependency and CI configuration. The
standalone core, FFI and SIMD review is tracked in the dependency's
`docs/RELEASE_REVIEW.md`. Existing C++ bridge changes were inspected and the
retained C++ build regression-tested. Unchanged research and vendored C++ are
not certified safe by this Rust preview; in particular, the historical Medicare
numeric-control failure remains documented rather than hidden.

This is one-agent maintainer-style review, not an independent security audit or
a mathematical guarantee of correctness/performance on arbitrary inputs.

## Corrections and policy decisions

- Failed block/row reads previously could expose an earlier block's values.
  Invalidate row counts before bounds checks; getters reject inaccessible rows.
  Tests exercise both v1 and v2 and subsequent buffer reuse.
- Bound CSV record reads before growing the buffer beyond 64 MiB + one sentinel
  byte, not only after `read_until` has consumed an arbitrarily long line.
- Default `compress-records` is now `balanced`: independent fields, one state,
  full probability precision, compact prepared tables. Library APIs distinguish
  `compress_records` from `compress_joint_records`. General mode requires an
  explicit boolean to enable joint modeling/chunks.
- Joint categorical symbols, four-byte chunks, reduced precision, four states,
  large LUTs and numeric fusion remain explicit tradeoffs, not generic wins.
- SIMD bulk uses a separate flagged payload contract and runtime detection with
  portable fallback. Models/workspaces are prepared once; extra direct LUTs have
  a 16 MiB per-archive budget. No SIMD or batching is inserted into single-row reads.
- CLI retains one-line JSON envelopes, structured errors and create-only atomic
  output. A new profile reports its choice; inspection distinguishes DC64 codecs.
- Benchmark source capture no longer assumes a sibling DC checkout: it resolves
  the actual locked Cargo dependency. Failed random runs exit nonzero, duplicate
  distributions are rejected, and a one-distribution run cannot satisfy a
  misleading "both distributions" flag.
- Documentation separates balanced, joint and SIMD results, full-process versus
  resident timings, mean latency versus percentiles, and bytes versus process RSS.
- Current Rust 1.98 Clippy requires constant word iterators to use `as_chunks`.
  Updated CRC word iteration and query-trace loading without changing tail
  semantics or the timed resident-read loop; the CRC reference tests were rerun.

## Verification

- Default Debug and all-feature Release suites, strict Clippy, formatting,
  Python syntax checks; portable all-target Rust 1.88 compatibility.
- Dependency pinned to published core `847df1439f0345d652515f19b486fd9bcf5264bd`;
  downloaded Git dependencies build without the development sibling directory.
  Current Rust 1.98 strict all-target/all-feature Clippy also passes.
- Pinned C++ bridge: both 1-row and 20,000-row thresholds on the first 20,000
  Census rows preserve payload/model/index bytes and all 8,200 tested seeks
  per backend/threshold against pristine historical C++.
- New regressions cover balanced-format identity, output invalidation, SIMD
  mixed-field/tail roundtrips, malformed profile flags and aggregate LUT budgets.
- Full-file SIMD checks on Jena, Forest and Census verify portable/AVX-512
  archive byte identity and cross-build restored CSV hashes; see [SIMD](SIMD.md).
- Standalone scalar/SIMD differential tests and SIMD AddressSanitizer passed;
  C ABI Miri and pristine historical DC differential checks passed.
- All **44,930,864** typed rows across eight balanced-profile archives verified;
  each full restored CSV matches the source SHA-256. These are existing ordinary
  one-state archives, not freshly retuned joint-model files. See
  [verification](benchmarks/results/2026-09-14-balanced-release/verification.json).
- [Balanced resident-query results](benchmarks/results/2026-09-14-balanced-release/SUMMARY.md):
  one million identical IDs per backend/distribution, uniform and Zipf, three
  alternating runs, CPU 2, matched warmup, complete typed output, setup excluded.
  Every pair is faster here; uniform gains range 1.64x–4.17x, Zipf 1.45x–3.82x.
  This does not establish performance on unknown workloads or every CPU.
- Total archive size is **not uniformly better**: balanced Arade is +65.7% and
  Forest +31.5% versus the historical control. The old and new numeric models
  differ; the new pipeline preserves source bytes. Evaluate space/RSS before
  migrating. The joint profile remains available but its results are separate.

## Non-goals and unresolved limitations

No online insert/update, conditional-model learner, transaction engine, partial
projection, C++ file reader or out-of-core build. Files/model columns are resident;
opening verifies a whole-file CRC. A new CLI process per query is not the resident
API. CRC is not authentication; enforce external memory/time/output quotas.
Formats and APIs are preview, not a long-term compatibility promise.

Cars/cps identity and the correct merged Corel file are unconfirmed. Medicare's
old numeric control is invalid for a speedup denominator. Historical reports are
retained with those caveats. No production certification, universal speedup,
hardware-limit proof or DC-specific advantage over another entropy coder is claimed.

Publication gates: reviewed source, tests, pinned reachable DC revision, clean
checkout build and CI inspection. Source corrections after this review require
focused regression tests; old branches and personal repositories are preserved.
