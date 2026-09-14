# Benchmarks and evidence

Run from the repository root. Data are external, never bundled or downloaded
implicitly: use DATA/tables/NAME.dat and DATA/config/NAME.config. Preserve source,
schema, executable and archive hashes. These are application tests, not isolated
entropy-coder comparisons or reproductions of a transaction engine.

## Recorded checkpoints

- [Balanced queries](results/2026-09-14-balanced-release/SUMMARY.md): three
  alternating runs, one million shared uniform/Zipf IDs, one Xeon thread.
  [Full verification](results/2026-09-14-balanced-release/verification.json)
  covers 44,930,864 rows. [Provenance](results/2026-09-14-balanced-release/PROVENANCE.md)
  identifies pre-cleanup binaries, not timings of a new build. Space is not
  uniformly better: Arade is +65.7%.
- [SIMD smoke](results/2026-09-14-simd-release/results.json): complete
  Jena/Forest/Census restoration and scalar/AVX file identity. One-run process
  times are diagnostic, not repeated performance estimates.
- [Dataset inventory](2026-09-14-paper-inventory.json): cps is not established
  to be Cars; the merged Corel input is unresolved. Medicare's historical C++
  numeric validation failed, so it is not a valid speedup denominator.

Old variants, intermediate reports and bridge code remain at the
[original preview tag](https://github.com/embryo-labs/Blitzcrank/tree/rust-preview-2026-09-14/rust/benchmarks).
They are not part of the current source or advertised as default results.

## Reproduce record measurements

Build `cargo build --release --locked --examples` and the CLI. Obtain pristine
C++ revision `0ed9c97908c51440b30a2eef3c1b90325dd2c87c` in a separate checkout,
following its build instructions (GCC 12 may need `-include cstdint`).

```sh
python3 benchmarks/paper_matrix.py --data-root /absolute/DATA \
  --cpp /absolute/legacy-build/tabular_blitzcrank \
  --rust target/release/blitzcrank-rs --datasets jena_climate \
  --modes record --runs 1 --cpu 2
```

This prints a fresh matrix directory and verifies full restoration. Models are
independent fields, full precision, one state. For process timing, use at least
three repetitions and summarize with summarize_matrix.py. Process timing includes
startup, parsing, model construction, CRC and I/O; resident timing excludes them.

```sh
c++ -O3 -DNDEBUG -std=gnu++17 \
  '-DBLITZCRANK_SOURCE="/absolute/legacy-source/tabular.cpp"' \
  -I/absolute/legacy-source/delayed_coding/include -I/absolute/legacy-source/rapidjson \
  benchmarks/cpp_seek.cpp /absolute/legacy-build/delayed_coding/libdb_compress.a \
  -o /absolute/scratch/cpp_seek
python3 benchmarks/verify_record_profile.py --root /absolute/matrix \
  --reader target/release/examples/trace_records \
  --csv target/release/examples/archive_csv --datasets jena_climate
python3 benchmarks/random_matrix.py --root /absolute/matrix \
  --cpp /absolute/scratch/cpp_seek --rust target/release/examples/trace_records \
  --datasets jena_climate --runs 3 --queries 1000000 --cpu 2
```

query_trace.py generates identical little-endian u32 IDs for both helpers.
Zipf uses theta 0.99 and unscrambled ranks, not the complete YCSB protocol.
Both helpers warm row zero and the first 1,000 IDs, then decode complete typed
rows, copying strings. Report medians of run means, not p50/p99. Export with
`summarize_random.py RUN --export NEW_DIR --verification VERIFY/results.json`.

## SIMD interoperability

Build portable and AVX-enabled CLIs in separate target directories. Pass paths
to simd_smoke.py (see --help). It tests identical cumulative DC64 files across
both builds plus ordinary bulk at the same block size, not random reads.
See [SIMD contracts](../docs/SIMD.md).
