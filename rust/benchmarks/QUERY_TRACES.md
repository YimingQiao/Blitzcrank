# Resident query comparison

`query_trace.py ROWS TRACE --queries 1000000 --distribution uniform|zipf`
generates little-endian u32 row IDs and a JSON manifest with SHA-256. Uniform
uses the existing xorshift64 sequence; Zipf uses a finite CDF with explicit
theta (default 0.99), Python's seeded RNG, and **unscrambled** row ranks. Query
generation is outside all timers. This is not asserted to be the paper's exact
YCSB generator, rank scrambling or complete workload reproduction.

Create independently compressed archives (`compress ... 1 1`, C++ threshold 1)
and validate their full roundtrips first. Both helpers use the **same trace
file**, pre-load model/index/archive, warm up row zero, then time locate+decode.
The current `trace_records` / C++ helpers additionally warm the first 1,000 trace
queries. The older `trace_queries` example and previously frozen results retain
their historical protocol; do not mix those measurements. Both current helpers
require one row per block. CRC, model initialization, file reads, query
generation and CSV/JSON formatting are not timed. Results are per-run mean
latencies, not latency percentiles or transaction response times.

```sh
cargo build --release --example trace_records
target/release/examples/trace_records records.bcr trace.u32

# Build the helper with the exact historical source/library, not modified code.
c++ -O3 -DNDEBUG -std=gnu++17 \
  '-DBLITZCRANK_SOURCE="/absolute/legacy-source/tabular.cpp"' \
  -I/absolute/legacy-source/delayed_coding/include \
  -I/absolute/legacy-source/rapidjson \
  benchmarks/cpp_seek.cpp /absolute/legacy-build/delayed_coding/libdb_compress.a \
  -o /absolute/scratch/cpp_seek
# In the C++ archive directory (payload.bin + original sidecars):
/absolute/scratch/cpp_seek /absolute/trace.u32 schema 0
```

Use delimiter flag 1 for Medicare pipes. The C++ helper preserves its previous
no-argument, 300,000-query uniform mode for reproducing the older Census report.
Run processes serially, on the same pinned CPU, alternating order across three
runs. Preserve trace and executable hashes. New Rust indexing is direct fixed-
block lookup while old C++ indexing uses its own locate path; this is an
application/indexing comparison, not an isolated entropy-kernel comparison.

The rewrite still lacks the paper's online insertion workload, dependency
learning and Silo/TPC-C integration. Do not label this full paper replication.
