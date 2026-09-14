# Blitzcrank Rust preview

A Rust tabular compression library and agent-friendly CLI, using the standalone
[Delayed Coding](https://github.com/embryo-labs/delayed-coding) project. Supports
INTEGER, ENUM, DOUBLE and STRING, exact CSV restoration, self-contained archives
and resident typed random reads. The [historical C++ implementation](https://github.com/embryo-labs/Blitzcrank/tree/main)
lives on `main`; this branch contains only the Rust application.

**Preview scope:** static compression and read-only archives, not a complete
port of the paper's learned models or transactional storage engine. No C++ file
compatibility, online insertion/update, schema evolution or production guarantee.

## Build

Rust 1.88+; Rust 1.89+ for the optional AVX-512 feature. The release manifest pins
the standalone crates to an immutable Git revision; no sibling checkout is needed.

```sh
git clone --branch rust-preview https://github.com/embryo-labs/Blitzcrank.git
cd Blitzcrank
cargo test --release --locked
cargo build --release --locked
target/release/blitzcrank-rs capabilities --json
```

## Independent records

```sh
target/release/blitzcrank-rs compress-records data.csv schema.config records.bcr --json
target/release/blitzcrank-rs inspect records.bcr --json
target/release/blitzcrank-rs seek-record-bench records.bcr 1000000 --json
target/release/blitzcrank-rs decompress records.bcr restored.csv --json
cmp data.csv restored.csv
```

`compress-records` defaults to **balanced**: one independently indexed row,
independent field models, one DC state, full 16-bit probability precision.
No decoded-row cache or workload-specific tuning is required.

Open the archive and prepare a `record_reader()` once, then reuse a caller-owned
`Record` for actual resident queries. Numeric dictionaries are prepared once;
strings are copied as logical bytes. File I/O, CRC and model setup are not part
of resident latency. Calling a fresh CLI process for each query pays those costs.
See [the typed API](docs/RECORD_API.md) and [JSON agent contract](docs/AGENT_API.md).

```rust
use blitzcrank_rs::{Archive, record::Record};
# fn example(bytes: &[u8]) -> blitzcrank_rs::Result<()> {
let archive = Archive::open(bytes)?;
let reader = archive.record_reader()?;
let mut record = Record::default();
reader.read(0, &mut record)?;
# Ok(()) }
```

For mixed-type v2 files, use `general::GeneralArchive::open` and the same
`record_reader()` interface. `Table::from_columns` accepts owned typed
INTEGER/ENUM columns without serializing them to CSV.

## Explicit tradeoffs

| Choice | Command / API | Tradeoff |
| --- | --- | --- |
| Balanced records | `compress-records INPUT SCHEMA OUTPUT` | Default independent-row profile |
| Ordinary bulk | `compress INPUT SCHEMA OUTPUT [256] [1]` | Better scan throughput/packing; whole-block random reads |
| SIMD bulk | `compress-simd INPUT SCHEMA OUTPUT [4096]` | 64-state entropy streams; startup/table costs and read amplification |
| Four-state records | `compress-records INPUT SCHEMA OUTPUT 4` | Explicit parallelism/space tradeoff |

Bulk integers use reversible wrapping-delta/zigzag bit packing; record integers
use varints. High-cardinality exact fixed-scale numeric columns can use the
same numeric representation. These are outer-pipeline codecs, not DC operations.
Low-cardinality fields use dictionaries; high-cardinality lexical fields fall
back to bytes. Ordinary bulk mode may share string prefixes within a block.

For hardware acceleration:

```sh
cargo build --release --locked --features avx512
target/release/blitzcrank-rs compress-simd data.csv schema.config bulk.bcr --json
```

AVX-512 has runtime detection and a byte-identical scalar fallback; portable
builds can read SIMD-profile archives. It does not change balanced records.
See [SIMD scope, memory budget and format](docs/SIMD.md). Neither wide SIMD nor extra
states are assumed to help single-row reads.

## Agent interface and output safety

Discover commands with `capabilities --json`. Supported operations include
`inspect`, `validate`, `decode-row`, `decode-rows`, compression and decompression.
[The Python client](examples/agent_client.py) uses argument arrays, not a shell.

Outputs must not exist. The CLI stages beside the target and atomically creates
it without replacement; filesystems must support hard links. Failure removes
the staging file; abrupt process termination may leave it. Flush is performed,
but fsync/crash durability is not promised. No remote service or tool execution
is hidden in the CLI.

## Correctness, limits and performance

- Lossless CSV lexemes: comma/pipe, quoted and multiline fields, LF/CRLF,
  final-newline state, negative zero and decimal spelling. Mixed line endings,
  empty files and nonfinite numeric values are rejected.
- Common limits: 1,024 columns, u32 rows/offsets, 65,536 rows per block,
  4,194,304 cells per block, 1 MiB tokens and 64 MiB input records.
- General v2 decoded blocks are capped at 64 MiB. Dictionary limits are 64 MiB
  per general column / 1 GiB total; v1 dictionary data is capped at 64 MiB total.
  These are format limits, not a process-RSS guarantee.
- Input columns and the output archive are currently materialized in RAM.
  This is not an out-of-core compressor. CRC detects accidental corruption,
  not malicious modifications. Apply external memory/CPU/output limits.
- [Benchmark evidence](benchmarks/README.md) separates resident queries from CSV
  throughput and reports space costs. No universal 4x/10x claim is made.
- Review and tests do not constitute a security audit. See
  [review and development](docs/DEVELOPMENT.md).

## Formats

`BLTZRS01`: canonical INTEGER/ENUM; `BLTZRS02`: mixed lexical CSV. Both embed
models, dictionaries, block offsets and CRC32. No external sidecars are needed.
The experimental joint/chunk formats and their APIs have been removed. Historical
compatibility is not a goal of this preview; unsupported flags/codecs are rejected.
The earlier release remains available under its immutable tag.
See the [format reference](docs/FORMAT.md).

[SIMD format extensions](docs/SIMD.md) document cumulative DC64 bulk streams.
