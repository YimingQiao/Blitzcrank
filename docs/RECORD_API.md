# Resident typed records

Keep the archive and its prepared reader resident for repeated queries. A new
CLI process per row pays file loading and whole-file checksum costs.

```sh
target/release/blitzcrank-rs compress-records input.csv schema.config records.bcr --json
target/release/blitzcrank-rs seek-record-bench records.bcr 1000000 --json
```

`compress-records INPUT SCHEMA OUTPUT [STATES=1]` writes one independently
indexed row per block, with independent fields and full 16-bit precision.
States can be 1 or 4. In Rust, use `compress(&table, Options { block_rows: 1,
lanes: 1 })`, or `general::compress(&table, 1, 1)` for lexical mixed-type data.

```rust
use blitzcrank_rs::{Archive, record::{Record, Value}};
# fn example(bytes: &[u8]) -> blitzcrank_rs::Result<()> {
let archive = Archive::open(bytes)?;
let reader = archive.record_reader()?;
let mut row = Record::default();
reader.read(42, &mut row)?;
for value in row.values() {
    match value {
        Value::Integer(n) => { /* use i64 */ }
        Value::Category(id) => { /* archive-local dictionary ID */ }
        _ => {}
    }
}
# Ok(()) }
```

`general::GeneralArchive::record_reader()` exposes the same interface for
mixed-type archives. Values are Integer(i64), Decimal(f64), Category(u32),
Text { start, len } or Null. `row.text(column)` gives logical unquoted bytes.
Numeric dictionaries are converted once; strings are copied into reusable
output. Byte-coded numeric fields are parsed per read. Empty and literal
`null` numeric tokens both become Null; CSV restoration preserves the original
spelling. Decimal conversion preserves negative zero.

Enum IDs are archive-local: resolve them with `enum_token` or `category_token`.
Enums promoted to byte coding return logical Text instead. Share an immutable
reader across threads but give each its own Record. Failed reads invalidate
output: values() becomes empty. prepared_bytes() measures additional plan heap,
not total RSS, the resident archive, original models or mutable row buffers.

Integer/fixed-number prefixes use lossless zigzag varints. Dictionary IDs and
lexical bytes share a mixed-model DC stream in field order. There is no
cross-row dependency, decoded-row cache, projection or transaction support.
Four states can cost more bytes on short records. SIMD is separate bulk coding,
not automatically selected for independent reads.

Joint models, string chunks, precision tuning and large-table/fused-reader
experiments have been removed, including their historical compatibility paths.
See [format](FORMAT.md), [agent API](AGENT_API.md) and
[measurement contract](../benchmarks/README.md).
