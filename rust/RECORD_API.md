# Resident typed records (experimental)

The primary optimization target is now **locate + decode one independent row**,
not CSV export or a 256-row scan. The historical archive and all model/index
setup must already be resident before starting a query timer. A CLI invocation
still pays file loading and a whole-archive checksum; embed the Rust reader for
actual repeated low-latency access.

```sh
cargo build --release
blitzcrank-rs compress-records input.csv schema.config records.bcr --json
blitzcrank-rs inspect records.bcr --json
blitzcrank-rs seek-record-bench records.bcr 1000000 --json
blitzcrank-rs decompress records.bcr restored.csv --json
```

`compress-records INPUT SCHEMA OUTPUT [STATES=1] [PRECISION=16] [PROFILE=balanced]`
uses one row per block. States can be 1 or 4; precision can be 8..16. The default
uses independent fields: no joint symbols, string chunks, reduced precision or
large direct lookup tables. Explicit `joint` enables the representation
experiments below. Output creation is
atomic and create-only, just like `compress`; comma/pipe and `--general` work.

For a v1 archive (canonical INTEGER/ENUM schema):

```rust
use blitzcrank_rs::{Archive, record::{Record, Value}};
# fn example(bytes: &[u8]) -> blitzcrank_rs::Result<()> {
let archive = Archive::open(bytes)?;       // CRC, dictionaries, models, index
let reader = archive.record_reader()?;    // immutable prepared decode plan
let mut row = Record::default();          // reusable per-thread output
reader.read(42, &mut row)?;               // exactly this row; no row cache
for value in row.values() {
    match value {
        Value::Integer(n) => { /* use i64 */ }
        Value::Category(id) => { /* archive-local dictionary ID */ }
        _ => {}
    }
}
# Ok(()) }
```

`general::GeneralArchive::record_reader()` exposes the same interface for v2.
Values are `Integer(i64)`, `Decimal(f64)`, `Category(u32)`, `Text { start, len }`
or `Null`; use `row.text(column)` for logical unquoted string bytes. Strings are
copied into a reusable row buffer, not merely returned as a dictionary pointer.
Numeric dictionaries are converted once at preparation; fixed-scale values
become numbers directly, and byte-coded numeric fields are parsed on each read.
Decimal results match Rust's correctly rounded decimal-to-f64 parsing, including
negative zero. Empty and literal `null` numeric tokens both become `Null` in this
typed API; the lexical CSV API preserves their distinction and original spelling.
Enum IDs are archive-local; use `enum_token` / `category_token` to resolve them.
An ENUM that exceeded the dictionary limit and fell back to byte coding instead
returns logical `Text`; it does not invent an unpersisted dictionary ID.

Readers require `block_rows=1`. Reuse one immutable reader across queries, with
separate mutable `Record` outputs for concurrent threads. Failed reads invalidate
the output (`values()` is empty); do not use old contents after an error. There
is no decoded-row cache, query batching, partial-row projection, or online
insertion/update API in this implementation.

## Record representation

- Integer fields use lossless ZigZag varints; fixed decimal fields similarly
  store their exact scaled integer. Those fields do not use DC.
- Only in the explicit `joint` profile, adjacent ENUM dictionary columns are greedily grouped, up to four fields and
  a Cartesian alphabet of 65,536. The group histogram is fitted on the table;
  one DC symbol reconstructs several field IDs. Integer/non-ENUM fields and
  alphabet-size limits end a group. This is not exhaustive dependency learning.
- Only in the explicit `joint` profile, high-cardinality STRING byte columns may use dictionary-coded four-byte
  substrings aligned to the start of each field. Partial final substrings have
  zero padding; the original byte length is stored. Each substring is a DC
  symbol. The dictionary is capped at 65,536 and the raw representation-size
  filter must pass; otherwise byte DC remains. No neighboring row is needed.
- Other dictionaries/bytes use the previous DC paths. There is no cross-row
  delta or prefix dependency. All original CSV bytes are recoverable.

The representation changes are useful with DC, but do not establish a unique
DC advantage over rANS. A same-model, same-container rANS ablation is still needed.

Library entry points: `compress_records(table, lanes, bits)` is balanced;
`compress_joint_records(table, lanes, bits)` opts into grouping. General tables
use `general::compress_records(table, lanes, bits, joint)`, passing false for
balanced. Existing experimental archives remain readable. SIMD is a separate
bulk profile, never silently selected by the typed record reader.

## Decoder experiments and cost accounting

The default prepared decoder uses packed alias slots and compact numeric
dictionaries. Resident offsets are u32 rather than usize; the on-disk offsets
were already u32. The four-state prepared mixed-model kernel additionally uses
u32 information/capacity states for DC16. All implementations remain safe Rust.

`reader.with_direct_tables(bits)` enables an **exact** direct table only for
models whose boundaries align sufficiently to need at most `2^bits` entries.
This does not quantize an existing archive. To alter probabilities, explicitly
choose the encoder's precision: it applies only to <=256-active-symbol models;
larger models retain 16-bit precision. Source values remain lossless but sizes
can change. A full 16-bit table can cost 512 KiB per model and was often slower.

`reader.with_fused_numeric()?` experimentally stores numeric values with alias
slots, avoiding a dependent dictionary load but increasing model memory. It is
off by default. `prepared_bytes()` reports additional plan/table heap storage,
not total process RSS, archive bytes, original models, or mutable row scratch.

Four states also increase per-record framing/coding overhead. Neither four
states, precision reduction, native CPU compilation nor larger tables is an
automatic win. Report every dataset's total bytes and process peak RSS.

## Format compatibility and verification

Both formats reserve flag bit 2 for joint categorical models. Following the
ordinary column metadata, flagged files store: group count u32; per group,
member count u8, column indices u16, Cartesian alphabet size u32 and that many
u32 frequencies. Every eligible enum dictionary column appears exactly once.
The record entropy stream contains ungrouped events first, then joint events.
General v2 codec 4 stores a chunk count u32, followed by four-byte word/u32
frequency pairs. The record prefix contains its original byte length.

Current readers support old files. **Older binaries reject these new flags or
codec tags**; formats remain experimental and are not C++ compatible. Ordinary
`compress` retains the earlier format/selection behavior. `inspect` reports
`dc_joint_dictionary` and `dc_string_chunks4` for new record-profile files.

`benchmarks/verify_record_profile.py` checks every typed row against the lexical
decoder and checks full decoded CSV SHA-256 against the input. The random query
runner uses shared traces, alternating backends, matched 1,000-query warmups,
and excludes setup/verification/formatting. Mean latency is not p50/p99 or a
database transaction response time. See `benchmarks/RANDOM_ACCESS.md` for results.
