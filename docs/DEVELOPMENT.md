# Development and scope

This is the Rust tabular application only. Delayed Coding is a pinned Git
dependency, not vendored or reimplemented here. Both DC packages come from
the same repository and revision.

- `table`, `general`: typed/lexical input and column dictionaries.
- `archive`, `general`: framing, offsets, CRC, reversible numeric packing, DC calls.
- `record`: prepared typed rows using DC's decoder API.
- `cli`, `view`: local commands and JSON presentation.
- `fixed`: exact decimal spelling and conversion.

The canonical INTEGER/ENUM path avoids lexical parsing for already typed data;
the general path preserves mixed-type CSV lexemes. These are current application
paths, not C++ compatibility layers. Neither implements DC arithmetic,
alias construction, reciprocal division or CPU intrinsics locally.

## Checks

```sh
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo test --release --all-features --locked
cargo +1.88.0 check --all-targets --locked
python3 -m py_compile examples/agent_client.py benchmarks/*.py
```

Portable code requires Rust 1.88; AVX-512 requires 1.89+. CI tests x86 and ARM.
Test AVX-512 on capable hardware too: fallback tests do not validate vector
execution. Keep malformed-input, stale-output and exact CSV roundtrip regressions.

The cleanup was checked with all 28 release tests, strict Clippy and Rust 1.88.
All 44,930,864 rows in the eight retained balanced archives were reverified as
typed values and restored CSV hashes. A fresh full Forest encode/decode checked
portable/AVX interoperability; both builds produced identical SIMD archives.
These are correctness checks, not updated performance measurements.

## Readiness and history

This is a preview, not an independently audited or long-deployed storage engine.
The original [release review](https://github.com/embryo-labs/Blitzcrank/blob/156b3f0b07587769bb4fb8b081dce8baa8cea742/rust/RELEASE_REVIEW.md)
records its exact scope; it is not certification of later changes. Core fuzzing
is short-run and does not directly cover every prepared/SIMD entry point or this
container. Sustained fuzzing, independent review and more hardware coverage remain
work. Apply external memory/time/output quotas; CRC is not authentication.

Historical compatibility is not maintained. Joint/chunk archives and removed
tuning APIs are unsupported. The earlier preview tag and historical main retain
old implementations; no history is rewritten. Current source no longer bundles
C++/RapidJSON, Census data, bridge tests or experiment variants. Build requires
neither CMake nor Git LFS.
