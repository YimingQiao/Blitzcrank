# Explicit AVX-512 bulk integration

Build with `cargo build --release --features avx512` (Rust 1.89+), then:

```sh
target/release/blitzcrank-rs capabilities --json
target/release/blitzcrank-rs compress-simd input.csv schema.config bulk.bcr 4096 --json
target/release/blitzcrank-rs validate bulk.bcr --json
target/release/blitzcrank-rs decompress bulk.bcr restored.csv --json
```

The library entry point is `general::compress_simd(&table, block_rows)`.
Ordinary compression and independent-record random reads do not select it.
The profile uses independent column models, as does ordinary bulk compression;
it does not jointly model fields or lower probability precision.

## Dispatch and cost

Eligible entropy columns (dictionary size <=256, or byte streams) use the
standalone `delayed-coding-simd` cumulative DC16/64 encoder and bounded decoder.
Larger dictionaries retain ordinary one-state DC16; fixed numbers keep the
existing lossless packing. Models and workspaces are reused across blocks.

Runtime AVX2, AVX512F/BW/VL/VBMI2 and POPCNT detection guards every intrinsic
entry. Without the feature, on non-x86, or on an unsupported CPU, the companion
uses a scalar implementation of the **same** format. Global `target-cpu=native`
is neither needed nor recommended for a portable binary. Feature availability
does not imply every column is accelerated.

An exact compact LUT costs 16 KiB when all frequencies are divisible by 16;
otherwise the full-precision decoder uses 512 KiB. Additional SIMD LUTs are
capped at 16 MiB per archive, allocated in column order; remaining eligible
columns decode scalarly. Scalar model metadata and typed dictionaries are extra.
`GeneralArchive::column_backend` and `simd_prepared_bytes` expose the plan.

64 states have startup overhead. A file's final block can be short even when
the nominal block size is large. Large blocks increase random-read amplification.
Preparation, parsing and numeric reconstruction may dominate file-to-file time.
This profile is opt-in, not a claim of broadly better end-to-end performance.

## Format extension

V2 header flag bit 3 (value 8) declares the SIMD bulk profile. It requires
block_rows >=4096, ordinary fallback states=1, and no joint-model flag. Column
metadata and length framing stay unchanged. For codec 0 with alphabet <=256,
or codecs 1/3 (256-byte alphabet), entropy payloads use cumulative intervals,
delay 16 and 64 round-robin lanes. The original normalized frequencies are used.
All other columns use the pre-existing codecs. Original symbol counts follow
from block rows or the stored byte-length prefixes, including suffix sharing.

Readers reject unknown flags and invalid profile combinations. Old readers
reject flag 8 rather than silently interpreting cumulative payloads as alias
payloads. Current portable and AVX-512 builds interoperate; no ISA marker or
implicit quantization is part of the file. Formats remain preview, not C++
compatible. CRC plus entropy checks are not authentication.

## Full-file release checks

The [recorded smoke run](benchmarks/results/2026-09-14-simd-release/results.json)
compresses all rows of Jena, Forest and Census in both portable and AVX-512
builds, then decodes each with the other build. All restored CSV SHA-256 hashes
match the inputs, and both encoders produce identical archives. The
[metadata](benchmarks/results/2026-09-14-simd-release/metadata.json) records
the frozen executables, commands and capabilities. Reproduce with
`benchmarks/simd_smoke.py --help`.

These are one-run full-process diagnostics, not repeated performance estimates.
Forest and Census SIMD archives are respectively about 21.7% and 28.7% larger
than ordinary one-state blocks at the same 4096-row block size. Their observed
AVX-512 decode times improve, but Jena shows essentially no change. Wide-state
startup cost and column eligibility matter: this is why SIMD bulk is opt-in,
and these numbers do not describe independent-record random reads.
