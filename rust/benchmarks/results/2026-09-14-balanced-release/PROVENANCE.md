# Release-candidate provenance

The frozen executable and archive SHA-256 values are in `metadata.json`; full
restored CSV hashes are in `verification.json`. Timing used Rust 1.92.0, thin
LTO, one codegen unit, no global target-cpu=native flag, CPU 2 on the recorded
Xeon host. Three alternating process runs per distribution, with one million
identical IDs and matched warmup. Setup, input reads, CRC and model preparation
are outside the resident query timer. The machine is shared and frequencies
are not locked; these are medians of run means, not tail-latency quantiles.

The Rust runtime being prepared for `rust-preview` was not committed when these
executables were frozen. The published source retains the same timed balanced
reader. Subsequent release fixes change only constant-sized CRC/query-trace
iterators and pin the Git dependency instead of a development path. They are
not a new performance run. The optional DC physical-lookahead iterator also
changed; this resident benchmark does not call that path. Do not equate final
rebuild executable hashes with the recorded pre-publication hashes.

Balanced archives are previously produced independent-field, one-state, full
precision archives. They were reverified in full with the candidate reader;
they were not regenerated using per-dataset tuning. A regression test checks
that the new default record compressor matches that existing representation.
The C++ control is pristine Blitzcrank `0ed9c97908c51440b30a2eef3c1b90325dd2c87c`,
GCC 12.2 Release -O3, no LTO, using the separately compiled typed-seek helper.
The new numeric representation is byte-exact; historical semantic numeric
tolerances differ. These are application comparisons, not isolated DC gains.
