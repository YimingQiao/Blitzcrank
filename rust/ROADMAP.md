# Multi-dataset / agent API / 10x acceptance work

## Release scope update (2026-09-14; supersedes the historical gates below)

The user now requests broadly useful defaults, optional hardware acceleration,
complete code review before publication, and publication of both repositories.
The candidate keeps balanced independent records as default. Joint modeling,
chunk dictionaries, reduced precision and large tables are explicit experiments.
AVX-512 is an opt-in bulk codec with portable decoding, not a random-read claim.
Publish a new Blitzcrank branch while preserving historical main; publish the
standalone DC project under embryo-labs, retaining the personal repository.
Review, regression tests, reachable pinned dependencies and honest limitations
remain release gates. Universal 4x/10x is not claimed or silently redefined.
Current handoff: `RELEASE_REVIEW.md`; old investigations below remain historical.

Latest scope update: the user prioritizes resident **independent-record random
reads**, requesting at least **4x** versus the original implementation across
datasets. CSV scan/export gains do not satisfy this. The new typed record API,
joint categorical models and string chunks are described in `RECORD_API.md`;
the current random-read evidence belongs in `benchmarks/RANDOM_ACCESS.md`.
The historical 10x CSV investigation below is retained as context, not the
current acceptance metric. No general speedup or hardware-limit proof may be
inferred from one favorable dataset.

User request (2026-09-14): validate beyond Census using paper datasets, make
Blitzcrank agent-friendly, pursue 10x versus old code or provide defensible
limits, then publish a new default branch and delayed-coding under embryo.

## Gates (do not silently redefine success)

1. Inventory paper datasets, source hashes, dimensions, actual field types and
   file provenance. Mark mismatches/missing data rather than substituting them.
2. Broaden lossless type/CSV support and robustness; keep v1 files readable.
   Compare total bytes, quality, peak RSS and repeated full-process timings.
3. Stable machine-readable CLI: discover capabilities, inspect, validate, query,
   compress/decompress; JSON success/error contracts; no prompts or overwrites.
4. Separately measure CSV file-to-file and paper-like resident tuple operations.
   Single-core comparisons remain primary. Any multicore result is separate.
   Do not claim universal 10x from one favorable dataset, caching or a weakened
   model/error tolerance. A measured gap is not proof of impossibility.
5. Publishing is gated on truthful reports, tests, clean dependency setup and
   confirmed remote. Preserve old default/history; no force-push/delete.

## Current evidence

- Prior v1 baseline and acceptance: rust/benchmarks/RESULTS.md (Census only).
- Paper: https://www.vldb.org/pvldb/vol17/p2528-zhang.pdf, Table 1, sections 6–7.
- GitHub resolves the historical Blitzcrank URL to embryo-labs/Blitzcrank,
  currently public, default main, authenticated permission ADMIN.
- delayed-coding remains YimingQiao/delayed-coding. Asked user to confirm embryo
  destination before any publication/default-branch change.
- Local research data exists in /home/yiming/projects/Blitzcrank/playground;
  that original checkout is user-dirty and is READ-ONLY for this work.
- Cars candidate cps.dat matches paper row count; identity still needs auditing.
- Corel feature components do not all have 68,040 rows; do not silently zip them.

## Implemented in this work session

- Paper inventory with hashes; eight named dimension matches plus an explicitly
  unconfirmed cps candidate. No fabricated Corel merge.
- Exact DOUBLE/STRING/quoted/multiline/pipe CSV support in the new v2 backend;
  exact fixed-scale packing, dictionary/byte fallback, block-local prefix sharing.
  Existing v1 files remain readable. Both formats are still experimental.
- Agent JSON contracts, stable errors, capabilities/inspect/validate, single-
  and batch-row reads, atomic create-only outputs and a minimal Python client.
- Repeated full bulk matrices, full record smoke coverage, shared uniform/Zipf
  query traces and explicit preservation of the failing Medicare C++ control.
- Byte-compatible optional lookup-table experiments, off by default. Their
  many-model regression is reported rather than treated as an automatic win.

See `benchmarks/PAPER_RESULTS.md` and its raw artifacts. One Jena decode result
crosses 10x at the median, but **general 10x is not achieved and impossibility
has not been proved**. Larger archives on several tables remain a release blocker.

## Next research/engineering steps, not completed claims

1. Prepared immutable models and typed record insertion APIs. The current
   rewrite is a bulk table builder plus read-only archive; it does not yet
   replace the paper's online insert/update engine or learned dependency models.
2. Reduce dictionary/tokenization work and improve numerical/string modeling.
   Current-stage budgets show why a zero-cost entropy kernel alone is not enough.
3. Investigate SIMD across independent rows while preserving **one state per
   row**, instead of putting 64 startup states into every short record. DC's
   frequency-only forward schedule can potentially plan exact record lengths
   and offsets before the information-buffer pass. Test whether this enables
   useful direct output placement; compare against rANS in the same outer
   pipeline before making any DC-specific advantage claim.
4. Confirm Cars/Corel provenance, expand resident workload coverage and repeat
   acceptance on an isolated host, with explicit speed/ratio/RAM budgets.
5. Confirm the embryo destination; publish the complete core dependency first,
   pin a reachable revision, add clean-checkout CI, then publish a candidate
   Blitzcrank branch. Preserve historical main; do not switch defaults merely
   because one favorable benchmark exceeds 10x.

No branch/default/remote publication operation has been performed during this
still-incomplete acceptance work.
