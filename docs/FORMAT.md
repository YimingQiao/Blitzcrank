# Preview container reference

All framing integers are little-endian; raw DC physical words are big-endian
u16. Offsets are absolute u32 byte positions. A trailing u32 IEEE CRC32 covers
all preceding bytes. These formats are self-contained but not frozen or C++
compatible. Unknown flags/codecs must be rejected, never guessed.

## V1 (`BLTZRS01`)

Header: eight-byte magic, u8 states (1/4), u8 flags, then u32 column count,
row count, block-row count, block count. Flag bit 0 selects CRLF, bit 1 says
the last row has a newline. All other bits are rejected.

Each column starts with u8 codec: 0 integer, 1 enum. An enum stores u32 alphabet
size, then repeated u32 token length, token bytes, u32 positive frequency.
Frequencies sum to 65,536. Then `(block_count + 1)` u32 offsets: first
equals payload start, last equals checksum position, strictly increasing.

For bulk blocks, each column starts with u32 payload length. Integer payloads
contain first i64, u8 bit width, then LSB-first packed zigzag-wrapping deltas;
unused padding bits must be zero. Enum payloads are fixed-model alias DC16,
with the header's state count and this block's row count.

Independent records (`block_rows=1`) contain integer zigzag varints in field
order, followed by one mixed-model alias DC16 stream in enum field order.
No per-column payload lengths in record mode.

## V2 (`BLTZRS02`)

Header: eight-byte magic, u8 flags, u8 delimiter (comma/pipe), u8 states (1/4),
then u32 columns, rows and block rows. Flags 0/1 have the same meanings as v1;
bit 2 is unsupported; bit 3 is the explicit [SIMD bulk extension](SIMD.md).

Each column stores u8 logical kind (integer=0, enum=1, decimal=2, string=3)
and u8 codec:

- 0 dictionary: alphabet, tokens and frequencies as in v1.
- 1 bytes: 256 u32 frequencies, with zero-frequency entries allowed.
- 2 exact fixed decimal: u8 scale 0..18; numeric kinds only.
- 3 prefix bytes: metadata as codec 1; bulk-only prefix sharing.

Then `ceil(rows/block_rows)+1`
absolute u32 offsets with the same endpoint/monotonicity rules.

Bulk blocks have u32 per-column payload lengths. Dictionary columns contain
DC ID streams; fixed decimals use the v1 integer packer. Byte columns prefix
the stream with one unsigned length varint per row; prefix mode instead stores
`(shared-prefix length, suffix length)` pairs followed by DC-coded suffix bytes.
The first shared-prefix length of each block is zero. Ordinary entropy streams
are alias DC16; flag 8 explicitly changes eligible columns as documented in SIMD.md.

Independent records prefix byte lengths and fixed-number zigzag varints in
column order. One mixed-model entropy stream then contains dictionary IDs and
byte events in field order. Fixed columns contribute no entropy events.

Length varints are canonical unsigned base-128 u32 (at most 5 bytes); integer
varints are canonical unsigned base-128 u64 after zigzag (at most 10 bytes).
Out-of-range terminal bytes, overlong encodings and truncated reads are rejected.

Readers verify framing/CRC at open and payload bounds/final states during reads.
This is not authentication or a guarantee of semantic validity under deliberately
recomputed CRC. Callers must bound resources; see [limits](../README.md) and
[development notes](DEVELOPMENT.md). Removed joint flags and chunk codec 4 are
rejected; historical preview compatibility is not maintained.
