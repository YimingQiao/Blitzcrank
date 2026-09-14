use crate::joint::Group;
use crate::{ensure, table::append_integer, Column, Result, Table};
use delayed_coding::{
    decode_grouped4_into, decode_into, encode_events_interleaved_into, encode_interleaved_into,
    encode_into, Decoder, Event, Model, Workspace,
};
use std::io::Write;

const MAGIC: &[u8; 8] = b"BLTZRS01";
const MAX_DICT_BYTES: usize = 64 << 20;

#[derive(Clone, Copy, Debug)]
pub struct Options {
    pub block_rows: usize,
    pub lanes: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            block_rows: 256,
            lanes: 1,
        }
    }
}

/// Format v1: independent column models, DC16 with 1 or 4 states, delta-packed
/// integers, absolute block offsets and whole-container CRC32. Not C++ compatible.
pub fn compress(table: &Table, options: Options) -> Result<Vec<u8>> {
    compress_with_precision(table, options, 16)
}

/// Explicit probability-precision experiment. <=256-active-symbol models use
/// `bits` (8..16); larger dictionaries retain full precision. Same container.
pub fn compress_with_precision(table: &Table, options: Options, bits: u32) -> Result<Vec<u8>> {
    compress_config(table, options, bits, false)
}

/// Balanced independent records: independent fields, full precision by default
/// when the caller passes 16. No joint-symbol or string-chunk experiment.
pub fn compress_records(table: &Table, lanes: usize, bits: u32) -> Result<Vec<u8>> {
    compress_with_precision(
        table,
        Options {
            block_rows: 1,
            lanes,
        },
        bits,
    )
}

/// Opt-in independent records with adjacent categorical fields jointly modeled.
/// Groups have <=4 fields and Cartesian alphabets <=65536; integers stay raw.
pub fn compress_joint_records(table: &Table, lanes: usize, bits: u32) -> Result<Vec<u8>> {
    compress_config(
        table,
        Options {
            block_rows: 1,
            lanes,
        },
        bits,
        true,
    )
}

fn groups(table: &Table, bits: u32) -> Result<Vec<Group>> {
    let input: Vec<_> = table
        .columns
        .iter()
        .map(|c| match c {
            Column::Enum {
                ids, dictionary, ..
            } => Some((ids.as_slice(), dictionary.len())),
            _ => None,
        })
        .collect();
    crate::joint::build(&input, table.rows, bits)
}

fn compress_config(table: &Table, options: Options, bits: u32, joint: bool) -> Result<Vec<u8>> {
    ensure((8..=16).contains(&bits), "precision must be 8..16")?;
    let Options { block_rows, lanes } = options;
    ensure(
        block_rows > 0 && block_rows <= 65536,
        "block rows must be 1..65536",
    )?;
    ensure(matches!(lanes, 1 | 4), "expected 1 or 4 states")?;
    ensure(
        block_rows * table.columns.len() <= 1 << 22,
        "block workspace limit exceeded",
    )?;
    let blocks = table.rows.div_ceil(block_rows);
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    out.push(lanes as u8);
    out.push(u8::from(table.crlf) | (u8::from(table.final_newline) << 1) | (u8::from(joint) << 2));
    put32(&mut out, table.columns.len() as u32);
    put32(&mut out, table.rows as u32);
    put32(&mut out, block_rows as u32);
    put32(&mut out, blocks as u32);
    let mut models = Vec::new();
    let mut dictionary_bytes = 0;
    for column in &table.columns {
        match column {
            Column::Integer(_) => {
                out.push(0);
                models.push(None);
            }
            Column::Enum {
                dictionary, counts, ..
            } => {
                out.push(1);
                put32(&mut out, dictionary.len() as u32);
                let model = crate::probability_model(counts, bits)?;
                for (token, frequency) in dictionary.iter().zip(model.frequencies()) {
                    dictionary_bytes += token.len();
                    ensure(
                        dictionary_bytes <= MAX_DICT_BYTES,
                        "dictionary byte limit exceeded",
                    )?;
                    put32(&mut out, token.len() as u32);
                    out.extend_from_slice(token);
                    put32(&mut out, frequency);
                }
                models.push(Some(model));
            }
        }
    }
    let groups = if joint {
        groups(table, bits)?
    } else {
        Vec::new()
    };
    if joint {
        put32(&mut out, groups.len() as u32);
        for group in &groups {
            out.push(group.columns.len() as u8);
            for &c in &group.columns {
                out.extend_from_slice(&(c as u16).to_le_bytes());
            }
            let frequencies = group.model.frequencies();
            put32(&mut out, frequencies.len() as u32);
            for f in frequencies {
                put32(&mut out, f);
            }
        }
    }
    let index_position = out.len();
    out.resize(out.len() + (blocks + 1) * 4, 0);
    let capacity = block_rows.max(table.columns.len());
    let mut workspace = Workspace::with_capacity(capacity);
    let mut symbols = Vec::with_capacity(block_rows);
    let mut events = Vec::with_capacity(table.columns.len());
    let mut storage = vec![0; capacity * 2];
    for block in 0..blocks {
        let start = block * block_rows;
        let end = (start + block_rows).min(table.rows);
        let offset = u32::try_from(out.len())?;
        out[index_position + block * 4..index_position + (block + 1) * 4]
            .copy_from_slice(&offset.to_le_bytes());
        if block_rows == 1 {
            // Share one DC stream across enum fields instead of resetting a
            // separate coder for every field of a small record.
            events.clear();
            for (column, model) in table.columns.iter().zip(&models) {
                match column {
                    Column::Integer(values) => put_varint(&mut out, zigzag(values[start])),
                    Column::Enum { ids, .. } if !joint => events.push(Event {
                        model: model.as_ref().unwrap(),
                        symbol: u32::from(ids[start]),
                    }),
                    Column::Enum { .. } => (),
                }
            }
            if joint {
                for group in &groups {
                    let mut symbol = 0;
                    for (&c, &radix) in group.columns.iter().zip(&group.radices) {
                        let Column::Enum { ids, .. } = &table.columns[c] else {
                            unreachable!()
                        };
                        symbol = symbol * radix + u32::from(ids[start]);
                    }
                    events.push(Event {
                        model: &group.model,
                        symbol,
                    });
                }
            }
            let range = if lanes == 4 {
                encode_events_interleaved_into::<16, 4>(&events, &mut storage, &mut workspace)?
            } else {
                encode_events_interleaved_into::<16, 1>(&events, &mut storage, &mut workspace)?
            };
            out.extend_from_slice(&storage[range]);
            continue;
        }
        for (column, model) in table.columns.iter().zip(&models) {
            let length_position = out.len();
            put32(&mut out, 0);
            match column {
                Column::Integer(values) => pack_integers(&values[start..end], &mut out),
                Column::Enum { ids, .. } => {
                    symbols.clear();
                    symbols.extend(ids[start..end].iter().map(|&v| u32::from(v)));
                    let model = model.as_ref().unwrap();
                    let range = if lanes == 4 {
                        encode_interleaved_into::<16, 4>(
                            model,
                            &symbols,
                            &mut storage,
                            &mut workspace,
                        )?
                    } else {
                        encode_into::<16>(model, &symbols, &mut storage, &mut workspace)?
                    };
                    out.extend_from_slice(&storage[range]);
                }
            }
            let length = u32::try_from(out.len() - length_position - 4)?;
            out[length_position..length_position + 4].copy_from_slice(&length.to_le_bytes());
        }
    }
    let end = u32::try_from(out.len())?;
    out[index_position + blocks * 4..index_position + (blocks + 1) * 4]
        .copy_from_slice(&end.to_le_bytes());
    let checksum = crc32(&out);
    put32(&mut out, checksum);
    Ok(out)
}

enum Spec<'a> {
    Integer,
    Enum {
        dictionary: Vec<&'a [u8]>,
        model: Model,
    },
}

/// Validated, resident compressed file. Opening verifies the full-file CRC;
/// subsequent block/row reads do not scan other blocks. Models are reusable.
pub struct Archive<'a> {
    bytes: &'a [u8],
    specs: Vec<Spec<'a>>,
    offsets: Vec<u32>,
    rows: usize,
    block_rows: usize,
    lanes: usize,
    crlf: bool,
    final_newline: bool,
    groups: Option<Vec<Group>>,
}

/// Reusable typed block. One vector per column, not one object per cell.
#[derive(Default)]
pub struct Block {
    integers: Vec<Vec<i64>>,
    symbols: Vec<Vec<u32>>,
    rows: usize,
}
impl Block {
    pub fn rows(&self) -> usize {
        self.rows
    }
    pub fn integer(&self, column: usize, row: usize) -> Option<i64> {
        if row >= self.rows {
            return None;
        }
        self.integers.get(column)?.get(row).copied()
    }
    pub fn symbol(&self, column: usize, row: usize) -> Option<u32> {
        if row >= self.rows {
            return None;
        }
        self.symbols.get(column)?.get(row).copied()
    }
}

impl<'a> Archive<'a> {
    pub fn open(bytes: &'a [u8]) -> Result<Self> {
        ensure(bytes.len() >= 30, "truncated header")?;
        let split = bytes.len() - 4;
        let expected = u32::from_le_bytes(bytes[split..].try_into()?);
        ensure(
            crc32(&bytes[..split]) == expected,
            "container checksum mismatch",
        )?;
        let mut reader = Reader {
            bytes: &bytes[..split],
            position: 0,
        };
        ensure(reader.take(8)? == MAGIC, "unsupported magic/version")?;
        let lanes = reader.byte()? as usize;
        ensure(matches!(lanes, 1 | 4), "unsupported states")?;
        let flags = reader.byte()?;
        ensure(flags & !7 == 0, "unknown flags")?;
        let columns = reader.u32()? as usize;
        let rows = reader.u32()? as usize;
        let block_rows = reader.u32()? as usize;
        let blocks = reader.u32()? as usize;
        ensure(
            columns > 0 && columns <= 1024 && rows > 0,
            "invalid dimensions",
        )?;
        ensure(
            block_rows > 0 && block_rows <= 65536 && block_rows * columns <= 1 << 22,
            "invalid block size",
        )?;
        ensure(blocks == rows.div_ceil(block_rows), "invalid block count")?;
        let mut specs = Vec::with_capacity(columns);
        let mut dictionary_bytes = 0usize;
        for _ in 0..columns {
            specs.push(match reader.byte()? {
                0 => Spec::Integer,
                1 => {
                    let count = reader.u32()? as usize;
                    ensure(
                        count > 0 && count <= 65536 && count <= reader.remaining() / 8,
                        "invalid alphabet",
                    )?;
                    let mut dictionary = Vec::with_capacity(count);
                    let mut frequencies = Vec::with_capacity(count);
                    for _ in 0..count {
                        let len = reader.u32()? as usize;
                        ensure(len <= 1 << 20, "token too long")?;
                        dictionary_bytes += len;
                        ensure(
                            dictionary_bytes <= MAX_DICT_BYTES,
                            "dictionary byte limit exceeded",
                        )?;
                        let token = reader.take(len)?;
                        ensure(
                            !token
                                .iter()
                                .any(|b| matches!(b, b',' | b'\r' | b'\n' | b'"')),
                            "invalid enum token",
                        )?;
                        dictionary.push(token);
                        let frequency = reader.u32()?;
                        ensure(frequency > 0, "zero frequency")?;
                        frequencies.push(frequency);
                    }
                    Spec::Enum {
                        dictionary,
                        model: crate::decoding_model(Model::new(&frequencies)?),
                    }
                }
                _ => return Err("unknown field codec".into()),
            });
        }
        let groups = if flags & 4 != 0 {
            ensure(block_rows == 1, "joint models require independent records")?;
            let n = reader.u32()? as usize;
            ensure(n <= columns, "invalid group count")?;
            let mut seen = vec![false; columns];
            let mut groups = Vec::with_capacity(n);
            for _ in 0..n {
                let count = reader.byte()? as usize;
                ensure((1..=4).contains(&count), "invalid group width")?;
                let mut members = Vec::with_capacity(count);
                let mut radices = Vec::with_capacity(count);
                let mut product = 1usize;
                for _ in 0..count {
                    let c = u16::from_le_bytes(reader.take(2)?.try_into()?) as usize;
                    ensure(c < columns && !seen[c], "invalid/repeated group column")?;
                    let Spec::Enum { dictionary, .. } = &specs[c] else {
                        return Err("integer in enum group".into());
                    };
                    seen[c] = true;
                    members.push(c);
                    radices.push(dictionary.len() as u32);
                    product = product
                        .checked_mul(dictionary.len())
                        .ok_or("group alphabet overflow")?;
                    ensure(product <= 65536, "group alphabet limit")?;
                }
                ensure(
                    reader.u32()? as usize == product && product <= reader.remaining() / 4,
                    "invalid group frequencies",
                )?;
                let frequencies: Vec<_> =
                    (0..product).map(|_| reader.u32()).collect::<Result<_>>()?;
                groups.push(Group {
                    columns: members,
                    radices,
                    model: crate::decoding_model(Model::new(&frequencies)?),
                });
            }
            ensure(
                specs
                    .iter()
                    .enumerate()
                    .all(|(c, s)| matches!(s, Spec::Integer) || seen[c]),
                "missing group column",
            )?;
            Some(groups)
        } else {
            None
        };
        ensure(blocks < reader.remaining() / 4, "truncated index")?;
        let mut offsets = Vec::with_capacity(blocks + 1);
        for _ in 0..=blocks {
            offsets.push(reader.u32()?);
        }
        ensure(
            offsets[0] as usize == reader.position && offsets[blocks] as usize == split,
            "invalid index endpoints",
        )?;
        ensure(
            offsets
                .windows(2)
                .all(|w| w[0] < w[1] && w[1] as usize <= split),
            "nonmonotonic index",
        )?;
        Ok(Self {
            bytes: &bytes[..split],
            specs,
            offsets,
            rows,
            block_rows,
            lanes,
            crlf: flags & 1 != 0,
            final_newline: flags & 2 != 0,
            groups,
        })
    }

    pub fn rows(&self) -> usize {
        self.rows
    }
    pub fn columns(&self) -> usize {
        self.specs.len()
    }
    pub fn blocks(&self) -> usize {
        self.offsets.len() - 1
    }
    pub fn block_rows(&self) -> usize {
        self.block_rows
    }

    pub fn state_count(&self) -> usize {
        self.lanes
    }
    pub fn joint_groups(&self) -> usize {
        self.groups.as_ref().map_or(0, Vec::len)
    }

    /// Prepare typed independent-record access, without changing archive bytes.
    pub fn record_reader(&self) -> Result<crate::record::RecordReader<'_>> {
        use crate::record::{EntropyField, Mapping, Prefix, RecordReader};
        ensure(
            self.block_rows == 1,
            "record reader requires one row per block",
        )?;
        let mut prefixes = Vec::new();
        let mut entropy = Vec::new();
        for (column, spec) in self.specs.iter().enumerate() {
            match spec {
                Spec::Integer => prefixes.push(Prefix::Integer(column)),
                Spec::Enum { model, .. } if self.groups.is_none() => entropy.push(EntropyField {
                    column,
                    model,
                    mapping: Mapping::Category,
                }),
                Spec::Enum { .. } => (),
            }
        }
        if let Some(groups) = &self.groups {
            for group in groups {
                let decoded = (0..group.model.alphabet_size())
                    .map(|id| {
                        let mut id = id as u32;
                        let mut values = [0u16; 4];
                        for (i, &radix) in group.radices.iter().enumerate().rev() {
                            values[i] = (id % radix) as u16;
                            id /= radix;
                        }
                        values
                    })
                    .collect();
                entropy.push(EntropyField {
                    column: group.columns[0],
                    model: &group.model,
                    mapping: Mapping::Group {
                        columns: group.columns.clone(),
                        decoded,
                    },
                });
            }
        }
        Ok(RecordReader {
            fused: Vec::new(),
            dictionary_only: true,
            models: entropy
                .iter()
                .map(|f| delayed_coding::DecodeModel::from_model(f.model))
                .collect(),
            bytes: self.bytes,
            offsets: &self.offsets,
            columns: self.columns(),
            lanes: self.lanes,
            prefixes,
            entropy,
        })
    }

    pub fn field_type(&self, column: usize) -> Option<crate::Field> {
        match self.specs.get(column)? {
            Spec::Integer => Some(crate::Field::Integer),
            Spec::Enum { .. } => Some(crate::Field::Enum),
        }
    }

    /// Resolve a decoded enum ID without copying its dictionary token.
    pub fn enum_token(&self, column: usize, symbol: u32) -> Option<&'a [u8]> {
        match self.specs.get(column)? {
            Spec::Enum { dictionary, .. } => dictionary.get(symbol as usize).copied(),
            Spec::Integer => None,
        }
    }

    pub fn decode_block(&self, index: usize, block: &mut Block) -> Result<()> {
        block.rows = 0;
        ensure(index < self.blocks(), "block out of range")?;
        let rows = (self.rows - index * self.block_rows).min(self.block_rows);
        block.rows = 0;
        block.integers.resize_with(self.specs.len(), Vec::new);
        block.symbols.resize_with(self.specs.len(), Vec::new);
        let mut reader = Reader {
            bytes: &self.bytes[self.offsets[index] as usize..self.offsets[index + 1] as usize],
            position: 0,
        };
        if self.block_rows == 1 {
            for (column, spec) in self.specs.iter().enumerate() {
                block.integers[column].clear();
                block.symbols[column].clear();
                if matches!(spec, Spec::Integer) {
                    let code = reader.varint()?;
                    block.integers[column].push(((code >> 1) as i64) ^ -((code & 1) as i64));
                }
            }
            let payload = reader.take(reader.remaining())?;
            if self.lanes == 4 {
                self.decode_record_enums::<4>(payload, block)?;
            } else {
                self.decode_record_enums::<1>(payload, block)?;
            }
            block.rows = 1;
            return Ok(());
        }
        for (column, spec) in self.specs.iter().enumerate() {
            let length = reader.u32()? as usize;
            let payload = reader.take(length)?;
            match spec {
                Spec::Integer => {
                    block.symbols[column].clear();
                    unpack_integers(payload, rows, &mut block.integers[column])?;
                }
                Spec::Enum { model, .. } => {
                    block.integers[column].clear();
                    block.symbols[column].resize(rows, 0);
                    if self.lanes == 4 {
                        decode_grouped4_into::<16>(model, payload, &mut block.symbols[column])?;
                    } else {
                        decode_into::<16>(model, payload, &mut block.symbols[column])?;
                    }
                }
            }
        }
        ensure(reader.remaining() == 0, "trailing block data")?;
        block.rows = rows;
        Ok(())
    }

    /// Locate a row and decode its containing block into caller-owned scratch.
    /// Returns the within-block row offset. Large blocks trade read amplification
    /// for throughput/size; block_rows=1 supplies independent record access.
    pub fn locate_row(&self, row: usize, block: &mut Block) -> Result<usize> {
        block.rows = 0;
        ensure(row < self.rows, "row out of range")?;
        self.decode_block(row / self.block_rows, block)?;
        Ok(row % self.block_rows)
    }

    fn decode_record_enums<const LANES: usize>(
        &self,
        payload: &[u8],
        block: &mut Block,
    ) -> Result<()> {
        let mut decoder = Decoder::<16, LANES>::new(payload)?;
        if let Some(groups) = &self.groups {
            for group in groups {
                let mut id = decoder.read(&group.model)?;
                for (&column, &radix) in group.columns.iter().zip(&group.radices).rev() {
                    block.symbols[column].push(id % radix);
                    id /= radix;
                }
            }
            decoder.finish()?;
            return Ok(());
        }
        for (column, spec) in self.specs.iter().enumerate() {
            if let Spec::Enum { model, .. } = spec {
                block.symbols[column].push(decoder.read(model)?);
            }
        }
        decoder.finish()?;
        Ok(())
    }

    pub fn write_csv(&self, mut writer: impl Write) -> Result<()> {
        let mut block = Block::default();
        let mut output = Vec::with_capacity(self.block_rows * self.columns() * 3);
        for index in 0..self.blocks() {
            self.decode_block(index, &mut block)?;
            output.clear();
            for row in 0..block.rows {
                for (column, spec) in self.specs.iter().enumerate() {
                    if column != 0 {
                        output.push(b',');
                    }
                    match spec {
                        Spec::Integer => append_integer(&mut output, block.integers[column][row]),
                        Spec::Enum { dictionary, .. } => output
                            .extend_from_slice(dictionary[block.symbols[column][row] as usize]),
                    }
                }
                if self.final_newline || index * self.block_rows + row + 1 < self.rows {
                    if self.crlf {
                        output.push(b'\r');
                    }
                    output.push(b'\n');
                }
            }
            writer.write_all(&output)?;
        }
        writer.flush()?;
        Ok(())
    }
}

fn put32(out: &mut Vec<u8>, n: u32) {
    out.extend_from_slice(&n.to_le_bytes());
}
struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Reader<'a> {
    fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }
    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        ensure(size <= self.remaining(), "truncated container")?;
        let start = self.position;
        self.position += size;
        Ok(&self.bytes[start..self.position])
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into()?))
    }
    fn varint(&mut self) -> Result<u64> {
        let mut value = 0u64;
        for i in 0..10 {
            let byte = self.byte()?;
            ensure(i < 9 || byte <= 1, "varint overflow")?;
            value |= u64::from(byte & 127) << (7 * i);
            if byte < 128 {
                ensure(i == 0 || byte != 0, "noncanonical varint")?;
                return Ok(value);
            }
        }
        Err("invalid varint".into())
    }
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 128 {
        out.push(value as u8 | 128);
        value >>= 7;
    }
    out.push(value as u8);
}

fn zigzag(delta: i64) -> u64 {
    ((delta as u64) << 1) ^ ((delta >> 63) as u64)
}
pub(crate) fn pack_integers(values: &[i64], out: &mut Vec<u8>) {
    out.extend_from_slice(&values[0].to_le_bytes());
    let union = values
        .windows(2)
        .fold(0, |bits, w| bits | zigzag(w[1].wrapping_sub(w[0])));
    let width = 64 - union.leading_zeros();
    out.push(width as u8);
    let mut accumulator = 0u128;
    let mut bits = 0;
    for pair in values.windows(2) {
        accumulator |= (zigzag(pair[1].wrapping_sub(pair[0])) as u128) << bits;
        bits += width;
        while bits >= 8 {
            out.push(accumulator as u8);
            accumulator >>= 8;
            bits -= 8;
        }
    }
    if bits != 0 {
        out.push(accumulator as u8);
    }
}
pub(crate) fn unpack_integers(payload: &[u8], rows: usize, values: &mut Vec<i64>) -> Result<()> {
    let mut reader = Reader {
        bytes: payload,
        position: 0,
    };
    let mut value = reader.u64()? as i64;
    let width = reader.byte()? as u32;
    ensure(width <= 64, "invalid integer width")?;
    ensure(
        reader.remaining() == ((rows - 1) * width as usize).div_ceil(8),
        "invalid integer payload size",
    )?;
    let mask = if width == 64 {
        u64::MAX
    } else {
        (1u64 << width) - 1
    };
    values.clear();
    values.reserve(rows);
    values.push(value);
    let mut accumulator = 0u128;
    let mut bits = 0;
    for _ in 1..rows {
        while bits < width {
            accumulator |= (reader.byte()? as u128) << bits;
            bits += 8;
        }
        let code = accumulator as u64 & mask;
        accumulator >>= width;
        bits -= width;
        let delta = ((code >> 1) as i64) ^ -((code & 1) as i64);
        value = value.wrapping_add(delta);
        values.push(value);
    }
    ensure(accumulator == 0, "nonzero integer padding")?;
    Ok(())
}

const fn crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = (crc >> 1) ^ (0xedb88320u32 & (0u32.wrapping_sub(crc & 1)));
            bit += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}
pub(crate) fn crc32(bytes: &[u8]) -> u32 {
    const TABLES: [[u32; 256]; 8] = crc_tables();
    let mut crc = u32::MAX;
    let (chunks, remainder) = bytes.as_chunks::<8>();
    for chunk in chunks {
        let lo = u32::from_le_bytes(chunk[..4].try_into().unwrap()) ^ crc;
        crc = TABLES[7][(lo & 255) as usize]
            ^ TABLES[6][((lo >> 8) & 255) as usize]
            ^ TABLES[5][((lo >> 16) & 255) as usize]
            ^ TABLES[4][(lo >> 24) as usize]
            ^ TABLES[3][chunk[4] as usize]
            ^ TABLES[2][chunk[5] as usize]
            ^ TABLES[1][chunk[6] as usize]
            ^ TABLES[0][chunk[7] as usize];
    }
    for &byte in remainder {
        crc = (crc >> 8) ^ TABLES[0][((crc as u8) ^ byte) as usize];
    }
    !crc
}

const fn crc_tables() -> [[u32; 256]; 8] {
    let mut tables = [[0; 256]; 8];
    tables[0] = crc_table();
    let mut level = 1;
    while level < 8 {
        let mut i = 0;
        while i < 256 {
            let previous = tables[level - 1][i];
            tables[level][i] = (previous >> 8) ^ tables[0][(previous & 255) as usize];
            i += 1;
        }
        level += 1;
    }
    tables
}

#[cfg(test)]
mod tests {
    #[test]
    fn crc_slicing_matches_byte_reference() {
        let table = super::crc_table();
        let bytes: Vec<u8> = (0..4096).map(|n| (n * 97 + n / 7) as u8).collect();
        for start in 0..16 {
            let mut reference = u32::MAX;
            assert_eq!(super::crc32(&bytes[start..start]), !reference);
            for end in start..bytes.len() {
                reference = (reference >> 8) ^ table[((reference as u8) ^ bytes[end]) as usize];
                assert_eq!(super::crc32(&bytes[start..=end]), !reference);
            }
        }
    }
    use super::*;
    #[test]
    fn crc_vector() {
        assert_eq!(crc32(b"123456789"), 0xcbf43926);
    }
    #[test]
    fn all_integer_widths() {
        for bit in 0..64 {
            let input = [0, 1i64.wrapping_shl(bit), -1, i64::MIN, i64::MAX, 0];
            let mut encoded = Vec::new();
            pack_integers(&input, &mut encoded);
            let mut decoded = Vec::new();
            unpack_integers(&encoded, input.len(), &mut decoded).unwrap();
            assert_eq!(decoded, input);
        }
    }

    #[test]
    fn repaired_checksum_mutations_reach_structural_checks() {
        use crate::Schema;
        use std::io::Cursor;
        let table = Table::read_csv(
            Cursor::new("1,a\n2,b\n3,a\n4,c\n"),
            &Schema::parse("INTEGER 0\nENUM 3 0").unwrap(),
        )
        .unwrap();
        let mut rng = 987654321u64;
        for block_rows in [1, 3] {
            for lanes in [1, 4] {
                let encoded = compress(&table, Options { block_rows, lanes }).unwrap();
                for _ in 0..2000 {
                    let mut bytes = encoded.clone();
                    let end = bytes.len() - 4;
                    rng ^= rng << 13;
                    rng ^= rng >> 7;
                    rng ^= rng << 17;
                    let position = rng as usize % end;
                    bytes[position] ^= 1 << ((rng >> 32) % 8);
                    let checksum = crc32(&bytes[..end]);
                    bytes[end..].copy_from_slice(&checksum.to_le_bytes());
                    // Valid alternate containers may be accepted. Every other
                    // mutation must return an error, never panic or read OOB.
                    if let Ok(archive) = Archive::open(&bytes) {
                        let mut block = Block::default();
                        for index in 0..archive.blocks() {
                            if archive.decode_block(index, &mut block).is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}
