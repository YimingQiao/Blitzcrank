//! Lossless lexical-token backend for mixed numeric/string CSV. This does not
//! implement the historical lossy numeric or learned string models. Low-cardinality
//! columns use DC dictionaries; high-cardinality columns use a byte DC fallback.
use crate::{ensure, Result};
use delayed_coding::{
    decode_grouped4_into, decode_into, encode_events_interleaved_into, encode_interleaved_into,
    Decoder, Event, Model, Workspace,
};
use std::{
    collections::HashMap,
    io::{BufRead, Write},
};

const MAGIC: &[u8; 8] = b"BLTZRS02";
const MAX_BLOCK: usize = 64 << 20;
const MAX_TOKEN: usize = 1 << 20;
const MAX_DICTIONARY: usize = 65536;

#[derive(Clone, Copy, Debug)]
pub enum Kind {
    Integer,
    Enum,
    Decimal,
    Text,
}
impl Kind {
    fn tag(self) -> u8 {
        match self {
            Self::Integer => 0,
            Self::Enum => 1,
            Self::Decimal => 2,
            Self::Text => 3,
        }
    }
    fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(Self::Integer),
            1 => Ok(Self::Enum),
            2 => Ok(Self::Decimal),
            3 => Ok(Self::Text),
            _ => Err("unknown field type".into()),
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Integer => "integer",
            Self::Enum => "enum",
            Self::Decimal => "decimal",
            Self::Text => "string",
        }
    }
}
pub fn schema(text: &str) -> Result<Vec<Kind>> {
    let mut out = Vec::new();
    for line in text.lines().filter(|s| !s.trim().is_empty()) {
        let words: Vec<_> = line.split_whitespace().collect();
        out.push(match words.as_slice() {
            ["STRING"] => Kind::Text,
            [name, tolerance] if matches!(*name, "INTEGER" | "DOUBLE") => {
                let t: f64 = tolerance.parse()?;
                ensure(t.is_finite() && t >= 0.0, "invalid tolerance")?;
                if *name == "INTEGER" {
                    Kind::Integer
                } else {
                    Kind::Decimal
                }
            }
            ["ENUM", count, "0"] => {
                let count: usize = count.parse()?;
                ensure(count > 0 && count <= 65536, "invalid enum cardinality")?;
                Kind::Enum
            }
            _ => return Err("unsupported schema type".into()),
        });
    }
    ensure(!out.is_empty() && out.len() <= 1024, "invalid field count")?;
    Ok(out)
}

enum Column {
    Fixed {
        values: Vec<i64>,
        scale: u8,
    },
    Dictionary {
        ids: Vec<u16>,
        tokens: Vec<Vec<u8>>,
        counts: Vec<u32>,
        map: HashMap<Vec<u8>, u16>,
        bytes: usize,
    },
    Bytes {
        data: Vec<u8>,
        ends: Vec<u32>,
        counts: Box<[u32; 256]>,
    },
}
impl Column {
    fn new() -> Self {
        Self::Dictionary {
            ids: Vec::new(),
            tokens: Vec::new(),
            counts: Vec::new(),
            map: HashMap::new(),
            bytes: 0,
        }
    }
    fn append(&mut self, token: &[u8], kind: Option<Kind>) -> Result<()> {
        ensure(token.len() <= MAX_TOKEN, "token limit exceeded")?;
        let mut validated = false;
        if let Self::Dictionary {
            ids,
            tokens,
            counts,
            map,
            bytes,
        } = self
        {
            if let Some(&id) = map.get(token) {
                ids.push(id);
                counts[id as usize] = counts[id as usize].checked_add(1).ok_or("too many rows")?;
                return Ok(());
            }
            validate_numeric(token, kind)?;
            validated = true;
            if tokens.len() < MAX_DICTIONARY && *bytes + token.len() <= MAX_BLOCK {
                let id = tokens.len() as u16;
                tokens.push(token.to_vec());
                map.insert(token.to_vec(), id);
                ids.push(id);
                counts.push(1);
                *bytes += token.len();
                return Ok(());
            }
            // One-time promotion retains all previous values losslessly.
            let mut replacement = Self::Bytes {
                data: Vec::new(),
                ends: vec![0],
                counts: Box::new([0; 256]),
            };
            for &id in ids.iter() {
                replacement.append(&tokens[id as usize], None)?;
            }
            *self = replacement;
        }
        if !validated {
            validate_numeric(token, kind)?;
        }
        if let Self::Bytes { data, ends, counts } = self {
            let end = data
                .len()
                .checked_add(token.len())
                .ok_or("column too large")?;
            ends.push(u32::try_from(end)?);
            data.extend_from_slice(token);
            for &b in token {
                counts[b as usize] = counts[b as usize]
                    .checked_add(1)
                    .ok_or("byte frequency overflow")?;
            }
        }
        Ok(())
    }
    fn token(&self, row: usize) -> &[u8] {
        match self {
            Self::Dictionary { ids, tokens, .. } => &tokens[ids[row] as usize],
            Self::Bytes { data, ends, .. } => &data[ends[row] as usize..ends[row + 1] as usize],
            Self::Fixed { .. } => unreachable!("fixed decimals are formatted, not borrowed"),
        }
    }
    fn compact_numeric(&mut self) {
        // Only replace the byte fallback. Low-cardinality decimal dictionaries
        // can be smaller and faster than packed numbers, so retain them.
        let Self::Bytes { ends, .. } = self else {
            return;
        };
        let rows = ends.len() - 1;
        let mut values = Vec::with_capacity(rows);
        let mut scale = None;
        for row in 0..rows {
            let Some((value, digits)) = crate::fixed::parse(self.token(row)) else {
                return;
            };
            if scale.is_some_and(|s| s != digits) {
                return;
            }
            scale = Some(digits);
            values.push(value);
        }
        *self = Self::Fixed {
            values,
            scale: scale.unwrap_or(0),
        };
    }
}

pub struct GeneralTable {
    columns: Vec<Column>,
    kinds: Vec<Kind>,
    rows: usize,
    delimiter: u8,
    crlf: bool,
    final_newline: bool,
}

struct ChunkPlan {
    words: Vec<[u8; 4]>,
    counts: Vec<u32>,
    ids: Vec<u16>,
    ends: Vec<usize>,
}
fn chunk_plan(column: &Column) -> Option<ChunkPlan> {
    let Column::Bytes { data, ends, .. } = column else {
        return None;
    };
    if data.len() < (ends.len() - 1) * 8 {
        return None;
    }
    let mut map = HashMap::<[u8; 4], u16>::new();
    let mut plan = ChunkPlan {
        words: Vec::new(),
        counts: Vec::new(),
        ids: Vec::new(),
        ends: vec![0],
    };
    for window in ends.windows(2) {
        for part in data[window[0] as usize..window[1] as usize].chunks(4) {
            let mut word = [0; 4];
            word[..part.len()].copy_from_slice(part);
            let id = if let Some(&id) = map.get(&word) {
                id
            } else {
                if plan.words.len() == 65536 {
                    return None;
                }
                let id = plan.words.len() as u16;
                map.insert(word, id);
                plan.words.push(word);
                plan.counts.push(0);
                id
            };
            plan.counts[id as usize] = plan.counts[id as usize].checked_add(1)?;
            plan.ids.push(id);
        }
        plan.ends.push(plan.ids.len());
    }
    // Conservative raw representation-size filter, not an entropy-size oracle.
    (plan.words.len() * 8 + plan.ids.len() * 2 < data.len()).then_some(plan)
}
impl GeneralTable {
    pub fn rows(&self) -> usize {
        self.rows
    }
    pub fn read(mut source: impl BufRead, kinds: Vec<Kind>, delimiter: u8) -> Result<Self> {
        ensure(matches!(delimiter, b',' | b'|'), "unsupported delimiter")?;
        ensure(!kinds.is_empty() && kinds.len() <= 1024, "invalid schema")?;
        let mut columns: Vec<_> = (0..kinds.len()).map(|_| Column::new()).collect();
        let mut record = Vec::new();
        let mut ranges = Vec::with_capacity(kinds.len());
        let mut rows = 0usize;
        let mut ending = None;
        let mut final_newline = false;
        loop {
            record.clear();
            let mut scan = 0;
            let mut state = 0u8;
            loop {
                let limit = MAX_BLOCK + 1 - record.len();
                let read = std::io::Read::take(&mut source, limit as u64)
                    .read_until(b'\n', &mut record)?;
                ensure(record.len() <= MAX_BLOCK, "record limit exceeded")?;
                if read == 0 {
                    ensure(state != 1, "unclosed quoted field")?;
                    break;
                }
                // Most research tables contain no quoted fields. Avoid the
                // full quote-state pass there; field validation still runs.
                if scan == 0 && !record.contains(&b'"') {
                    break;
                }
                // 0 unquoted/start, 1 quoted, 2 after closing quote. A doubled
                // quote returns from state 2 to 1. Final validation below also
                // rejects quotes in the middle of unquoted fields.
                while scan < record.len() {
                    let b = record[scan];
                    match state {
                        1 => {
                            if b == b'"' {
                                state = 2
                            }
                        }
                        2 => {
                            if b == b'"' {
                                state = 1
                            } else if b == delimiter || b == b'\n' || b == b'\r' {
                                state = 0
                            } else {
                                return Err("characters after closing quote".into());
                            }
                        }
                        _ => {
                            if b == b'"' {
                                state = 1
                            }
                        }
                    }
                    scan += 1;
                }
                if state != 1 {
                    break;
                }
            }
            if record.is_empty() {
                break;
            }
            final_newline = record.last() == Some(&b'\n');
            if final_newline {
                record.pop();
                let this = record.last() == Some(&b'\r');
                if this {
                    record.pop();
                }
                ensure(
                    ending.is_none_or(|v| v == this),
                    "mixed line endings unsupported",
                )?;
                ending = Some(this);
            }
            fields_into(&record, delimiter, &mut ranges)?;
            ensure(ranges.len() == columns.len(), "field count mismatch")?;
            for ((column, kind), range) in columns.iter_mut().zip(&kinds).zip(&ranges) {
                let token = &record[range.clone()];
                // A dictionary hit was already validated on first insertion.
                column.append(token, Some(*kind))?;
            }
            rows += 1;
            ensure(rows <= u32::MAX as usize, "too many rows")?;
        }
        ensure(rows > 0, "empty input unsupported")?;
        for (column, kind) in columns.iter_mut().zip(&kinds) {
            if matches!(kind, Kind::Integer | Kind::Decimal) {
                column.compact_numeric();
            }
        }
        Ok(Self {
            columns,
            kinds,
            rows,
            delimiter,
            crlf: ending.unwrap_or(false),
            final_newline,
        })
    }
}

fn fields(record: &[u8], delimiter: u8) -> Result<Vec<std::ops::Range<usize>>> {
    let mut ranges = Vec::new();
    fields_into(record, delimiter, &mut ranges)?;
    Ok(ranges)
}
fn validate_numeric(token: &[u8], kind: Option<Kind>) -> Result<()> {
    if !matches!(kind, Some(Kind::Integer | Kind::Decimal)) {
        return Ok(());
    }
    let plain = if token.first() == Some(&b'"') {
        &token[1..token.len() - 1]
    } else {
        token
    };
    if plain == b"null" || plain.is_empty() {
        return Ok(());
    }
    let text = std::str::from_utf8(plain)?;
    if matches!(kind, Some(Kind::Integer)) {
        if text.parse::<i64>().is_err() {
            let (value, scale) = crate::fixed::parse(plain).ok_or("invalid integer spelling")?;
            ensure(
                value % 10i64.pow(u32::from(scale)) == 0,
                "fractional INTEGER value",
            )?;
        }
    } else {
        let n = text.parse::<f64>()?;
        ensure(n.is_finite(), "nonfinite number unsupported")?;
    }
    Ok(())
}
fn fields_into(
    record: &[u8],
    delimiter: u8,
    ranges: &mut Vec<std::ops::Range<usize>>,
) -> Result<()> {
    ranges.clear();
    let mut start = 0;
    let mut i = 0;
    loop {
        if record.get(i) == Some(&b'"') {
            i += 1;
            loop {
                let b = *record.get(i).ok_or("unclosed quoted field")?;
                i += 1;
                if b == b'"' {
                    if record.get(i) == Some(&b'"') {
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            ensure(
                i == record.len() || record[i] == delimiter,
                "characters after closing quote",
            )?;
        } else {
            while i < record.len() && record[i] != delimiter {
                ensure(
                    !matches!(record[i], b'"' | b'\r' | b'\n'),
                    "invalid unquoted field",
                )?;
                i += 1;
            }
        }
        ranges.push(start..i);
        if i == record.len() {
            break;
        }
        i += 1;
        start = i;
    }
    Ok(())
}

pub fn compress(table: &GeneralTable, block_rows: usize, lanes: usize) -> Result<Vec<u8>> {
    compress_with_precision(table, block_rows, lanes, 16)
}

/// Same precision policy as the v1 backend; source tokens remain lossless.
pub fn compress_with_precision(
    table: &GeneralTable,
    block_rows: usize,
    lanes: usize,
    bits: u32,
) -> Result<Vec<u8>> {
    compress_config(table, block_rows, lanes, bits, false, false)
}

/// Opt-in independent-record string chunks. Four-byte, row-local aligned
/// substrings become dictionary/DC symbols. No cross-row decoding dependency.
pub fn compress_records(
    table: &GeneralTable,
    lanes: usize,
    bits: u32,
    chunk_strings: bool,
) -> Result<Vec<u8>> {
    compress_config(table, 1, lanes, bits, chunk_strings, false)
}

/// Explicit bulk-throughput profile, not an independent-record format.
/// Eligible <=256-symbol columns use cumulative DC16/64 with full precision.
/// Scalar builds read exactly the same files; AVX-512 is runtime dispatched.
pub fn compress_simd(table: &GeneralTable, block_rows: usize) -> Result<Vec<u8>> {
    ensure(
        block_rows >= 4096,
        "SIMD profile requires at least 4096 rows per block",
    )?;
    compress_config(table, block_rows, 1, 16, false, true)
}

// Limit extra direct-table storage for a many-column archive. Remaining
// eligible columns retain the same format but use scalar cumulative decoding.
fn simd_models<'a>(
    models: impl IntoIterator<Item = Option<&'a Model>>,
    enabled: bool,
) -> Result<Vec<Option<delayed_coding_simd::SimdModel>>> {
    let mut budget = 16usize << 20;
    models
        .into_iter()
        .map(|model| {
            let Some(model) = model.filter(|m| enabled && m.alphabet_size() <= 256) else {
                return Ok(None);
            };
            let frequencies = model.frequencies();
            let needed = if frequencies.iter().all(|f| f % 16 == 0) {
                16384
            } else {
                524288
            };
            let prepared = if needed <= budget {
                delayed_coding_simd::SimdModel::new_cumulative(&frequencies)?
            } else {
                delayed_coding_simd::SimdModel::new_cumulative_scalar(&frequencies)?
            };
            budget -= prepared.decode_table_bytes();
            Ok(Some(prepared))
        })
        .collect()
}

fn compress_config(
    table: &GeneralTable,
    block_rows: usize,
    lanes: usize,
    bits: u32,
    chunk_strings: bool,
    simd: bool,
) -> Result<Vec<u8>> {
    ensure((8..=16).contains(&bits), "precision must be 8..16")?;
    ensure(
        block_rows > 0 && block_rows <= 65536 && matches!(lanes, 1 | 4),
        "invalid coding options",
    )?;
    ensure(
        block_rows * table.columns.len() <= 1 << 22,
        "block dimension limit",
    )?;
    let blocks = table.rows.div_ceil(block_rows);
    let mut out = MAGIC.to_vec();
    let joint = chunk_strings && block_rows == 1;
    out.push(
        u8::from(table.crlf)
            | (u8::from(table.final_newline) << 1)
            | (u8::from(joint) << 2)
            | (u8::from(simd) << 3),
    );
    out.push(table.delimiter);
    out.push(lanes as u8);
    put(&mut out, table.columns.len())?;
    put(&mut out, table.rows)?;
    put(&mut out, block_rows)?;
    let mut models = Vec::new();
    let chunks: Vec<_> = table
        .columns
        .iter()
        .zip(&table.kinds)
        .map(|(column, kind)| {
            if chunk_strings && block_rows == 1 && matches!(kind, Kind::Text) {
                chunk_plan(column)
            } else {
                None
            }
        })
        .collect();
    let prefixes: Vec<bool> = table
        .columns
        .iter()
        .map(|column| {
            if block_rows == 1 || !matches!(column, Column::Bytes { .. }) {
                return false;
            }
            let samples = table.rows.min(4096);
            let saved: usize = (1..samples)
                .filter(|r| r % block_rows != 0)
                .map(|r| common_prefix(column.token(r - 1), column.token(r)))
                .sum();
            saved > samples * 2
        })
        .collect();
    let mut dictionary_bytes = 0usize;
    for (c, (column, kind)) in table.columns.iter().zip(&table.kinds).enumerate() {
        out.push(kind.tag());
        if let Some(plan) = &chunks[c] {
            out.push(4);
            let model = crate::probability_model(&plan.counts, bits)?;
            put(&mut out, plan.words.len())?;
            for (word, frequency) in plan.words.iter().zip(model.frequencies()) {
                out.extend_from_slice(word);
                put(&mut out, frequency as usize)?;
            }
            models.push(model);
            continue;
        }
        match column {
            Column::Fixed { scale, .. } => {
                out.push(2);
                out.push(*scale);
                models.push(Model::new(&[65536])?); // No entropy symbols for this column.
            }
            Column::Dictionary { tokens, counts, .. } => {
                out.push(0);
                let model = crate::probability_model(counts, bits)?;
                put(&mut out, tokens.len())?;
                for (token, freq) in tokens.iter().zip(model.frequencies()) {
                    dictionary_bytes += token.len();
                    ensure(
                        dictionary_bytes <= MAX_BLOCK * 16,
                        "dictionary memory limit",
                    )?;
                    put(&mut out, token.len())?;
                    out.extend_from_slice(token);
                    put(&mut out, freq as usize)?;
                }
                models.push(model);
            }
            Column::Bytes { counts, .. } => {
                out.push(if prefixes[c] { 3 } else { 1 });
                let model = crate::probability_model(counts.as_slice(), bits)?;
                for freq in model.frequencies() {
                    put(&mut out, freq as usize)?;
                }
                models.push(model);
            }
        }
    }
    let simd = simd_models(
        models
            .iter()
            .zip(&table.columns)
            .map(|(m, c)| (!matches!(c, Column::Fixed { .. })).then_some(m)),
        simd,
    )?;
    let mut simd_workspace = delayed_coding_simd::EncodeWorkspace::default();
    let joint_columns: Vec<_> = table
        .columns
        .iter()
        .zip(&table.kinds)
        .map(|(c, k)| match c {
            Column::Dictionary { ids, tokens, .. } if matches!(k, Kind::Enum) => {
                Some((ids.as_slice(), tokens.len()))
            }
            _ => None,
        })
        .collect();
    let groups = if joint {
        crate::joint::build(&joint_columns, table.rows, bits)?
    } else {
        Vec::new()
    };
    if joint {
        crate::joint::write(&groups, &mut out);
    }
    let index = out.len();
    out.resize(index + 4 * (blocks + 1), 0);
    let mut symbols = Vec::new();
    let mut storage = Vec::new();
    let mut workspace = Workspace::default();
    let mut events = Vec::new();
    for block in 0..blocks {
        set(&mut out, index + block * 4)?;
        let begin = block * block_rows;
        let end = (begin + block_rows).min(table.rows);
        let mut decoded_size = (end - begin) * (table.columns.len() + 2);
        for column in &table.columns {
            for row in begin..end {
                decoded_size += if matches!(column, Column::Fixed { .. }) {
                    40
                } else {
                    column.token(row).len()
                };
                ensure(
                    decoded_size <= MAX_BLOCK,
                    "decoded block limit; use fewer block rows",
                )?;
            }
        }
        if block_rows == 1 {
            events.clear();
            for (c, (column, model)) in table.columns.iter().zip(&models).enumerate() {
                match column {
                    Column::Fixed { values, .. } => var64(&mut out, zigzag(values[begin])),
                    Column::Dictionary { ids, .. } if !joint || joint_columns[c].is_none() => {
                        events.push(Event {
                            model,
                            symbol: u32::from(ids[begin]),
                        })
                    }
                    Column::Dictionary { .. } => (),
                    Column::Bytes { .. } => {
                        let token = column.token(begin);
                        var(&mut out, token.len());
                        if let Some(plan) = &chunks[c] {
                            events.extend(
                                plan.ids[plan.ends[begin]..plan.ends[begin + 1]].iter().map(
                                    |&id| Event {
                                        model,
                                        symbol: u32::from(id),
                                    },
                                ),
                            );
                        } else {
                            events.extend(token.iter().map(|&b| Event {
                                model,
                                symbol: u32::from(b),
                            }));
                        }
                    }
                }
                ensure(events.len() <= MAX_BLOCK, "record symbol limit")?;
            }
            for group in &groups {
                let mut symbol = 0;
                for (&c, &radix) in group.columns.iter().zip(&group.radices) {
                    symbol = symbol * radix + u32::from(joint_columns[c].unwrap().0[begin]);
                }
                events.push(Event {
                    model: &group.model,
                    symbol,
                });
            }
            storage.resize(events.len() * 2, 0);
            let range = if lanes == 1 {
                encode_events_interleaved_into::<16, 1>(&events, &mut storage, &mut workspace)?
            } else {
                encode_events_interleaved_into::<16, 4>(&events, &mut storage, &mut workspace)?
            };
            out.extend_from_slice(&storage[range]);
        } else {
            for (c, (column, model)) in table.columns.iter().zip(&models).enumerate() {
                let length = out.len();
                put(&mut out, 0)?;
                symbols.clear();
                match column {
                    Column::Fixed { values, .. } => {
                        crate::archive::pack_integers(&values[begin..end], &mut out);
                        let bytes = u32::try_from(out.len() - length - 4)?;
                        out[length..length + 4].copy_from_slice(&bytes.to_le_bytes());
                        continue;
                    }
                    Column::Dictionary { ids, .. } => {
                        symbols.extend(ids[begin..end].iter().map(|&v| u32::from(v)))
                    }
                    Column::Bytes { data, ends, .. } => {
                        let start = ends[begin] as usize;
                        let finish = ends[end] as usize;
                        ensure(finish - start <= MAX_BLOCK, "block byte limit")?;
                        if prefixes[c] {
                            let mut previous: &[u8] = &[];
                            for row in begin..end {
                                let token = column.token(row);
                                let prefix = common_prefix(previous, token);
                                var(&mut out, prefix);
                                var(&mut out, token.len() - prefix);
                                symbols.extend(token[prefix..].iter().map(|&v| u32::from(v)));
                                previous = token;
                            }
                        } else {
                            for pair in ends[begin..=end].windows(2) {
                                var(&mut out, (pair[1] - pair[0]) as usize);
                            }
                            symbols.extend(data[start..finish].iter().map(|&v| u32::from(v)));
                        }
                    }
                }
                storage.resize(symbols.len() * 2, 0);
                let range = if let Some(model) = &simd[c] {
                    model.encode64_into(&symbols, &mut storage, &mut simd_workspace)?
                } else if lanes == 1 {
                    encode_interleaved_into::<16, 1>(model, &symbols, &mut storage, &mut workspace)?
                } else {
                    encode_interleaved_into::<16, 4>(model, &symbols, &mut storage, &mut workspace)?
                };
                out.extend_from_slice(&storage[range]);
                let bytes = u32::try_from(out.len() - length - 4)?;
                out[length..length + 4].copy_from_slice(&bytes.to_le_bytes());
            }
        }
    }
    set(&mut out, index + blocks * 4)?;
    let crc = crate::archive::crc32(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

enum Spec<'a> {
    Fixed { scale: u8 },
    Dictionary { tokens: Vec<&'a [u8]>, model: Model },
    Bytes { model: Model, prefix: bool },
    Chunks { model: Model, words: Vec<[u8; 4]> },
}
#[derive(Default)]
pub struct GeneralBlock {
    prefixes: Vec<usize>,
    numbers: Vec<i64>,
    ids: Vec<Vec<u32>>,
    data: Vec<Vec<u8>>,
    ends: Vec<Vec<usize>>,
    rows: usize,
}
pub struct GeneralArchive<'a> {
    bytes: &'a [u8],
    specs: Vec<Spec<'a>>,
    kinds: Vec<Kind>,
    offsets: Vec<u32>,
    rows: usize,
    block_rows: usize,
    lanes: usize,
    delimiter: u8,
    flags: u8,
    groups: Option<Vec<crate::joint::Group>>,
    simd: Vec<Option<delayed_coding_simd::SimdModel>>,
}
impl<'a> GeneralArchive<'a> {
    pub fn open(bytes: &'a [u8]) -> Result<Self> {
        ensure(bytes.len() >= 31, "truncated v2 header")?;
        let end = bytes.len() - 4;
        ensure(
            crate::archive::crc32(&bytes[..end]) == u32::from_le_bytes(bytes[end..].try_into()?),
            "checksum mismatch",
        )?;
        let mut r = Reader {
            bytes: &bytes[..end],
            p: 0,
        };
        ensure(r.take(8)? == MAGIC, "unsupported version")?;
        let flags = r.byte()?;
        let delimiter = r.byte()?;
        let lanes = r.byte()? as usize;
        ensure(
            flags & !15 == 0 && matches!(delimiter, b',' | b'|') && matches!(lanes, 1 | 4),
            "invalid options",
        )?;
        let columns = r.word()?;
        let rows = r.word()?;
        let block_rows = r.word()?;
        ensure(
            columns > 0
                && columns <= 1024
                && rows > 0
                && block_rows > 0
                && block_rows <= 65536
                && block_rows * columns <= 1 << 22,
            "invalid dimensions",
        )?;
        ensure(
            flags & 8 == 0 || (block_rows >= 4096 && flags & 4 == 0 && lanes == 1),
            "invalid SIMD bulk profile",
        )?;
        let mut specs = Vec::new();
        let mut kinds = Vec::new();
        let mut dictionary_bytes = 0usize;
        for _ in 0..columns {
            kinds.push(Kind::from_tag(r.byte()?)?);
            specs.push(match r.byte()? {
                0 => {
                    let n = r.word()?;
                    ensure(
                        n > 0 && n <= MAX_DICTIONARY && n <= r.left() / 8,
                        "invalid dictionary size",
                    )?;
                    let mut tokens = Vec::with_capacity(n);
                    let mut freq = Vec::with_capacity(n);
                    for _ in 0..n {
                        let len = r.word()?;
                        ensure(len <= MAX_TOKEN, "token limit")?;
                        dictionary_bytes += len;
                        ensure(
                            dictionary_bytes <= MAX_BLOCK * 16,
                            "dictionary memory limit",
                        )?;
                        let token = r.take(len)?;
                        let token_fields = fields(token, delimiter)?;
                        ensure(token_fields.len() == 1, "invalid dictionary token")?;
                        tokens.push(token);
                        let f = r.word()?;
                        ensure(f > 0 && f <= 65536, "invalid frequency")?;
                        freq.push(f as u32);
                    }
                    Spec::Dictionary {
                        tokens,
                        model: crate::decoding_model(Model::new(&freq)?),
                    }
                }
                codec @ (1 | 3) => {
                    ensure(
                        codec != 3 || block_rows > 1,
                        "prefix codec requires bulk blocks",
                    )?;
                    let mut freq = vec![0; 256];
                    for f in &mut freq {
                        *f = r.word()? as u32;
                    }
                    Spec::Bytes {
                        model: crate::decoding_model(Model::new(&freq)?),
                        prefix: codec == 3,
                    }
                }
                2 => {
                    let scale = r.byte()?;
                    ensure(
                        scale <= 18 && matches!(kinds.last(), Some(Kind::Integer | Kind::Decimal)),
                        "invalid fixed decimal",
                    )?;
                    Spec::Fixed { scale }
                }
                4 => {
                    ensure(
                        block_rows == 1 && matches!(kinds.last(), Some(Kind::Text)),
                        "chunk codec requires independent string records",
                    )?;
                    let n = r.word()?;
                    ensure(
                        n > 0 && n <= 65536 && n <= r.left() / 8,
                        "invalid chunk alphabet",
                    )?;
                    let mut words = Vec::with_capacity(n);
                    let mut frequencies = Vec::with_capacity(n);
                    for _ in 0..n {
                        words.push(r.take(4)?.try_into()?);
                        frequencies.push(r.word()? as u32);
                    }
                    Spec::Chunks {
                        model: crate::decoding_model(Model::new(&frequencies)?),
                        words,
                    }
                }
                _ => return Err("unknown column codec".into()),
            });
        }
        let groups = if flags & 4 != 0 {
            ensure(block_rows == 1, "joint models require records")?;
            let sizes: Vec<_> = specs
                .iter()
                .zip(&kinds)
                .map(|(s, k)| match s {
                    Spec::Dictionary { tokens, .. } if matches!(k, Kind::Enum) => {
                        Some(tokens.len())
                    }
                    _ => None,
                })
                .collect();
            let (groups, consumed) = crate::joint::read(&r.bytes[r.p..], &sizes)?;
            r.take(consumed)?;
            Some(groups)
        } else {
            None
        };
        let blocks = rows.div_ceil(block_rows);
        ensure(blocks < r.left() / 4, "truncated index")?;
        let mut offsets = Vec::with_capacity(blocks + 1);
        for _ in 0..=blocks {
            offsets.push(r.word()? as u32);
        }
        ensure(
            offsets[0] as usize == r.p
                && offsets[blocks] as usize == end
                && offsets
                    .windows(2)
                    .all(|w| w[0] < w[1] && w[1] as usize <= end),
            "invalid index",
        )?;
        let simd = simd_models(
            specs.iter().map(|s| match s {
                Spec::Dictionary { model, .. } | Spec::Bytes { model, .. } => Some(model),
                _ => None,
            }),
            flags & 8 != 0,
        )?;
        Ok(Self {
            bytes: &bytes[..end],
            specs,
            kinds,
            offsets,
            rows,
            block_rows,
            lanes,
            delimiter,
            flags,
            groups,
            simd,
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
    pub fn states(&self) -> usize {
        self.lanes
    }
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }
    pub fn kinds(&self) -> &[Kind] {
        &self.kinds
    }
    /// Prepare numeric dictionaries and logical strings once, then decode
    /// directly to typed rows. Existing lexical APIs remain unchanged.
    pub fn record_reader(&self) -> Result<crate::record::RecordReader<'_>> {
        use crate::record::{dictionary, EntropyField, Mapping, Prefix, RecordReader};
        ensure(
            self.block_rows == 1,
            "record reader requires one row per block",
        )?;
        let mut prefixes = Vec::new();
        let mut entropy = Vec::new();
        for (column, (spec, &kind)) in self.specs.iter().zip(&self.kinds).enumerate() {
            match spec {
                Spec::Fixed { scale } => prefixes.push(Prefix::Fixed {
                    column,
                    scale: *scale,
                    kind,
                }),
                Spec::Dictionary { model, tokens }
                    if self.groups.is_none() || !matches!(kind, Kind::Enum) =>
                {
                    entropy.push(EntropyField {
                        column,
                        model,
                        mapping: dictionary(tokens, kind)?,
                    })
                }
                Spec::Dictionary { .. } => (),
                Spec::Bytes { model, .. } => {
                    prefixes.push(Prefix::Length(column));
                    entropy.push(EntropyField {
                        column,
                        model,
                        mapping: Mapping::Bytes(kind),
                    });
                }
                Spec::Chunks { model, words } => {
                    prefixes.push(Prefix::Length(column));
                    entropy.push(EntropyField {
                        column,
                        model,
                        mapping: Mapping::Chunks(words.clone()),
                    });
                }
            }
        }
        if let Some(groups) = &self.groups {
            for group in groups {
                entropy.push(EntropyField {
                    column: group.columns[0],
                    model: &group.model,
                    mapping: crate::joint::mapping(group),
                });
            }
        }
        Ok(RecordReader {
            fused: Vec::new(),
            dictionary_only: entropy
                .iter()
                .all(|f| !matches!(f.mapping, Mapping::Bytes(_) | Mapping::Chunks(_))),
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
    pub fn column_codec(&self, column: usize) -> Option<&'static str> {
        if self.simd.get(column)?.is_some() {
            return Some(match self.specs.get(column)? {
                Spec::Dictionary { .. } => "dc64_cumulative_dictionary",
                Spec::Bytes { prefix: true, .. } => "prefix_dc64_cumulative_bytes",
                _ => "dc64_cumulative_bytes",
            });
        }
        Some(match self.specs.get(column)? {
            Spec::Dictionary { .. }
                if self.groups.is_some() && matches!(self.kinds[column], Kind::Enum) =>
            {
                "dc_joint_dictionary"
            }
            Spec::Dictionary { .. } => "dc_dictionary",
            Spec::Bytes { prefix: false, .. } => "dc_bytes",
            Spec::Bytes { prefix: true, .. } => "prefix_dc_bytes",
            Spec::Fixed { .. } => "exact_fixed_decimal",
            Spec::Chunks { .. } => "dc_string_chunks4",
        })
    }
    /// Backend used for an eligible SIMD-profile column; None for ordinary DC.
    pub fn column_backend(&self, column: usize) -> Option<&'static str> {
        self.simd.get(column)?.as_ref().map(|m| m.backend())
    }
    pub fn simd_prepared_bytes(&self) -> usize {
        self.simd.iter().flatten().map(|m| m.memory_bytes()).sum()
    }
    fn decode_column(
        &self,
        column: usize,
        model: &Model,
        payload: &[u8],
        out: &mut [u32],
    ) -> Result<()> {
        if let Some(model) = &self.simd[column] {
            model.decode64_bounded(payload, out)?;
            Ok(())
        } else {
            decode(model, payload, out, self.lanes)
        }
    }
    /// Resolve a category ID returned by a prepared record reader. IDs are
    /// archive-local, not portable between separately fitted dictionaries.
    pub fn category_token(&self, column: usize, id: u32) -> Option<&[u8]> {
        match self.specs.get(column)? {
            Spec::Dictionary { tokens, .. } if matches!(self.kinds[column], Kind::Enum) => {
                tokens.get(id as usize).copied()
            }
            _ => None,
        }
    }
    pub fn token<'b>(&'b self, b: &'b GeneralBlock, column: usize, row: usize) -> Option<&'b [u8]> {
        if row >= b.rows {
            return None;
        }
        match self.specs.get(column)? {
            Spec::Dictionary { tokens, .. } => {
                tokens.get(*b.ids.get(column)?.get(row)? as usize).copied()
            }
            Spec::Bytes { .. } | Spec::Fixed { .. } | Spec::Chunks { .. } => {
                let ends = b.ends.get(column)?;
                b.data
                    .get(column)?
                    .get(*ends.get(row)?..*ends.get(row + 1)?)
            }
        }
    }
    pub fn decode_block(&self, index: usize, b: &mut GeneralBlock) -> Result<()> {
        b.rows = 0;
        ensure(index < self.blocks(), "block out of range")?;
        b.rows = 0;
        b.ids.resize_with(self.columns(), Vec::new);
        b.data.resize_with(self.columns(), Vec::new);
        b.ends.resize_with(self.columns(), Vec::new);
        let rows = (self.rows - index * self.block_rows).min(self.block_rows);
        let mut r = Reader {
            bytes: &self.bytes[self.offsets[index] as usize..self.offsets[index + 1] as usize],
            p: 0,
        };
        if self.block_rows == 1 {
            let mut total = 0usize;
            for (c, spec) in self.specs.iter().enumerate() {
                b.ids[c].clear();
                b.data[c].clear();
                b.ends[c].clear();
                b.ends[c].push(0);
                if let Spec::Fixed { scale } = spec {
                    let value = unzigzag(r.var64()?);
                    crate::fixed::append(&mut b.data[c], value, *scale);
                    b.ends[c].push(b.data[c].len());
                }
                if matches!(spec, Spec::Bytes { .. } | Spec::Chunks { .. }) {
                    let len = r.var()?;
                    ensure(len <= MAX_TOKEN, "token limit")?;
                    total += len;
                    ensure(total <= MAX_BLOCK, "record limit")?;
                    b.ends[c].push(len);
                }
            }
            let payload = r.take(r.left())?;
            if self.lanes == 1 {
                self.record::<1>(payload, b)?
            } else {
                self.record::<4>(payload, b)?
            }
        } else {
            let mut decoded_size = rows * (self.columns() + 2);
            for (c, spec) in self.specs.iter().enumerate() {
                let len = r.word()?;
                let payload = r.take(len)?;
                match spec {
                    Spec::Chunks { .. } => return Err("chunk codec requires records".into()),
                    Spec::Fixed { scale } => {
                        crate::archive::unpack_integers(payload, rows, &mut b.numbers)?;
                        b.data[c].clear();
                        b.ends[c].clear();
                        b.ends[c].push(0);
                        for &value in &b.numbers {
                            crate::fixed::append(&mut b.data[c], value, *scale);
                            b.ends[c].push(b.data[c].len());
                        }
                        decoded_size += b.data[c].len();
                        ensure(decoded_size <= MAX_BLOCK, "decoded block limit")?;
                    }
                    Spec::Dictionary { model, tokens } => {
                        b.ids[c].resize(rows, 0);
                        self.decode_column(c, model, payload, &mut b.ids[c])?;
                        for &id in &b.ids[c] {
                            decoded_size += tokens[id as usize].len();
                            ensure(decoded_size <= MAX_BLOCK, "decoded block limit")?;
                        }
                    }
                    Spec::Bytes { model, prefix } => {
                        let mut p = Reader {
                            bytes: payload,
                            p: 0,
                        };
                        b.ends[c].clear();
                        b.ends[c].push(0);
                        b.prefixes.clear();
                        let mut total = 0;
                        let mut previous_len = 0;
                        let mut decoded_len = 0;
                        for _ in 0..rows {
                            let shared = if *prefix { p.var()? } else { 0 };
                            let len = p.var()?;
                            ensure(
                                shared <= previous_len && shared + len <= MAX_TOKEN,
                                "invalid prefix or token limit",
                            )?;
                            total += len;
                            ensure(total <= MAX_BLOCK, "block limit")?;
                            decoded_len += shared + len;
                            ensure(decoded_len <= MAX_BLOCK, "decoded block limit")?;
                            b.ends[c].push(decoded_len);
                            b.prefixes.push(shared);
                            previous_len = shared + len;
                        }
                        decoded_size += decoded_len;
                        ensure(decoded_size <= MAX_BLOCK, "decoded block limit")?;
                        b.ids[c].resize(total, 0);
                        self.decode_column(c, model, p.take(p.left())?, &mut b.ids[c])?;
                        b.data[c].clear();
                        if *prefix {
                            let mut symbol = 0;
                            for row in 0..rows {
                                let shared = b.prefixes[row];
                                let from = if row == 0 { 0 } else { b.ends[c][row - 1] };
                                b.data[c].extend_from_within(from..from + shared);
                                let suffix = b.ends[c][row + 1] - b.ends[c][row] - shared;
                                b.data[c].extend(
                                    b.ids[c][symbol..symbol + suffix].iter().map(|&s| s as u8),
                                );
                                symbol += suffix;
                            }
                        } else {
                            b.data[c].extend(b.ids[c].iter().map(|&s| s as u8));
                        }
                    }
                }
            }
            ensure(r.left() == 0, "trailing block data")?;
        }
        if self.block_rows == 1 {
            let mut decoded_size = rows * (self.columns() + 2);
            for (column, spec) in self.specs.iter().enumerate() {
                match spec {
                    Spec::Dictionary { tokens, .. } => {
                        for &id in &b.ids[column] {
                            decoded_size += tokens[id as usize].len();
                            ensure(decoded_size <= MAX_BLOCK, "decoded block limit")?;
                        }
                    }
                    Spec::Bytes { .. } | Spec::Fixed { .. } | Spec::Chunks { .. } => {
                        decoded_size += b.data[column].len();
                        ensure(decoded_size <= MAX_BLOCK, "decoded block limit")?;
                    }
                }
            }
        }
        b.rows = rows;
        Ok(())
    }
    fn record<const LANES: usize>(&self, payload: &[u8], b: &mut GeneralBlock) -> Result<()> {
        let mut decoder = Decoder::<16, LANES>::new(payload)?;
        for (c, spec) in self.specs.iter().enumerate() {
            match spec {
                Spec::Fixed { .. } => (),
                Spec::Dictionary { model, .. }
                    if self.groups.is_none() || !matches!(self.kinds[c], Kind::Enum) =>
                {
                    b.ids[c].push(decoder.read(model)?)
                }
                Spec::Dictionary { .. } => (),
                Spec::Bytes { model, .. } => {
                    for _ in 0..b.ends[c][1] {
                        b.data[c].push(decoder.read(model)? as u8);
                    }
                }
                Spec::Chunks { model, words } => {
                    let len = b.ends[c][1];
                    for _ in 0..len.div_ceil(4) {
                        b.data[c].extend_from_slice(&words[decoder.read(model)? as usize]);
                    }
                    ensure(
                        b.data[c][len..].iter().all(|&v| v == 0),
                        "nonzero chunk padding",
                    )?;
                    b.data[c].truncate(len);
                }
            }
        }
        if let Some(groups) = &self.groups {
            for group in groups {
                let mut id = decoder.read(&group.model)?;
                for (&c, &radix) in group.columns.iter().zip(&group.radices).rev() {
                    b.ids[c].push(id % radix);
                    id /= radix;
                }
            }
        }
        decoder.finish()?;
        Ok(())
    }
    pub fn locate_row(&self, row: usize, b: &mut GeneralBlock) -> Result<usize> {
        b.rows = 0;
        ensure(row < self.rows, "row out of range")?;
        self.decode_block(row / self.block_rows, b)?;
        Ok(row % self.block_rows)
    }
    pub fn write_csv(&self, mut writer: impl Write) -> Result<()> {
        let mut block = GeneralBlock::default();
        let mut output = Vec::new();
        for index in 0..self.blocks() {
            self.decode_block(index, &mut block)?;
            output.clear();
            for row in 0..block.rows {
                for c in 0..self.columns() {
                    if c != 0 {
                        output.push(self.delimiter);
                    }
                    output.extend_from_slice(
                        self.token(&block, c, row).ok_or("invalid decoded token")?,
                    );
                }
                if self.flags & 2 != 0 || index * self.block_rows + row + 1 < self.rows {
                    if self.flags & 1 != 0 {
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

fn decode(model: &Model, payload: &[u8], out: &mut [u32], lanes: usize) -> Result<()> {
    if lanes == 1 {
        decode_into::<16>(model, payload, out)?
    } else {
        decode_grouped4_into::<16>(model, payload, out)?
    }
    Ok(())
}
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(a, b)| a == b).count()
}
fn put(out: &mut Vec<u8>, n: usize) -> Result<()> {
    out.extend_from_slice(&u32::try_from(n)?.to_le_bytes());
    Ok(())
}
fn set(out: &mut [u8], position: usize) -> Result<()> {
    let n = u32::try_from(out.len())?;
    out[position..position + 4].copy_from_slice(&n.to_le_bytes());
    Ok(())
}
fn var(out: &mut Vec<u8>, mut n: usize) {
    while n >= 128 {
        out.push(n as u8 | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
fn zigzag(n: i64) -> u64 {
    ((n as u64) << 1) ^ ((n >> 63) as u64)
}
fn unzigzag(n: u64) -> i64 {
    ((n >> 1) as i64) ^ -((n & 1) as i64)
}
fn var64(out: &mut Vec<u8>, mut n: u64) {
    while n >= 128 {
        out.push(n as u8 | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
struct Reader<'a> {
    bytes: &'a [u8],
    p: usize,
}
impl<'a> Reader<'a> {
    fn left(&self) -> usize {
        self.bytes.len() - self.p
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        ensure(n <= self.left(), "truncated data")?;
        let p = self.p;
        self.p += n;
        Ok(&self.bytes[p..self.p])
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn word(&mut self) -> Result<usize> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into()?) as usize)
    }
    fn var(&mut self) -> Result<usize> {
        let mut n = 0u32;
        for i in 0..5 {
            let b = self.byte()?;
            ensure(i < 4 || b < 16, "length overflow")?;
            n |= u32::from(b & 127) << (7 * i);
            if b < 128 {
                ensure(i == 0 || b != 0, "noncanonical length")?;
                return Ok(n as usize);
            }
        }
        Err("invalid length".into())
    }
    fn var64(&mut self) -> Result<u64> {
        let mut n = 0u64;
        for i in 0..10 {
            let b = self.byte()?;
            ensure(i < 9 || b <= 1, "integer overflow")?;
            n |= u64::from(b & 127) << (7 * i);
            if b < 128 {
                ensure(i == 0 || b != 0, "noncanonical integer")?;
                return Ok(n);
            }
        }
        Err("integer overflow".into())
    }
}
