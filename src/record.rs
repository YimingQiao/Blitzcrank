//! Prepared, typed independent-record reads. No decoded-row cache, cross-row
//! dependencies, or CSV formatting in the normal numeric path. Dictionaries are
//! converted once during preparation; byte-coded numbers are parsed per query.
use crate::{ensure, general::Kind, Result};
use delayed_coding::{DecodeModel, Decoder, Model};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Decimal(f64),
    Category(u32),
    Text { start: usize, len: usize },
}

/// Caller-owned reusable output. Text is logical unquoted data, not CSV lexemes.
#[derive(Default)]
pub struct Record {
    values: Vec<Value>,
    text: Vec<u8>,
    lengths: Vec<usize>,
    symbols: Vec<u32>,
    valid: bool,
}
impl Record {
    pub fn values(&self) -> &[Value] {
        if self.valid {
            &self.values
        } else {
            &[]
        }
    }
    pub fn text(&self, column: usize) -> Option<&[u8]> {
        match self.values().get(column)? {
            Value::Text { start, len } => self.text.get(*start..start + len),
            _ => None,
        }
    }
}

pub(crate) enum Mapping {
    Category,
    Numbers(Vec<Value>),
    Decimals(Vec<f64>),
    Text(Vec<Vec<u8>>),
    Bytes(Kind),
}
pub(crate) struct EntropyField<'a> {
    pub column: usize,
    pub model: &'a Model,
    pub mapping: Mapping,
}
pub(crate) enum Prefix {
    Integer(usize),
    Fixed {
        column: usize,
        scale: u8,
        kind: Kind,
    },
    Length(usize),
}

/// Immutable prepared plan; borrow a resident archive and share between threads.
/// Each reader needs its own `Record`. Preparation includes typed dictionaries.
pub struct RecordReader<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) offsets: &'a [u32],
    pub(crate) columns: usize,
    pub(crate) lanes: usize,
    pub(crate) prefixes: Vec<Prefix>,
    pub(crate) entropy: Vec<EntropyField<'a>>,
    pub(crate) models: Vec<DecodeModel>,
    pub(crate) dictionary_only: bool,
}
impl RecordReader<'_> {
    pub fn rows(&self) -> usize {
        self.offsets.len() - 1
    }
    pub fn columns(&self) -> usize {
        self.columns
    }
    /// Additional prepared-plan heap storage, not archive/model/index storage.
    pub fn prepared_bytes(&self) -> usize {
        self.prefixes.capacity() * std::mem::size_of::<Prefix>()
            + self.entropy.capacity() * std::mem::size_of::<EntropyField<'_>>()
            + self
                .entropy
                .iter()
                .map(|f| match &f.mapping {
                    Mapping::Numbers(v) => v.capacity() * std::mem::size_of::<Value>(),
                    Mapping::Decimals(v) => v.capacity() * 8,
                    Mapping::Text(v) => {
                        v.capacity() * std::mem::size_of::<Vec<u8>>()
                            + v.iter().map(Vec::capacity).sum::<usize>()
                    }
                    _ => 0,
                })
                .sum::<usize>()
            + self.models.capacity() * std::mem::size_of::<DecodeModel>()
            + self
                .models
                .iter()
                .map(DecodeModel::memory_bytes)
                .sum::<usize>()
    }
    pub fn read(&self, row: usize, out: &mut Record) -> Result<()> {
        out.valid = false;
        ensure(row < self.rows(), "row out of range")?;
        out.values.resize(self.columns, Value::Null);
        out.lengths.resize(self.columns, 0);
        out.text.clear();
        let payload = &self.bytes[self.offsets[row] as usize..self.offsets[row + 1] as usize];
        let mut position = 0;
        let mut total = 0;
        for prefix in &self.prefixes {
            let code = varint(payload, &mut position)?;
            let integer = ((code >> 1) as i64) ^ -((code & 1) as i64);
            match *prefix {
                Prefix::Integer(c) => out.values[c] = Value::Integer(integer),
                Prefix::Fixed {
                    column,
                    scale,
                    kind,
                } => {
                    out.values[column] = fixed_value(integer, scale, kind)?;
                }
                Prefix::Length(c) => {
                    ensure(code <= 1 << 20, "token limit")?;
                    total += code as usize;
                    ensure(total <= 64 << 20, "record limit")?;
                    out.lengths[c] = code as usize;
                }
            }
        }
        if self.lanes == 1 {
            self.decode::<1>(&payload[position..], out)?;
        } else {
            self.decode::<4>(&payload[position..], out)?;
        }
        out.valid = true;
        Ok(())
    }
    fn decode<const LANES: usize>(&self, payload: &[u8], out: &mut Record) -> Result<()> {
        let mut decoder = Decoder::<16, LANES>::new(payload)?;
        let buffered = LANES == 4 && self.dictionary_only;
        if buffered {
            out.symbols.resize(self.models.len(), 0);
            delayed_coding::decode_prepared_into::<16, LANES>(
                &self.models,
                payload,
                &mut out.symbols,
            )?;
        }
        for (i, (field, model)) in self.entropy.iter().zip(&self.models).enumerate() {
            let c = field.column;
            let symbol = if matches!(field.mapping, Mapping::Bytes(_)) {
                0
            } else if buffered {
                out.symbols[i]
            } else {
                decoder.read_prepared(model)?
            };
            let value = match &field.mapping {
                Mapping::Category => Value::Category(symbol),
                Mapping::Numbers(values) => values[symbol as usize],
                Mapping::Decimals(values) => {
                    let n = values[symbol as usize];
                    if n.is_nan() {
                        Value::Null
                    } else {
                        Value::Decimal(n)
                    }
                }
                Mapping::Text(tokens) => {
                    let token = &tokens[symbol as usize];
                    let start = out.text.len();
                    ensure(start + token.len() <= 64 << 20, "record text limit")?;
                    out.text.extend_from_slice(token);
                    Value::Text {
                        start,
                        len: token.len(),
                    }
                }
                Mapping::Bytes(kind) => {
                    let start = out.text.len();
                    let len = out.lengths[c];
                    ensure(start + len <= 64 << 20, "record text limit")?;
                    out.text.reserve(len);
                    let mut remaining = len;
                    if LANES == 4 {
                        while remaining >= 4 {
                            let ids = decoder.read_prepared4([model; 4])?;
                            out.text.extend(ids.map(|s| s as u8));
                            remaining -= 4;
                        }
                    }
                    for _ in 0..remaining {
                        out.text.push(decoder.read_prepared(model)? as u8);
                    }
                    if matches!(kind, Kind::Integer | Kind::Decimal) {
                        let value = number(&out.text[start..], *kind)?;
                        out.text.truncate(start);
                        value
                    } else {
                        // Decode doubled quotes in place; raw, non-UTF8 strings
                        // remain bytes. CSV syntax was checked by the producer.
                        let len = unquote_in_place(&mut out.text[start..])?;
                        out.text.truncate(start + len);
                        Value::Text { start, len }
                    }
                }
            };
            out.values[c] = value;
        }
        if !buffered {
            decoder.finish()?;
        }
        Ok(())
    }
}

pub(crate) fn dictionary(tokens: &[&[u8]], kind: Kind) -> Result<Mapping> {
    Ok(match kind {
        Kind::Enum => Mapping::Category,
        Kind::Integer => Mapping::Numbers(
            tokens
                .iter()
                .map(|t| number(t, kind))
                .collect::<Result<_>>()?,
        ),
        Kind::Decimal => Mapping::Decimals(
            tokens
                .iter()
                .map(|t| match number(t, kind)? {
                    Value::Decimal(n) => Ok(n),
                    Value::Null => Ok(f64::NAN),
                    _ => unreachable!(),
                })
                .collect::<Result<_>>()?,
        ),
        Kind::Text => Mapping::Text(
            tokens
                .iter()
                .map(|t| {
                    let mut bytes = t.to_vec();
                    let len = unquote_in_place(&mut bytes)?;
                    bytes.truncate(len);
                    Ok(bytes)
                })
                .collect::<Result<_>>()?,
        ),
    })
}

pub(crate) fn number(token: &[u8], kind: Kind) -> Result<Value> {
    let plain = if token.first() == Some(&b'"') {
        ensure(
            token.len() >= 2 && token.last() == Some(&b'"'),
            "invalid numeric quoting",
        )?;
        &token[1..token.len() - 1]
    } else {
        token
    };
    if plain.is_empty() || plain == b"null" {
        return Ok(Value::Null);
    }
    let text = std::str::from_utf8(plain)?;
    if matches!(kind, Kind::Integer) {
        if let Ok(n) = text.parse::<i64>() {
            return Ok(Value::Integer(n));
        }
        let (n, scale) = crate::fixed::parse(plain).ok_or("invalid integer")?;
        fixed_value(n, scale, kind)
    } else {
        let n: f64 = text.parse()?;
        ensure(n.is_finite(), "nonfinite decimal")?;
        Ok(Value::Decimal(n))
    }
}

fn fixed_value(n: i64, scale: u8, kind: Kind) -> Result<Value> {
    let divisor = 10i64.pow(u32::from(scale));
    if matches!(kind, Kind::Integer) {
        ensure(n % divisor == 0, "nonintegral INTEGER")?;
        Ok(Value::Integer(n / divisor))
    } else if n.unsigned_abs() <= 1 << 53 {
        // Both operands are exactly representable (10^0 through 10^18).
        // One correctly rounded division, not an approximate reciprocal.
        Ok(Value::Decimal(n as f64 / divisor as f64))
    } else {
        let mut token = Vec::new();
        crate::fixed::append(&mut token, n, scale);
        number(&token, Kind::Decimal)
    }
}

fn unquote_in_place(bytes: &mut [u8]) -> Result<usize> {
    if bytes.first() != Some(&b'"') {
        return Ok(bytes.len());
    }
    ensure(
        bytes.len() >= 2 && bytes.last() == Some(&b'"'),
        "invalid string quoting",
    )?;
    let mut read = 1;
    let mut write = 0;
    while read < bytes.len() - 1 {
        let b = bytes[read];
        read += 1;
        if b == b'"' {
            ensure(
                read < bytes.len() - 1 && bytes[read] == b'"',
                "invalid doubled quote",
            )?;
            read += 1;
        }
        bytes[write] = b;
        write += 1;
    }
    Ok(write)
}

#[inline]
fn varint(bytes: &[u8], position: &mut usize) -> Result<u64> {
    let mut value = 0;
    for shift in 0..10 {
        let b = *bytes.get(*position).ok_or("truncated varint")?;
        *position += 1;
        ensure(shift < 9 || b <= 1, "varint overflow")?;
        value |= u64::from(b & 127) << (shift * 7);
        if b < 128 {
            ensure(shift == 0 || b != 0, "noncanonical varint")?;
            return Ok(value);
        }
    }
    Err("invalid varint".into())
}
