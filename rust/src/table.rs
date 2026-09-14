use crate::{ensure, Result};
use std::{collections::HashMap, io::BufRead};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Field {
    Integer,
    Enum,
}

#[derive(Clone, Debug)]
pub struct Schema(pub Vec<Field>);

impl Schema {
    /// Accept INTEGER/ENUM schemas. Integers remain exact for every accepted
    /// nonnegative tolerance; this parser never quantizes values.
    pub fn parse(text: &str) -> Result<Self> {
        let mut fields = Vec::new();
        for line in text.lines().filter(|s| !s.trim().is_empty()) {
            let words: Vec<_> = line.split_whitespace().collect();
            fields.push(match words.as_slice() {
                ["INTEGER", tolerance] => {
                    let tolerance: f64 = tolerance.parse()?;
                    ensure(
                        tolerance.is_finite() && tolerance >= 0.0,
                        "invalid integer tolerance",
                    )?;
                    // Lossless integers satisfy any nonnegative error bound.
                    Field::Integer
                }
                ["ENUM", cardinality, "0"] => {
                    let n: usize = cardinality.parse()?;
                    ensure(n > 0 && n <= 65536, "invalid ENUM cardinality")?;
                    Field::Enum
                }
                _ => {
                    return Err(
                        "supported schema: INTEGER nonnegative_tolerance / ENUM cardinality 0"
                            .into(),
                    )
                }
            });
        }
        ensure(
            !fields.is_empty() && fields.len() <= 1024,
            "expected 1..1024 fields",
        )?;
        Ok(Self(fields))
    }
}

#[derive(Debug)]
pub enum Column {
    Integer(Vec<i64>),
    Enum {
        ids: Vec<u16>,
        dictionary: Vec<Vec<u8>>,
        counts: Vec<u32>,
    },
}

impl Column {
    /// Construct dictionary-encoded input; `Table::from_columns` validates it
    /// and computes counts. Every dictionary entry must occur at least once.
    pub fn enumerated(ids: Vec<u16>, dictionary: Vec<Vec<u8>>) -> Self {
        Self::Enum {
            ids,
            dictionary,
            counts: Vec::new(),
        }
    }
}

#[derive(Debug)]
pub struct Table {
    pub(crate) columns: Vec<Column>,
    pub(crate) rows: usize,
    pub(crate) crlf: bool,
    pub(crate) final_newline: bool,
}

impl Table {
    /// Take ownership of already-typed columns, with no CSV round-trip or copy
    /// of the cell arrays. Recompute enum counts once. CSV export is canonical LF.
    pub fn from_columns(mut columns: Vec<Column>) -> Result<Self> {
        ensure(
            !columns.is_empty() && columns.len() <= 1024,
            "invalid column count",
        )?;
        let rows = match &columns[0] {
            Column::Integer(v) => v.len(),
            Column::Enum { ids, .. } => ids.len(),
        };
        ensure(rows > 0 && rows <= u32::MAX as usize, "invalid row count")?;
        let mut dictionary_bytes = 0usize;
        for column in &mut columns {
            match column {
                Column::Integer(v) => ensure(v.len() == rows, "column length mismatch")?,
                Column::Enum {
                    ids,
                    dictionary,
                    counts,
                } => {
                    ensure(
                        ids.len() == rows && !dictionary.is_empty() && dictionary.len() <= 65536,
                        "invalid enum column",
                    )?;
                    let mut unique = std::collections::HashSet::new();
                    for token in dictionary.iter() {
                        dictionary_bytes = dictionary_bytes
                            .checked_add(token.len())
                            .ok_or("dictionary overflow")?;
                        ensure(
                            token.len() <= 1 << 20 && dictionary_bytes <= 64 << 20,
                            "dictionary byte limit exceeded",
                        )?;
                        ensure(
                            !token
                                .iter()
                                .any(|b| matches!(b, b',' | b'\r' | b'\n' | b'"')),
                            "unsupported enum token",
                        )?;
                        ensure(unique.insert(token.as_slice()), "duplicate enum token")?;
                    }
                    counts.clear();
                    counts.resize(dictionary.len(), 0);
                    for &id in ids.iter() {
                        let count = counts.get_mut(id as usize).ok_or("invalid enum ID")?;
                        *count += 1;
                    }
                    ensure(counts.iter().all(|&c| c > 0), "unused dictionary entry")?;
                }
            }
        }
        Ok(Self {
            columns,
            rows,
            crlf: false,
            final_newline: true,
        })
    }

    pub fn rows(&self) -> usize {
        self.rows
    }
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Single pass: parse + dictionary encode + count. No per-cell allocation
    /// for existing values, and no full copy of the input text is retained.
    /// Unquoted CSV only; quoted/multiline fields and mixed line endings fail.
    pub fn read_csv(mut reader: impl BufRead, schema: &Schema) -> Result<Self> {
        ensure(
            !schema.0.is_empty() && schema.0.len() <= 1024,
            "invalid schema",
        )?;
        let mut columns: Vec<_> = schema
            .0
            .iter()
            .map(|field| match field {
                Field::Integer => Column::Integer(Vec::new()),
                Field::Enum => Column::Enum {
                    ids: Vec::new(),
                    dictionary: Vec::new(),
                    counts: Vec::new(),
                },
            })
            .collect();
        let mut maps: Vec<HashMap<Vec<u8>, u16>> =
            (0..columns.len()).map(|_| HashMap::new()).collect();
        // Fast exact cache for the common one-/two-digit enum tokens. Tokens
        // such as 01 are deliberately not canonicalized or conflated with 1.
        let mut numeric_cache = vec![[u32::MAX; 100]; columns.len()];
        let mut line = Vec::new();
        let mut rows = 0usize;
        let mut crlf = None;
        let mut final_newline = false;
        loop {
            line.clear();
            if std::io::Read::take(&mut reader, (64 << 20) + 1).read_until(b'\n', &mut line)? == 0 {
                break;
            }
            ensure(line.len() <= 64 << 20, "record byte limit exceeded")?;
            final_newline = line.last() == Some(&b'\n');
            if final_newline {
                line.pop();
                let this_crlf = line.last() == Some(&b'\r');
                if this_crlf {
                    line.pop();
                }
                ensure(
                    crlf.is_none_or(|v| v == this_crlf),
                    "mixed line endings unsupported",
                )?;
                crlf = Some(this_crlf);
            }
            ensure(
                !line.contains(&b'"') && !line.contains(&b'\r'),
                "quoted CSV / embedded CR unsupported",
            )?;
            let mut fields = line.split(|&b| b == b',');
            for (index, column) in columns.iter_mut().enumerate() {
                let token = fields.next().ok_or("too few fields")?;
                match column {
                    Column::Integer(values) => values.push(parse_integer(token)?),
                    Column::Enum {
                        ids,
                        dictionary,
                        counts,
                    } => {
                        let key = numeric_key(token);
                        let cached = key.map_or(u32::MAX, |k| numeric_cache[index][k]);
                        let id = if cached != u32::MAX {
                            cached as u16
                        } else if let Some(&id) = maps[index].get(token) {
                            id
                        } else {
                            ensure(
                                dictionary.len() < 65536 && token.len() <= 1 << 20,
                                "enum limit exceeded",
                            )?;
                            let id = dictionary.len() as u16;
                            dictionary.push(token.to_vec());
                            maps[index].insert(token.to_vec(), id);
                            counts.push(0);
                            id
                        };
                        if let Some(k) = key {
                            numeric_cache[index][k] = u32::from(id);
                        }
                        counts[id as usize] =
                            counts[id as usize].checked_add(1).ok_or("too many rows")?;
                        ids.push(id);
                    }
                }
            }
            ensure(fields.next().is_none(), "too many fields")?;
            rows += 1;
            ensure(rows <= u32::MAX as usize, "too many rows")?;
        }
        ensure(rows > 0, "empty input unsupported")?;
        Ok(Self {
            columns,
            rows,
            crlf: crlf.unwrap_or(false),
            final_newline,
        })
    }
}

fn numeric_key(token: &[u8]) -> Option<usize> {
    match token {
        [a] if a.is_ascii_digit() => Some((a - b'0') as usize),
        [a, b] if (b'1'..=b'9').contains(a) && b.is_ascii_digit() => {
            Some(((a - b'0') * 10 + b - b'0') as usize)
        }
        _ => None,
    }
}

fn parse_integer(token: &[u8]) -> Result<i64> {
    let negative = token.first() == Some(&b'-');
    let digits = if negative { &token[1..] } else { token };
    ensure(!digits.is_empty(), "empty integer")?;
    ensure(
        digits.len() == 1 || digits[0] != b'0',
        "noncanonical integer (leading zeros)",
    )?;
    let mut value = 0u64;
    for &byte in digits {
        ensure(byte.is_ascii_digit(), "invalid integer")?;
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(u64::from(byte - b'0')))
            .ok_or("integer overflow")?;
    }
    if negative {
        ensure(
            value > 0 && value <= (1u64 << 63),
            "invalid negative integer",
        )?;
        Ok(value.wrapping_neg() as i64)
    } else {
        ensure(value <= i64::MAX as u64, "integer overflow")?;
        Ok(value as i64)
    }
}

pub(crate) fn append_integer(out: &mut Vec<u8>, value: i64) {
    let mut buffer = [0u8; 20];
    let mut cursor = buffer.len();
    let mut n = value.unsigned_abs();
    loop {
        cursor -= 1;
        buffer[cursor] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    if value < 0 {
        out.push(b'-');
    }
    out.extend_from_slice(&buffer[cursor..]);
}
