//! Typed resident query benchmark. Verification is outside the query timer.
use blitzcrank_rs::{
    general::{GeneralArchive, GeneralBlock, Kind},
    record::{Record, RecordReader, Value},
    Archive, Block, Result,
};
use std::{fs, hint::black_box, time::Instant};

fn logical(token: &[u8]) -> Vec<u8> {
    if token.first() != Some(&b'"') {
        return token.to_vec();
    }
    let mut out = Vec::new();
    let mut i = 1;
    while i + 1 < token.len() {
        out.push(token[i]);
        i += if token[i] == b'"' { 2 } else { 1 };
    }
    out
}

fn verify_general(
    archive: &GeneralArchive<'_>,
    reader: &RecordReader<'_>,
    ids: impl Iterator<Item = usize>,
) -> Result<usize> {
    let mut block = GeneralBlock::default();
    let mut out = Record::default();
    let mut count = 0;
    for row in ids {
        reader.read(row, &mut out)?;
        archive.locate_row(row, &mut block)?;
        for (c, value) in out.values().iter().enumerate() {
            let raw = archive.token(&block, c, 0).ok_or("missing token")?;
            let token = logical(raw);
            let valid = match *value {
                Value::Null => token.is_empty() || token == b"null",
                Value::Integer(n) => {
                    let text = std::str::from_utf8(&token)?;
                    if let Ok(expected) = text.parse::<i64>() {
                        n == expected
                    } else {
                        // INTEGER decimal spellings are integral. Compare a
                        // canonical integer prefix; do not round through f64.
                        let (whole, fraction) = text.split_once('.').ok_or("integer spelling")?;
                        fraction.bytes().all(|b| b == b'0') && whole.parse::<i64>()? == n
                    }
                }
                Value::Decimal(n) => {
                    n.to_bits() == std::str::from_utf8(&token)?.parse::<f64>()?.to_bits()
                }
                Value::Category(id) => archive.category_token(c, id) == Some(raw),
                Value::Text { .. } => out.text(c) == Some(token.as_slice()),
            };
            if !valid {
                return Err(format!(
                    "typed mismatch row={row} col={c} value={value:?} token={token:?}"
                )
                .into());
            }
            if matches!(archive.kinds()[c], Kind::Decimal)
                && !matches!(value, Value::Decimal(_) | Value::Null)
            {
                return Err("decimal not materialized".into());
            }
        }
        count += 1;
    }
    Ok(count)
}
fn verify_simple(
    archive: &Archive<'_>,
    reader: &RecordReader<'_>,
    ids: impl Iterator<Item = usize>,
) -> Result<usize> {
    let mut block = Block::default();
    let mut out = Record::default();
    let mut count = 0;
    for row in ids {
        reader.read(row, &mut out)?;
        archive.locate_row(row, &mut block)?;
        for (c, value) in out.values().iter().enumerate() {
            let valid = match *value {
                Value::Integer(n) => block.integer(c, 0) == Some(n),
                Value::Category(n) => block.symbol(c, 0) == Some(n),
                _ => false,
            };
            if !valid {
                return Err(format!("typed mismatch row={row} col={c}").into());
            }
        }
        count += 1;
    }
    Ok(count)
}
fn measure(
    reader: &RecordReader<'_>,
    ids: &[usize],
    verified: usize,
    verify_only: bool,
) -> Result<()> {
    let mut out = Record::default();
    reader.read(0, &mut out)?;
    for &row in ids.iter().take(1000) {
        reader.read(row, &mut out)?;
        black_box(&out);
    }
    let start = Instant::now();
    if !verify_only {
        for &row in ids {
            reader.read(black_box(row), &mut out)?;
            black_box(&out);
        }
    }
    let elapsed = start.elapsed();
    println!(
        "{}",
        serde_json::json!({
            "queries": if verify_only { 0 } else { ids.len() },
            "mean_ns_per_row": if verify_only { 0.0 } else { elapsed.as_nanos() as f64 / ids.len() as f64 },
        "verified_rows":verified, "prepared_bytes":reader.prepared_bytes(),
        "warmup_queries":ids.len().min(1000),
            "setup_included":false, "measurement":"resident locate + typed whole-row decode; strings copied, numeric dictionaries prepared; no decoded-row cache"
        })
    );
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if !(3..=4).contains(&args.len()) || args[3..].iter().any(|s| s != "verify-all") {
        return Err("trace_records ARCHIVE TRACE_U32_LE [verify-all]".into());
    }
    let verify_all = args[3..].iter().any(|s| s == "verify-all");
    let bytes = fs::read(&args[1])?;
    let trace = fs::read(&args[2])?;
    if trace.is_empty() || trace.len() % 4 != 0 || trace.len() > 40000000 {
        return Err("invalid trace".into());
    }
    let ids: Vec<_> = trace
        .as_chunks::<4>()
        .0
        .iter()
        .map(|&b| u32::from_le_bytes(b) as usize)
        .collect();
    if bytes.starts_with(b"BLTZRS02") {
        let archive = GeneralArchive::open(&bytes)?;
        let reader = archive.record_reader()?;
        if ids.iter().any(|&r| r >= reader.rows()) {
            return Err("invalid query id".into());
        }
        let verified = if verify_all {
            verify_general(&archive, &reader, 0..reader.rows())?
        } else {
            verify_general(&archive, &reader, ids.iter().copied().take(1000))?
        };
        measure(&reader, &ids, verified, verify_all)
    } else {
        let archive = Archive::open(&bytes)?;
        let reader = archive.record_reader()?;
        if ids.iter().any(|&r| r >= reader.rows()) {
            return Err("invalid query id".into());
        }
        let verified = if verify_all {
            verify_simple(&archive, &reader, 0..reader.rows())?
        } else {
            verify_simple(&archive, &reader, ids.iter().copied().take(1000))?
        };
        measure(&reader, &ids, verified, verify_all)
    }
}
