//! Resident query benchmark: use the same trace file for the C++ helper.
use blitzcrank_rs::{
    general::{GeneralArchive, GeneralBlock},
    Archive, Block, Result,
};
use std::{fs, hint::black_box, time::Instant};

fn measure(ids: &[usize], rows: usize, mut query: impl FnMut(usize) -> Result<()>) -> Result<()> {
    if ids.is_empty() || ids.iter().any(|&r| r >= rows) {
        return Err("invalid query trace".into());
    }
    query(0)?;
    let start = Instant::now();
    for &row in ids {
        query(black_box(row))?;
    }
    println!(
        "{}",
        serde_json::json!({"queries":ids.len(), "mean_ns_per_row":start.elapsed().as_nanos() as f64 / ids.len() as f64,
        "setup_included":false,"measurement":"resident locate + decode, one row per block required"})
    );
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 3 {
        return Err("trace_queries ARCHIVE TRACE_U32_LE".into());
    }
    let bytes = fs::read(&args[1])?;
    let trace = fs::read(&args[2])?;
    if trace.len() % 4 != 0 || trace.len() > 40000000 {
        return Err("invalid trace length".into());
    }
    let ids: Vec<_> = trace
        .as_chunks::<4>()
        .0
        .iter()
        .map(|&b| u32::from_le_bytes(b) as usize)
        .collect();
    if bytes.starts_with(b"BLTZRS02") {
        let archive = GeneralArchive::open(&bytes)?;
        if archive.block_rows() != 1 {
            return Err("use independent record mode".into());
        }
        let mut block = GeneralBlock::default();
        measure(&ids, archive.rows(), |row| {
            archive.locate_row(row, &mut block)?;
            black_box(&block);
            Ok(())
        })
    } else {
        let archive = Archive::open(&bytes)?;
        if archive.block_rows() != 1 {
            return Err("use independent record mode".into());
        }
        let mut block = Block::default();
        measure(&ids, archive.rows(), |row| {
            archive.locate_row(row, &mut block)?;
            black_box(&block);
            Ok(())
        })
    }
}
