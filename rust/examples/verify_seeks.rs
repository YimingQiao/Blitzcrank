//! Verify every field after shuffled/boundary seeks against independently parsed
//! source data. Intentionally outside the timed query loop.
use blitzcrank_rs::{Archive, Block, Column, Result, Schema, Table};
use std::{fs, io::BufReader};
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("usage: verify_seeks INPUT_CSV CONFIG ARCHIVE".into());
    }
    let schema = Schema::parse(&fs::read_to_string(&args[2])?)?;
    let table = Table::read_csv(BufReader::new(fs::File::open(&args[1])?), &schema)?;
    let bytes = fs::read(&args[3])?;
    let archive = Archive::open(&bytes)?;
    if table.rows() != archive.rows() || table.columns().len() != archive.columns() {
        return Err("dimension mismatch".into());
    }
    let mut ids = vec![0, table.rows() - 1];
    let mut rng = 456789u64;
    for _ in 0..2048 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        ids.push((rng % table.rows() as u64) as usize);
    }
    // Include both sides of early block boundaries as well as random queries.
    for boundary in (archive.block_rows()..table.rows())
        .step_by(archive.block_rows())
        .take(32)
    {
        ids.push(boundary - 1);
        ids.push(boundary);
    }
    let mut block = Block::default();
    for &row in &ids {
        let within = archive.locate_row(row, &mut block)?;
        for (column, values) in table.columns().iter().enumerate() {
            match values {
                Column::Integer(v) => assert_eq!(Some(v[row]), block.integer(column, within)),
                Column::Enum {
                    ids, dictionary, ..
                } => {
                    assert_eq!(
                        archive.enum_token(
                            column,
                            block.symbol(column, within).ok_or("missing symbol")?
                        ),
                        Some(dictionary[ids[row] as usize].as_slice())
                    );
                }
            }
        }
    }
    println!(
        "Verified {} shuffled/boundary seeks, {} fields per row",
        ids.len(),
        archive.columns()
    );
    Ok(())
}
