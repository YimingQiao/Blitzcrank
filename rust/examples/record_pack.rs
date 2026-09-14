//! Explicit experiment builder; does not silently change the default encoder.
use blitzcrank_rs::{
    general::{self, GeneralTable},
    Options, Result, Schema, Table,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, Write},
};
fn main() -> Result<()> {
    let a: Vec<_> = std::env::args().collect();
    if !(7..=8).contains(&a.len()) || a.get(7).is_some_and(|v| v != "chunks") {
        return Err("record_pack INPUT SCHEMA OUTPUT LANES PRECISION comma|pipe".into());
    }
    if fs::symlink_metadata(&a[3]).is_ok() {
        return Err("output exists".into());
    }
    let lanes = a[4].parse()?;
    let bits = a[5].parse()?;
    let delimiter = match a[6].as_str() {
        "comma" => b',',
        "pipe" => b'|',
        _ => return Err("invalid delimiter".into()),
    };
    let config = fs::read_to_string(&a[2])?;
    let simple = if delimiter == b',' {
        Schema::parse(&config)
            .ok()
            .and_then(|s| Table::read_csv(BufReader::new(File::open(&a[1]).ok()?), &s).ok())
    } else {
        None
    };
    let bytes = if let Some(table) = simple {
        if a.len() == 8 {
            blitzcrank_rs::compress_joint_records(&table, lanes, bits)?
        } else {
            blitzcrank_rs::compress_with_precision(
                &table,
                Options {
                    block_rows: 1,
                    lanes,
                },
                bits,
            )?
        }
    } else {
        let table = GeneralTable::read(
            BufReader::new(File::open(&a[1])?),
            general::schema(&config)?,
            delimiter,
        )?;
        general::compress_records(&table, lanes, bits, a.len() == 8)?
    };
    // Example only: create-only but not atomic/durable. Production CLI retains
    // its staging protocol; a failed example write may leave a partial file.
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&a[3])?
        .write_all(&bytes)?;
    println!(
        "{}",
        serde_json::json!({"bytes": bytes.len(), "states":lanes, "small_alphabet_precision":bits})
    );
    Ok(())
}
