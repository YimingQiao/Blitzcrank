//! Stream an archive as exact CSV for external SHA-256 verification.
use blitzcrank_rs::{general::GeneralArchive, Archive, Result};
use std::{
    fs,
    io::{self, BufWriter},
};
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 2 {
        return Err("archive_csv ARCHIVE".into());
    }
    let bytes = fs::read(&args[1])?;
    let output = BufWriter::with_capacity(1 << 20, io::stdout().lock());
    if bytes.starts_with(b"BLTZRS02") {
        GeneralArchive::open(&bytes)?.write_csv(output)?;
    } else {
        Archive::open(&bytes)?.write_csv(output)?;
    }
    Ok(())
}
