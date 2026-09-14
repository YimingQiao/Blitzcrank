//! Experimental tabular pipeline. Independent-column models, not the complete
//! Blitzcrank model learner or a reader for historical C++ files.
#![forbid(unsafe_code)]

mod archive;
mod fixed;
pub mod general;
mod joint;
pub mod record;
mod table;
pub use archive::{
    compress, compress_joint_records, compress_records, compress_with_precision, Archive, Block,
    Options,
};
pub use table::{Column, Field, Schema, Table};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) fn probability_model(counts: &[u32], bits: u32) -> Result<delayed_coding::Model> {
    ensure((8..=16).contains(&bits), "precision must be 8..16")?;
    // Keep large dictionaries at full precision; a small-table request only
    // applies to alphabets <=256. Explicit policy, not a timing-based oracle.
    let precision = if counts.iter().filter(|&&n| n != 0).count() <= 256 {
        bits
    } else {
        16
    };
    Ok(encoding_model(delayed_coding::Model::new(
        &delayed_coding::Model::normalize_precision(counts, precision)?,
    )?))
}

pub(crate) fn encoding_model(model: delayed_coding::Model) -> delayed_coding::Model {
    #[cfg(feature = "encode-tables")]
    let model = model.with_tables(delayed_coding::TableOptions {
        direct_encode: true,
        direct_decode: false,
    });
    model
}
pub(crate) fn decoding_model(model: delayed_coding::Model) -> delayed_coding::Model {
    #[cfg(feature = "decode-tables")]
    let model = model.with_tables(delayed_coding::TableOptions {
        direct_encode: false,
        direct_decode: true,
    });
    model
}

pub(crate) fn ensure(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(message.into())
    }
}
