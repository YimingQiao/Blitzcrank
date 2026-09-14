//! Experimental tabular pipeline. Independent-column models, not the complete
//! Blitzcrank model learner or a reader for historical C++ files.
#![forbid(unsafe_code)]

mod archive;
mod fixed;
pub mod general;
pub mod record;
mod table;
pub use archive::{compress, Archive, Block, Options};
pub use table::{Column, Field, Schema, Table};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) fn ensure(ok: bool, message: &str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(message.into())
    }
}
