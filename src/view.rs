use blitzcrank_rs::{
    general::{GeneralArchive, GeneralBlock},
    Archive, Block, Field, Result,
};
use serde_json::{json, Value};
use std::io::Write;
pub enum View<'a> {
    Fast(Archive<'a>),
    General(GeneralArchive<'a>),
}
pub enum Scratch {
    Fast(Block),
    General(GeneralBlock),
}
impl<'a> View<'a> {
    pub fn open(bytes: &'a [u8]) -> Result<Self> {
        if bytes.starts_with(b"BLTZRS02") {
            Ok(Self::General(GeneralArchive::open(bytes)?))
        } else {
            Ok(Self::Fast(Archive::open(bytes)?))
        }
    }
    pub fn rows(&self) -> usize {
        match self {
            Self::Fast(a) => a.rows(),
            Self::General(a) => a.rows(),
        }
    }
    pub fn record_reader(&self) -> Result<blitzcrank_rs::record::RecordReader<'_>> {
        match self {
            Self::Fast(a) => a.record_reader(),
            Self::General(a) => a.record_reader(),
        }
    }
    pub fn columns(&self) -> usize {
        match self {
            Self::Fast(a) => a.columns(),
            Self::General(a) => a.columns(),
        }
    }
    pub fn blocks(&self) -> usize {
        match self {
            Self::Fast(a) => a.blocks(),
            Self::General(a) => a.blocks(),
        }
    }
    pub fn block_rows(&self) -> usize {
        match self {
            Self::Fast(a) => a.block_rows(),
            Self::General(a) => a.block_rows(),
        }
    }
    pub fn states(&self) -> usize {
        match self {
            Self::Fast(a) => a.state_count(),
            Self::General(a) => a.states(),
        }
    }
    pub fn format(&self) -> &'static str {
        match self {
            Self::Fast(_) => "BLTZRS01",
            Self::General(_) => "BLTZRS02",
        }
    }
    pub fn delimiter(&self) -> String {
        char::from(match self {
            Self::Fast(_) => b',',
            Self::General(a) => a.delimiter(),
        })
        .to_string()
    }
    pub fn fields(&self) -> Vec<&'static str> {
        match self {
            Self::Fast(a) => (0..a.columns())
                .map(|c| match a.field_type(c).unwrap() {
                    Field::Integer => "integer",
                    Field::Enum => "enum",
                })
                .collect(),
            Self::General(a) => a.kinds().iter().map(|k| k.name()).collect(),
        }
    }
    pub fn codecs(&self) -> Vec<&'static str> {
        match self {
            Self::Fast(a) => (0..a.columns())
                .map(|c| match a.field_type(c).unwrap() {
                    Field::Integer if a.block_rows() == 1 => "integer_varint",
                    Field::Integer => "integer_delta_packed",
                    Field::Enum => "dc_dictionary",
                })
                .collect(),
            Self::General(a) => (0..a.columns())
                .map(|c| a.column_codec(c).unwrap())
                .collect(),
        }
    }
    pub fn scratch(&self) -> Scratch {
        match self {
            Self::Fast(_) => Scratch::Fast(Block::default()),
            Self::General(_) => Scratch::General(GeneralBlock::default()),
        }
    }
    pub fn decode_block(&self, index: usize, b: &mut Scratch) -> Result<()> {
        match (self, b) {
            (Self::Fast(a), Scratch::Fast(b)) => a.decode_block(index, b),
            (Self::General(a), Scratch::General(b)) => a.decode_block(index, b),
            _ => Err("scratch type mismatch".into()),
        }
    }
    pub fn locate_row(&self, row: usize, b: &mut Scratch) -> Result<usize> {
        match (self, b) {
            (Self::Fast(a), Scratch::Fast(b)) => a.locate_row(row, b),
            (Self::General(a), Scratch::General(b)) => a.locate_row(row, b),
            _ => Err("scratch type mismatch".into()),
        }
    }
    pub fn write_csv(&self, writer: impl Write) -> Result<()> {
        match self {
            Self::Fast(a) => a.write_csv(writer),
            Self::General(a) => a.write_csv(writer),
        }
    }
    pub fn row_json(&self, row: usize, b: &Scratch) -> Result<Vec<Value>> {
        match(self,b){
            (Self::Fast(a),Scratch::Fast(b))=>(0..a.columns()).map(|c|match a.field_type(c).unwrap(){
                Field::Integer=>Ok(json!({"type":"integer","decimal":b.integer(c,row).ok_or("missing integer")?.to_string()})),
                Field::Enum=>{let symbol=b.symbol(c,row).ok_or("missing symbol")?;let token=a.enum_token(c,symbol).ok_or("invalid symbol")?;let mut v=token_json(token,"utf8");v["type"]=json!("enum");v["symbol"]=json!(symbol);Ok(v)}
            }).collect(),
            (Self::General(a),Scratch::General(b))=>(0..a.columns()).map(|c|{let token=a.token(b,c,row).ok_or("missing token")?;let mut v=token_json(token,"raw_utf8");v["type"]=json!(a.kinds()[c].name());v["encoding"]=json!("csv_lexeme");Ok(v)}).collect(),
            _=>Err("scratch type mismatch".into())
        }
    }
}
fn token_json(token: &[u8], key: &str) -> Value {
    match std::str::from_utf8(token) {
        Ok(text) => json!({key:text}),
        Err(_) => json!({"bytes_hex":token.iter().map(|b|format!("{b:02x}")).collect::<String>()}),
    }
}
