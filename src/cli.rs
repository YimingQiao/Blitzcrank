use crate::view::View;
use blitzcrank_rs::{
    compress,
    general::{self, GeneralTable},
    Options, Schema, Table,
};
use serde_json::{json, Value};
use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

struct Failure {
    code: &'static str,
    message: String,
    exit: i32,
}
type ApiResult<T> = Result<T, Failure>;
fn failure(code: &'static str, message: impl ToString) -> Failure {
    Failure {
        code,
        message: message.to_string(),
        exit: match code {
            "E_USAGE" | "E_SCHEMA" => 2,
            "E_IO" => 3,
            "E_EXISTS" => 5,
            _ => 4,
        },
    }
}
trait Context<T> {
    fn context(self, code: &'static str) -> ApiResult<T>;
}
impl<T, E: std::fmt::Display> Context<T> for Result<T, E> {
    fn context(self, code: &'static str) -> ApiResult<T> {
        self.map_err(|e| failure(code, e))
    }
}
fn io_failure(error: std::io::Error) -> Failure {
    failure(
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            "E_EXISTS"
        } else {
            "E_IO"
        },
        error,
    )
}

fn codec_failure(code: &'static str, error: Box<dyn std::error::Error + Send + Sync>) -> Failure {
    failure(
        if error.downcast_ref::<std::io::Error>().is_some() {
            "E_IO"
        } else {
            code
        },
        error,
    )
}
fn archive_bytes(path: &str) -> ApiResult<Vec<u8>> {
    let metadata = fs::metadata(path).map_err(io_failure)?;
    if metadata.len() > u64::from(u32::MAX) + 4 {
        return Err(failure("E_FORMAT", "archive exceeds format size limit"));
    }
    fs::read(path).map_err(io_failure)
}
enum Input {
    Fast(Table),
    General(GeneralTable),
}
impl Input {
    fn rows(&self) -> usize {
        match self {
            Self::Fast(t) => t.rows(),
            Self::General(t) => t.rows(),
        }
    }
    fn encode(&self, options: Options) -> blitzcrank_rs::Result<Vec<u8>> {
        match self {
            Self::Fast(t) => compress(t, options),
            Self::General(t) => general::compress(t, options.block_rows, options.lanes),
        }
    }
}
fn input(path: &str, config: &str, delimiter: u8, force_general: bool) -> ApiResult<Input> {
    let kinds = general::schema(config).context("E_SCHEMA")?;
    if delimiter == b',' && !force_general {
        if let Ok(schema) = Schema::parse(config) {
            let file = File::open(path).map_err(io_failure)?;
            match Table::read_csv(BufReader::with_capacity(1 << 20, file), &schema) {
                Ok(t) => return Ok(Input::Fast(t)),
                Err(e) if e.downcast_ref::<std::io::Error>().is_some() => {
                    return Err(codec_failure("E_DATA", e))
                }
                Err(_) => (), // Retry lexical mode for quotes, nulls or noncanonical numeric spelling.
            }
        }
    }
    let file = File::open(path).map_err(io_failure)?;
    GeneralTable::read(BufReader::with_capacity(1 << 20, file), kinds, delimiter)
        .map(Input::General)
        .map_err(|e| codec_failure("E_DATA", e))
}

// Stage beside the target, then link atomically without replacing any name.
// No unsafe rename/overwrite fallback on filesystems without hard-link support.
struct AtomicOutput {
    file: File,
    temporary: PathBuf,
    destination: PathBuf,
}
impl AtomicOutput {
    fn new(destination: &str) -> ApiResult<Self> {
        let destination = PathBuf::from(destination);
        match fs::symlink_metadata(&destination) {
            Ok(_) => return Err(failure("E_EXISTS", "output already exists")),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
            Err(e) => return Err(io_failure(e)),
        }
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        for _ in 0..32 {
            let temporary = parent.join(format!(
                ".blitzcrank-{}-{}.tmp",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
            {
                Ok(file) => {
                    return Ok(Self {
                        file,
                        temporary,
                        destination,
                    })
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(io_failure(e)),
            }
        }
        Err(failure("E_IO", "could not allocate a staging file"))
    }
    fn commit(mut self) -> ApiResult<()> {
        self.file.flush().map_err(io_failure)?;
        fs::hard_link(&self.temporary, &self.destination).map_err(io_failure)?;
        Ok(())
    }
}
impl Drop for AtomicOutput {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.temporary);
    }
}

fn capabilities() -> Value {
    json!({"name":"blitzcrank-rs", "version":env!("CARGO_PKG_VERSION"),
        "formats_read":["BLTZRS01","BLTZRS02"], "format_write":"automatic_v1_or_v2", "threads":1,
        "simd":delayed_coding_simd::SimdModel::available(),
        "avx512":{"compiled":cfg!(feature="avx512"),"available":delayed_coding_simd::SimdModel::available(),"scope":"explicit compress-simd bulk profile only","scalar_fallback":true},
        "commands":{
            "compress":{"arguments":["input_csv","schema","output_archive"],"optional":["block_rows=256","states=1"]},
            "compress-records":{"arguments":["input_csv","schema","output_archive"],"optional":["states=1"],"block_rows":1},
            "compress-simd":{"arguments":["input_csv","schema","output_archive"],"optional":["block_rows=4096"],"entropy_states":64,"max_alphabet":256,"precision":16,"experimental":true},
            "decompress":{"arguments":["input_archive","output_csv"]},
            "inspect":{"arguments":["input_archive"]},
            "validate":{"arguments":["input_archive"]},
            "decode-row":{"arguments":["input_archive","zero_based_row"]},
            "decode-rows":{"arguments":["input_archive","zero_based_start","count"],"max_count":1024,"max_response_field_bytes":8388608},
            "seek-bench":{"arguments":["input_archive"],"optional":["queries=300000"]},
            "seek-record-bench":{"arguments":["input_archive"],"optional":["queries=300000"],"typed":true,"setup_included":false}},
        "schema_types":["INTEGER nonnegative_tolerance","ENUM cardinality 0","DOUBLE nonnegative_tolerance","STRING"],
        "lossless":true,"quoted_csv":true,"multiline_csv":true,"cpp_format_compatible":false,
        "delimiter_flags":["--delimiter=comma","--delimiter=pipe"],"backend_flags":["--general"],
        "states":[1,4],"max_columns":1024,"max_block_rows":65536,"max_dictionary_entries":65536,
        "errors":{"E_USAGE":2,"E_SCHEMA":2,"E_IO":3,"E_DATA":4,"E_FORMAT":4,"E_EXISTS":5},
        "output_policy":"atomic-create-only; existing paths never overwritten; no fsync durability guarantee",
        "json_flag":"--json; one response on stdout, exit status is authoritative",
        "integer_json":"decimal strings to preserve all i64 bits"})
}

fn run(args: &[String], delimiter: u8, force_general: bool) -> ApiResult<Value> {
    match args.first().map(String::as_str) {
        Some("capabilities") if args.len() == 1 => Ok(capabilities()),
        Some("compress-simd") if (4..=5).contains(&args.len()) => {
            let block_rows = args.get(4).map(|s| s.parse()).transpose().context("E_USAGE")?.unwrap_or(4096usize);
            if !(4096..=65536).contains(&block_rows) { return Err(failure("E_USAGE", "expected SIMD block_rows 4096..65536")); }
            let mut output = AtomicOutput::new(&args[3])?;
            let config = fs::read_to_string(&args[2]).map_err(io_failure)?;
            let table = input(&args[1], &config, delimiter, true)?;
            let Input::General(table) = table else { unreachable!() };
            let bytes = general::compress_simd(&table, block_rows).context("E_DATA")?;
            output.file.write_all(&bytes).map_err(io_failure)?;
            output.commit()?;
            Ok(json!({"rows":table.rows(),"bytes":bytes.len(),"block_rows":block_rows,"profile":"simd-bulk","avx512_available":delayed_coding_simd::SimdModel::available(),"independent_records":false}))
        }
        Some("compress-records") if (4..=5).contains(&args.len()) => {
            let lanes = args.get(4).map(|s| s.parse()).transpose().context("E_USAGE")?.unwrap_or(1);
            if !matches!(lanes, 1|4) { return Err(failure("E_USAGE", "expected states 1 or 4")); }
            let mut output = AtomicOutput::new(&args[3])?;
            let begin = Instant::now();
            let config = fs::read_to_string(&args[2]).map_err(io_failure)?;
            let table = input(&args[1], &config, delimiter, force_general)?;
            let parsed = Instant::now();
            let bytes = table.encode(Options { block_rows: 1, lanes }).context("E_DATA")?;
            let encoded = Instant::now();
            output.file.write_all(&bytes).map_err(io_failure)?;
            output.commit()?;
            Ok(json!({"rows":table.rows(),"bytes":bytes.len(),"block_rows":1,"states":lanes,"small_alphabet_precision":16,"profile":"balanced",
                "timings_s":{"parse_count":(parsed-begin).as_secs_f64(),"model_encode_crc":(encoded-parsed).as_secs_f64(),"write_commit":encoded.elapsed().as_secs_f64()}}))
        }
        Some("seek-record-bench") if (2..=3).contains(&args.len()) => {
            let queries = args.get(2).map(|s| s.parse()).transpose().context("E_USAGE")?.unwrap_or(300000usize);
            if queries == 0 || queries > 10000000 { return Err(failure("E_USAGE", "expected queries 1..10000000")); }
            let bytes = archive_bytes(&args[1])?;
            let archive = View::open(&bytes).context("E_FORMAT")?;
            if archive.block_rows() != 1 { return Err(failure("E_USAGE", "typed record reader requires one row per block")); }
            let reader = archive.record_reader().context("E_FORMAT")?;
            let mut out = blitzcrank_rs::record::Record::default();
            let mut rng = 123456u64;
            let ids: Vec<_> = (0..queries).map(|_| { rng^=rng<<13; rng^=rng>>7; rng^=rng<<17; (rng % reader.rows() as u64) as usize }).collect();
            reader.read(0, &mut out).context("E_FORMAT")?;
            let start = Instant::now();
            for row in ids { reader.read(std::hint::black_box(row), &mut out).context("E_FORMAT")?; std::hint::black_box(&out); }
            Ok(json!({"queries":queries,"mean_ns_per_row":start.elapsed().as_nanos() as f64/queries as f64,"prepared_bytes":reader.prepared_bytes(),"setup_included":false,"typed":true,"text_copied":true,"block_rows":1,"query_distribution":"uniform_xorshift64_seed_123456"}))
        }
        Some("compress") if (4..=6).contains(&args.len()) => {
            let options = Options {
                block_rows: args.get(4).map(|s| s.parse()).transpose().context("E_USAGE")?.unwrap_or(256),
                lanes: args.get(5).map(|s| s.parse()).transpose().context("E_USAGE")?.unwrap_or(1),
            };
            if options.block_rows == 0 || options.block_rows > 65536 || !matches!(options.lanes,1|4) { return Err(failure("E_USAGE","expected block_rows 1..65536 and states 1 or 4")); }
            let mut output = AtomicOutput::new(&args[3])?;
            let begin = Instant::now();
            let config = fs::read_to_string(&args[2]).map_err(io_failure)?;
            let table = input(&args[1],&config,delimiter,force_general)?;
            let parsed = Instant::now();
            let bytes = table.encode(options).context("E_DATA")?;
            let encoded = Instant::now();
            output.file.write_all(&bytes).map_err(io_failure)?;
            output.commit()?;
            Ok(json!({"rows":table.rows(),"bytes":bytes.len(),"block_rows":options.block_rows,"states":options.lanes,
                "timings_s":{"parse_count":(parsed-begin).as_secs_f64(),"model_encode_crc":(encoded-parsed).as_secs_f64(),"write_commit":encoded.elapsed().as_secs_f64()}}))
        }
        Some("decompress") if args.len() == 3 => {
            let mut output = AtomicOutput::new(&args[2])?;
            let begin = Instant::now();
            let bytes = archive_bytes(&args[1])?;
            let archive = View::open(&bytes).context("E_FORMAT")?;
            let opened = Instant::now();
            archive.write_csv(BufWriter::with_capacity(1 << 20, &mut output.file)).map_err(|e|codec_failure("E_FORMAT",e))?;
            output.commit()?;
            Ok(json!({"rows":archive.rows(),"timings_s":{"read_crc_model":(opened-begin).as_secs_f64(),"decode_csv_write":opened.elapsed().as_secs_f64()}}))
        }
        Some("inspect" | "validate" | "decode-row" | "decode-rows" | "seek-bench") => {
            let command = &args[0];
            let valid = match command.as_str() { "inspect" | "validate" => args.len() == 2, "decode-row" => args.len() == 3, "decode-rows" => args.len() == 4, _ => (2..=3).contains(&args.len()) };
            if !valid { return Err(failure("E_USAGE", "invalid arguments; use capabilities --json")); }
            let bytes = archive_bytes(&args[1])?;
            let archive = View::open(&bytes).context("E_FORMAT")?;
            let mut block = archive.scratch();
            match command.as_str() {
                "inspect" => Ok(json!({"format":archive.format(),"bytes":bytes.len(),"rows":archive.rows(),"columns":archive.columns(),
                    "block_rows":archive.block_rows(),"blocks":archive.blocks(),"states":archive.states(),"checksum":"verified","delimiter":archive.delimiter(),
                    "fields":archive.fields(), "codecs":archive.codecs()})),
                "validate" => {
                    for index in 0..archive.blocks() { archive.decode_block(index,&mut block).context("E_FORMAT")?; }
                    Ok(json!({"rows":archive.rows(),"blocks_checked":archive.blocks(),"checksum":"verified","entropy_states":"verified"}))
                }
                "decode-row" => {
                    let row: usize = args[2].parse().context("E_USAGE")?;
                    if row >= archive.rows() { return Err(failure("E_USAGE", "row out of range")); }
                    let within = archive.locate_row(row,&mut block).context("E_FORMAT")?;
                    let fields=archive.row_json(within,&block).context("E_FORMAT")?;
                    Ok(json!({"row":row,"fields":fields}))
                }
                "decode-rows" => {
                    let start: usize = args[2].parse().context("E_USAGE")?;
                    let count: usize = args[3].parse().context("E_USAGE")?;
                    if count == 0 || count > 1024 || start >= archive.rows() || count > archive.rows() - start {
                        return Err(failure("E_USAGE", "expected an in-range slice of 1..1024 rows"));
                    }
                    let mut rows = Vec::with_capacity(count);
                    let mut loaded = None;
                    let mut response_bytes = 0usize;
                    for row in start..start + count {
                        let index = row / archive.block_rows();
                        if loaded != Some(index) {
                            archive.decode_block(index,&mut block).context("E_FORMAT")?;
                            loaded = Some(index);
                        }
                        let fields = archive.row_json(row % archive.block_rows(),&block).context("E_FORMAT")?;
                        response_bytes += serde_json::to_vec(&fields).context("E_DATA")?.len();
                        if response_bytes > 8 << 20 { return Err(failure("E_DATA", "response field budget exceeded; request fewer rows")); }
                        rows.push(json!({"row":row,"fields":fields}));
                    }
                    Ok(json!({"start":start,"count":count,"rows":rows}))
                }
                _ => {
                    let queries: usize = args.get(2).map(|s|s.parse()).transpose().context("E_USAGE")?.unwrap_or(300000);
                    if queries == 0 || queries > 10_000_000 { return Err(failure("E_USAGE","expected 1..10000000 queries")); }
                    let mut rng = 123456u64;
                    let ids: Vec<_> = (0..queries).map(|_| { rng^=rng<<13; rng^=rng>>7; rng^=rng<<17; (rng % archive.rows() as u64) as usize }).collect();
                    archive.locate_row(0,&mut block).context("E_FORMAT")?;
                    let start = Instant::now();
                    for row in ids { let within=archive.locate_row(row,&mut block).context("E_FORMAT")?; std::hint::black_box((&block,within)); }
                    Ok(json!({"queries":queries,"block_rows":archive.block_rows(),"mean_ns_per_row":start.elapsed().as_nanos() as f64/queries as f64,
                        "setup_included":false,"query_distribution":"uniform_xorshift64_seed_123456"}))
                }
            }
        }
        _ => Err(failure("E_USAGE", "use capabilities --json to discover commands; compress INPUT CONFIG OUTPUT [BLOCK_ROWS] [STATES]; decompress INPUT OUTPUT")),
    }
}

pub fn main() {
    let raw: Vec<_> = std::env::args_os().skip(1).collect();
    let machine =
        raw.iter().any(|s| s == "--json") || raw.first().is_some_and(|s| s == "capabilities");
    let converted: Result<Vec<String>, _> = raw.into_iter().map(|s| s.into_string()).collect();
    let argument_error = converted.is_err();
    let mut args = converted.unwrap_or_default();
    let force_general = args.iter().any(|s| s == "--general");
    let delimiter_flags = args
        .iter()
        .filter(|s| s.starts_with("--delimiter="))
        .count();
    let unknown_delimiter = args.iter().any(|s| {
        s.starts_with("--delimiter=")
            && !matches!(s.as_str(), "--delimiter=comma" | "--delimiter=pipe")
    });
    let delimiter = if args.iter().any(|s| s == "--delimiter=pipe") {
        b'|'
    } else {
        b','
    };
    args.retain(|s| {
        !matches!(
            s.as_str(),
            "--json" | "--general" | "--delimiter=pipe" | "--delimiter=comma"
        )
    });
    let command = args.first().map(String::as_str).unwrap_or("");
    let outcome = if argument_error {
        Err(failure("E_USAGE", "arguments must be valid UTF-8"))
    } else if unknown_delimiter
        || delimiter_flags > 1
        || (!matches!(command, "compress" | "compress-records" | "compress-simd")
            && (force_general || delimiter_flags != 0))
    {
        Err(failure(
            "E_USAGE",
            "invalid or conflicting compression flags",
        ))
    } else {
        run(&args, delimiter, force_general)
    };
    let (response, exit) = match outcome {
        Ok(result) => (
            json!({"api_version":1,"ok":true,"command":command,"result":result}),
            0,
        ),
        Err(e) => (
            json!({"api_version":1,"ok":false,"command":command,"error":{"code":e.code,"message":e.message}}),
            e.exit,
        ),
    };
    if machine {
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        if serde_json::to_writer(&mut out, &response).is_err() || writeln!(out).is_err() {
            std::process::exit(3);
        }
    } else if exit == 0 {
        println!("{}", response["result"]);
    } else {
        eprintln!(
            "{}: {}",
            response["error"]["code"], response["error"]["message"]
        );
    }
    if exit != 0 {
        std::process::exit(exit);
    }
}
