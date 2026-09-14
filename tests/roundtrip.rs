use blitzcrank_rs::{compress, Archive, Block, Column, Options, Schema, Table};
use std::io::Cursor;

fn check(input: &[u8], schema: &str, rows_per_block: usize, lanes: usize) {
    let table = Table::read_csv(Cursor::new(input), &Schema::parse(schema).unwrap()).unwrap();
    let bytes = compress(
        &table,
        Options {
            block_rows: rows_per_block,
            lanes,
        },
    )
    .unwrap();
    let archive = Archive::open(&bytes).unwrap();
    let mut restored = Vec::new();
    archive.write_csv(&mut restored).unwrap();
    assert_eq!(restored, input);
    let mut block = Block::default();
    let prepared = (rows_per_block == 1).then(|| archive.record_reader().unwrap());
    let mut record = blitzcrank_rs::record::Record::default();
    // Reverse order crosses every block boundary and checks every typed field.
    for row in (0..table.rows()).rev() {
        let within = archive.locate_row(row, &mut block).unwrap();
        if let Some(reader) = &prepared {
            reader.read(row, &mut record).unwrap();
        }
        for (column, values) in table.columns().iter().enumerate() {
            match values {
                Column::Integer(values) => {
                    if prepared.is_some() {
                        assert_eq!(
                            record.values()[column],
                            blitzcrank_rs::record::Value::Integer(values[row])
                        );
                    }
                    assert_eq!(block.integer(column, within), Some(values[row]))
                }
                Column::Enum { ids, .. } => {
                    if prepared.is_some() {
                        assert_eq!(
                            record.values()[column],
                            blitzcrank_rs::record::Value::Category(u32::from(ids[row]))
                        );
                    }
                    assert_eq!(block.symbol(column, within), Some(u32::from(ids[row])))
                }
            }
        }
    }
    assert!(archive.locate_row(table.rows(), &mut block).is_err());
    assert!(archive.decode_block(archive.blocks(), &mut block).is_err());
    for len in 0..bytes.len() {
        assert!(Archive::open(&bytes[..len]).is_err());
    }
    for index in (0..bytes.len()).step_by((bytes.len() / 100).max(1)) {
        let mut corrupt = bytes.clone();
        corrupt[index] ^= 1;
        assert!(Archive::open(&corrupt).is_err());
    }
}

#[test]
fn endings_constants_empty_enums_and_integer_extremes() {
    for input in [
        "-9223372036854775808,,01\n9223372036854775807,x,1\n0,x,0\n-1,,99\n",
        "-9223372036854775808,,01\r\n9223372036854775807,x,1\r\n0,x,0\r\n-1,,99",
        "1,constant,constant",
        "0,,\n0,,\n0,,\n",
    ] {
        for size in [1, 2, 3, 16, 1024] {
            for lanes in [1, 4] {
                check(
                    input.as_bytes(),
                    "INTEGER 0\nENUM 5 0\nENUM 5 0",
                    size,
                    lanes,
                );
            }
        }
    }
}

#[test]
fn skewed_and_diverse_columns() {
    let mut csv = String::new();
    let mut rng = 42u64;
    for row in 0..517 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        csv.push_str(&format!(
            "{},{},{},{}\n",
            rng as i64,
            if row % 17 == 0 { "rare" } else { "common" },
            row % 100,
            row
        ));
    }
    for size in [1, 16, 257, 1024] {
        for lanes in [1, 4] {
            check(
                csv.as_bytes(),
                "INTEGER 0\nENUM 2 0\nENUM 100 0\nENUM 600 0",
                size,
                lanes,
            );
        }
    }
}

#[test]
fn rejects_unsupported_or_ambiguous_input() {
    for schema in ["", "STRING 0", "INTEGER -0.01", "ENUM 0 0", "ENUM 65537 0"] {
        assert!(Schema::parse(schema).is_err());
    }
    let schema = Schema::parse("INTEGER 0\nENUM 3 0").unwrap();
    for input in [
        "",
        "1",
        "1,x,y\n",
        "01,x\n",
        "-0,x\n",
        "+1,x\n",
        "9223372036854775808,x\n",
        "-9223372036854775809,x\n",
        "1,\"x\"\n",
        "1,x\r\n2,y\n",
        "1,x\r",
    ] {
        assert!(
            Table::read_csv(Cursor::new(input), &schema).is_err(),
            "{input:?}"
        );
    }
    let table = Table::read_csv(Cursor::new("1,x\n"), &schema).unwrap();
    for options in [
        Options {
            block_rows: 0,
            lanes: 1,
        },
        Options {
            block_rows: 65537,
            lanes: 1,
        },
        Options {
            block_rows: 1,
            lanes: 2,
        },
    ] {
        assert!(compress(&table, options).is_err());
    }
}

#[test]
fn integer_only_and_enum_only_records() {
    for size in [1, 2, 256] {
        for lanes in [1, 4] {
            check(
                b"-9223372036854775808\n0\n9223372036854775807",
                "INTEGER 0",
                size,
                lanes,
            );
            check(b"\nx\n\n01\n1\n", "ENUM 8 0", size, lanes);
        }
    }
}

#[test]
fn typed_input_without_csv() {
    let table = Table::from_columns(vec![
        Column::Integer(vec![10, 11]),
        Column::enumerated(vec![0, 1], vec![b"red".to_vec(), b"blue".to_vec()]),
    ])
    .unwrap();
    let bytes = compress(&table, Options::default()).unwrap();
    let archive = Archive::open(&bytes).unwrap();
    let mut block = Block::default();
    let row = archive.locate_row(1, &mut block).unwrap();
    assert_eq!(
        archive.enum_token(1, block.symbol(1, row).unwrap()),
        Some(b"blue".as_slice())
    );
    assert_eq!(archive.enum_token(0, 0), None);
    let mut csv = Vec::new();
    archive.write_csv(&mut csv).unwrap();
    assert_eq!(csv, b"10,red\n11,blue\n");
    assert!(Table::from_columns(vec![]).is_err());
    assert!(
        Table::from_columns(vec![Column::Integer(vec![1]), Column::Integer(vec![1, 2])]).is_err()
    );
    assert!(Table::from_columns(vec![Column::enumerated(vec![1], vec![b"x".to_vec()])]).is_err());
    assert!(Table::from_columns(vec![Column::enumerated(
        vec![0],
        vec![b"x".to_vec(), b"y".to_vec()]
    )])
    .is_err());
    assert!(Table::from_columns(vec![Column::enumerated(
        vec![0, 1],
        vec![b"x".to_vec(), b"x".to_vec()]
    )])
    .is_err());
}
