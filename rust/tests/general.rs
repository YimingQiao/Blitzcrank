use blitzcrank_rs::general::{self, GeneralArchive, GeneralBlock, GeneralTable};
use std::io::Cursor;

fn check(input: &[u8], config: &str, delimiter: u8, block_rows: usize, lanes: usize) {
    let table = GeneralTable::read(
        Cursor::new(input),
        general::schema(config).unwrap(),
        delimiter,
    )
    .unwrap();
    let data = general::compress(&table, block_rows, lanes).unwrap();
    let archive = GeneralArchive::open(&data).unwrap();
    let mut output = Vec::new();
    archive.write_csv(&mut output).unwrap();
    assert_eq!(output, input);
    let mut block = GeneralBlock::default();
    for row in (0..table.rows().min(1000)).rev() {
        let within = archive.locate_row(row, &mut block).unwrap();
        for c in 0..archive.columns() {
            assert!(archive.token(&block, c, within).is_some());
        }
    }
    assert!(archive.locate_row(table.rows(), &mut block).is_err());
    for pos in (0..data.len()).step_by((data.len() / 100).max(1)) {
        let mut bad = data.clone();
        bad[pos] ^= 1;
        assert!(GeneralArchive::open(&bad).is_err());
        assert!(GeneralArchive::open(&data[..pos]).is_err());
    }
}

#[test]
fn lexical_numbers_quotes_multiline_delimiters() {
    for block in [1, 2, 16, 256] {
        for lanes in [1, 4] {
            check(b"01,-0.000000,\"a,b\",x\r\nnull,1.2300,\"a\"\"b\",\"x\"\r\n\"-2\",null,\"two\r\nlines\",\r\n", "INTEGER 0.5\nDOUBLE 0.01\nSTRING\nENUM 2 0",b',',block,lanes);
            check(
                b"1|4.200000|a,b\n2|4.200000|\"x|y\"",
                "INTEGER 0\nDOUBLE 0\nSTRING",
                b'|',
                block,
                lanes,
            );
            check(b"\n\n\n", "STRING", b',', block, lanes);
            check(
                b"19300.000000\n01\nnull\n",
                "INTEGER 0.5",
                b',',
                block,
                lanes,
            );
        }
    }
}

#[test]
fn high_cardinality_promotes_to_bytes() {
    let mut input = String::new();
    for row in 0..66000 {
        input.push_str(&format!("text-{row:08},{}\n", row % 3));
    }
    for block in [1, 256] {
        for lanes in [1, 4] {
            check(input.as_bytes(), "STRING\nENUM 3 0", b',', block, lanes);
        }
    }
}

#[test]
fn high_cardinality_decimals_pack_losslessly() {
    let mut input = String::new();
    for row in 0..66000 {
        input.push_str(&format!(
            "{}.{:06},{},text-{:08}\r\n",
            row / 1000,
            row % 1000,
            row - 33000,
            row
        ));
    }
    for block in [1, 256] {
        for lanes in [1, 4] {
            check(
                input.as_bytes(),
                "DOUBLE 0.001\nINTEGER 0\nSTRING",
                b',',
                block,
                lanes,
            );
        }
    }
    // A single incompatible spelling must retain the lexical representation.
    input.push_str("-0.000000,null,final\r\n");
    check(
        input.as_bytes(),
        "DOUBLE 0\nINTEGER 0\nSTRING",
        b',',
        256,
        1,
    );
}

#[test]
fn fixed_only_records_have_no_entropy_symbols() {
    let mut input = String::new();
    for value in 0..66000 {
        input.push_str(&format!("{value}.000000\n"));
    }
    for block in [1, 256] {
        for lanes in [1, 4] {
            check(input.as_bytes(), "INTEGER 0.5", b',', block, lanes);
        }
    }
}

#[test]
fn checksum_repaired_mutations_do_not_panic() {
    fn crc(bytes: &[u8]) -> u32 {
        let mut c = u32::MAX;
        for &b in bytes {
            c ^= u32::from(b);
            for _ in 0..8 {
                c = (c >> 1) ^ (0xedb88320 & 0u32.wrapping_sub(c & 1));
            }
        }
        !c
    }
    let table = GeneralTable::read(
        Cursor::new(b"1.25,a\n2.50,b\n"),
        general::schema("DOUBLE 0\nSTRING").unwrap(),
        b',',
    )
    .unwrap();
    for block_rows in [1, 256] {
        let original = general::compress(&table, block_rows, 1).unwrap();
        let end = original.len() - 4;
        for position in 0..end {
            for value in [0, 1, 127, 255] {
                let mut bad = original.clone();
                bad[position] = value;
                let sum = crc(&bad[..end]);
                bad[end..].copy_from_slice(&sum.to_le_bytes());
                if let Ok(archive) = GeneralArchive::open(&bad) {
                    let mut block = GeneralBlock::default();
                    for index in 0..archive.blocks() {
                        let _ = archive.decode_block(index, &mut block);
                    }
                }
            }
        }
    }
}

#[test]
fn invalid_records_rejected() {
    for text in [
        "a\"b\n",
        "\"unclosed\n",
        "\"x\"junk\n",
        "x\ry\n",
        "x\r\ny\n",
    ] {
        assert!(
            GeneralTable::read(Cursor::new(text), general::schema("STRING").unwrap(), b',')
                .is_err(),
            "{text:?}"
        );
    }
    for text in ["NaN\n", "Infinity\n", "abc\n"] {
        assert!(GeneralTable::read(
            Cursor::new(text),
            general::schema("DOUBLE 0").unwrap(),
            b','
        )
        .is_err());
    }
    assert!(GeneralTable::read(
        Cursor::new("1.5\n"),
        general::schema("INTEGER 0.5").unwrap(),
        b','
    )
    .is_err());
    assert!(GeneralTable::read(Cursor::new(""), general::schema("STRING").unwrap(), b',').is_err());
}
