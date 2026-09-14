use blitzcrank_rs::{
    general::{self, GeneralArchive, GeneralBlock},
    *,
};
use std::io::Cursor;

#[test]
fn balanced_records_are_the_existing_independent_format() {
    let t = Table::read_csv(
        Cursor::new("1,a,x\n2,b,y\n3,a,y\n"),
        &Schema::parse("INTEGER 0\nENUM 2 0\nENUM 2 0").unwrap(),
    )
    .unwrap();
    for lanes in [1, 4] {
        let old = compress(
            &t,
            Options {
                block_rows: 1,
                lanes,
            },
        )
        .unwrap();
        let new = compress_records(&t, lanes, 16).unwrap();
        assert_eq!(old, new);
        assert_eq!(Archive::open(&new).unwrap().joint_groups(), 0);
        let joint = compress_joint_records(&t, lanes, 16).unwrap();
        assert_eq!(Archive::open(&joint).unwrap().joint_groups(), 1);
    }
}

#[test]
fn failed_block_and_row_reads_invalidate_previous_results() {
    let t = Table::read_csv(
        Cursor::new("1,a\n2,b\n"),
        &Schema::parse("INTEGER 0\nENUM 2 0").unwrap(),
    )
    .unwrap();
    let bytes = compress(&t, Options::default()).unwrap();
    let a = Archive::open(&bytes).unwrap();
    let mut b = Block::default();
    for by_row in [false, true] {
        a.decode_block(0, &mut b).unwrap();
        assert_eq!(b.integer(0, 0), Some(1));
        if by_row {
            assert!(a.locate_row(usize::MAX, &mut b).is_err());
        } else {
            assert!(a.decode_block(usize::MAX, &mut b).is_err());
        }
        assert_eq!(b.rows(), 0);
        assert_eq!(b.integer(0, 0), None);
        assert_eq!(b.symbol(1, 0), None);
    }
    let t = general::GeneralTable::read(Cursor::new("x\ny\n"), vec![general::Kind::Text], b',')
        .unwrap();
    let bytes = general::compress(&t, 2, 1).unwrap();
    let a = GeneralArchive::open(&bytes).unwrap();
    let mut b = GeneralBlock::default();
    a.decode_block(0, &mut b).unwrap();
    assert!(a.locate_row(usize::MAX, &mut b).is_err());
    assert_eq!(a.token(&b, 0, 0), None);
    a.decode_block(0, &mut b).unwrap();
    assert!(a.decode_block(usize::MAX, &mut b).is_err());
    assert_eq!(a.token(&b, 0, 0), None);
}

#[test]
fn simd_bulk_full_precision_mixed_types_and_tail() {
    // Small and >256-symbol dictionaries, numeric lexemes and strings; 65,537
    // rows also promote the last column to bytes and exercise a one-row tail.
    let mut input = String::new();
    for i in 0..65537 {
        input.push_str(&format!(
            "{}|{}|{}.00|\"item,{i:08}\"\r\n",
            i % 7,
            i % 300,
            i
        ));
    }
    let kinds = general::schema("ENUM 7 0\nENUM 300 0\nDOUBLE 0\nSTRING").unwrap();
    let table = general::GeneralTable::read(Cursor::new(input.as_bytes()), kinds, b'|').unwrap();
    assert!(general::compress_simd(&table, 1).is_err());
    let bytes = general::compress_simd(&table, 4096).unwrap();
    assert_ne!(bytes[8] & 8, 0);
    let archive = GeneralArchive::open(&bytes).unwrap();
    assert_eq!(archive.column_codec(0), Some("dc64_cumulative_dictionary"));
    assert_eq!(archive.column_codec(1), Some("dc_dictionary"));
    assert_eq!(archive.column_codec(2), Some("exact_fixed_decimal"));
    assert_eq!(
        archive.column_codec(3),
        Some("prefix_dc64_cumulative_bytes")
    );
    assert!(archive.record_reader().is_err());
    let mut restored = Vec::new();
    archive.write_csv(&mut restored).unwrap();
    assert_eq!(restored, input.as_bytes());
    let mut block = GeneralBlock::default();
    for row in [65536, 17, 4096, 4095, 0] {
        let within = archive.locate_row(row, &mut block).unwrap();
        assert_eq!(
            archive.token(&block, 1, within).unwrap(),
            (row % 300).to_string().as_bytes()
        );
    }
    // Verify the aggregate LUT budget independently of non-LUT model memory.
    assert!(archive.simd_prepared_bytes() < 17 << 20);
}

#[test]
fn simd_profile_header_mutations_rejected() {
    fn crc(bytes: &[u8]) -> u32 {
        let mut c = !0u32;
        for &b in bytes {
            c ^= u32::from(b);
            for _ in 0..8 {
                c = (c >> 1) ^ (0xedb88320 & (0u32.wrapping_sub(c & 1)));
            }
        }
        !c
    }
    let table = general::GeneralTable::read(Cursor::new("a\nb\n"), vec![general::Kind::Text], b',')
        .unwrap();
    let bytes = general::compress_simd(&table, 4096).unwrap();
    for (position, value) in [(8, 15), (10, 4), (19, 1), (20, 0)] {
        let mut bad = bytes.clone();
        bad[position] = value;
        // block_rows starts at byte 19; force a record, not a bulk block.
        if position == 19 || position == 20 {
            bad[19..23].copy_from_slice(&1u32.to_le_bytes());
        }
        let end = bad.len() - 4;
        let c = crc(&bad[..end]);
        bad[end..].copy_from_slice(&c.to_le_bytes());
        assert!(GeneralArchive::open(&bad).is_err());
    }
}

#[test]
fn many_simd_columns_respect_lut_budget_and_keep_the_same_format() {
    let input = [
        vec!["a"; 40].join(","),
        vec!["b"; 40].join(","),
        vec!["b"; 40].join(","),
    ]
    .join("\n");
    let table = general::GeneralTable::read(
        Cursor::new(input.as_bytes()),
        vec![general::Kind::Enum; 40],
        b',',
    )
    .unwrap();
    let bytes = general::compress_simd(&table, 4096).unwrap();
    let archive = GeneralArchive::open(&bytes).unwrap();
    let mut output = Vec::new();
    archive.write_csv(&mut output).unwrap();
    assert_eq!(input.as_bytes(), output);
    if delayed_coding_simd::SimdModel::available() {
        assert_eq!(archive.column_backend(31), Some("avx512"));
        assert_eq!(archive.column_backend(32), Some("scalar"));
    } else {
        assert_eq!(archive.column_backend(0), Some("scalar"));
    }
    assert_eq!(archive.column_codec(39), Some("dc64_cumulative_dictionary"));
}
