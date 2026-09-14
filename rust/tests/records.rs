use blitzcrank_rs::{
    general::{self, GeneralArchive, GeneralTable},
    record::{Record, Value},
};
use std::io::Cursor;

#[test]
fn typed_dictionary_records_and_failure_invalidation() {
    let input = b"01,-0.000000,\"a,b\",x\n19300.000000,1.2300,\"a\"\"b\",y\nnull,null,,x\n";
    for lanes in [1, 4] {
        let table = GeneralTable::read(
            Cursor::new(input),
            general::schema("INTEGER 0\nDOUBLE 0\nSTRING\nENUM 2 0").unwrap(),
            b',',
        )
        .unwrap();
        let bytes = general::compress(&table, 1, lanes).unwrap();
        let archive = GeneralArchive::open(&bytes).unwrap();
        let reader = archive.record_reader().unwrap();
        let mut out = Record::default();
        reader.read(0, &mut out).unwrap();
        assert_eq!(out.values()[0], Value::Integer(1));
        match out.values()[1] {
            Value::Decimal(v) => assert_eq!(v.to_bits(), (-0.0f64).to_bits()),
            _ => panic!(),
        }
        assert_eq!(out.text(2), Some(b"a,b".as_slice()));
        reader.read(1, &mut out).unwrap();
        assert_eq!(out.values()[0], Value::Integer(19300));
        assert_eq!(out.values()[1], Value::Decimal(1.23));
        assert_eq!(out.text(2), Some(b"a\"b".as_slice()));
        reader.read(2, &mut out).unwrap();
        assert_eq!(out.values()[0], Value::Null);
        assert_eq!(out.values()[1], Value::Null);
        assert!(reader.read(3, &mut out).is_err());
        assert!(out.values().is_empty());
        assert!(out.text(2).is_none());
        reader.read(0, &mut out).unwrap();
        let bulk = general::compress(&table, 2, lanes).unwrap();
        assert!(GeneralArchive::open(&bulk)
            .unwrap()
            .record_reader()
            .is_err());
    }
}

#[test]
fn high_cardinality_typed_fixed_and_byte_strings() {
    let mut input = String::new();
    for i in 0..66000 {
        input.push_str(&format!("{}.{:06},\"text-{i:08}\"\n", i / 1000, i % 1000));
    }
    let table = GeneralTable::read(
        Cursor::new(input.as_bytes()),
        general::schema("DOUBLE 0\nSTRING").unwrap(),
        b',',
    )
    .unwrap();
    for lanes in [1, 4] {
        let bytes = general::compress(&table, 1, lanes).unwrap();
        let archive = GeneralArchive::open(&bytes).unwrap();
        let reader = archive.record_reader().unwrap();
        let mut out = Record::default();
        for i in (0..66000).step_by(17) {
            reader.read(i, &mut out).unwrap();
            let expected: f64 = format!("{}.{:06}", i / 1000, i % 1000).parse().unwrap();
            assert_eq!(out.values()[0], Value::Decimal(expected));
            assert_eq!(out.text(1).unwrap(), format!("text-{i:08}").as_bytes());
        }
    }
}

#[test]
fn independent_chunk_strings_roundtrip_and_typed_output() {
    let mut input = String::new();
    for i in 0..66000 {
        input.push_str(&format!("\"text-{i:08},\\\"\"\",{}\n", i % 3));
    }
    let table = GeneralTable::read(
        Cursor::new(input.as_bytes()),
        general::schema("STRING\nENUM 3 0").unwrap(),
        b',',
    )
    .unwrap();
    for lanes in [1, 4] {
        let bytes = general::compress_records(&table, lanes, 10, true).unwrap();
        let archive = GeneralArchive::open(&bytes).unwrap();
        assert_eq!(archive.column_codec(0), Some("dc_string_chunks4"));
        let mut restored = Vec::new();
        archive.write_csv(&mut restored).unwrap();
        assert_eq!(restored, input.as_bytes());
        let reader = archive.record_reader().unwrap().with_direct_tables(10);
        let mut out = Record::default();
        for i in (0..66000).step_by(29) {
            reader.read(i, &mut out).unwrap();
            assert_eq!(out.text(0).unwrap(), format!("text-{i:08},\\\"").as_bytes());
        }
    }
}

#[test]
fn joint_categorical_records_preserve_every_field() {
    use blitzcrank_rs::{compress_joint_records, Archive, Schema, Table};
    for input in [
        "1,x,a,b,c,z\n-2,y,a,d,c,w\n3,x,e,b,f,z\n",
        "0,x,a,b,c,z\n0,x,a,b,c,z\n",
    ] {
        let table = Table::read_csv(
            Cursor::new(input),
            &Schema::parse("INTEGER 0\nENUM 2 0\nENUM 2 0\nENUM 2 0\nENUM 2 0\nENUM 2 0").unwrap(),
        )
        .unwrap();
        for lanes in [1, 4] {
            for bits in [8, 10, 16] {
                let bytes = compress_joint_records(&table, lanes, bits).unwrap();
                let archive = Archive::open(&bytes).unwrap();
                assert_eq!(archive.joint_groups(), 2);
                let mut csv = Vec::new();
                archive.write_csv(&mut csv).unwrap();
                assert_eq!(csv, input.as_bytes());
                let reader = archive.record_reader().unwrap().with_direct_tables(bits);
                let mut out = Record::default();
                let mut block = blitzcrank_rs::Block::default();
                for row in (0..table.rows()).rev() {
                    reader.read(row, &mut out).unwrap();
                    archive.locate_row(row, &mut block).unwrap();
                    for c in 0..6 {
                        if c == 0 {
                            assert_eq!(
                                out.values()[c],
                                Value::Integer(block.integer(c, 0).unwrap())
                            );
                        } else {
                            assert_eq!(
                                out.values()[c],
                                Value::Category(block.symbol(c, 0).unwrap())
                            );
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn mixed_joint_record_event_order_and_corruption() {
    let source = b"x,a,1.25,red,b,c\ny,b,-0.0,blue,b,d\nx,a,null,red,e,c\n";
    let table = GeneralTable::read(
        Cursor::new(source),
        general::schema("ENUM 2 0\nENUM 2 0\nDOUBLE 0\nSTRING\nENUM 2 0\nENUM 2 0").unwrap(),
        b',',
    )
    .unwrap();
    for lanes in [1, 4] {
        let bytes = general::compress_records(&table, lanes, 16, true).unwrap();
        let archive = GeneralArchive::open(&bytes).unwrap();
        let mut csv = Vec::new();
        archive.write_csv(&mut csv).unwrap();
        assert_eq!(csv, source);
        for fused in [false, true] {
            let reader = archive.record_reader().unwrap();
            let reader = if fused {
                reader.with_fused_numeric().unwrap()
            } else {
                reader
            };
            let mut out = Record::default();
            reader.read(1, &mut out).unwrap();
            assert_eq!(out.text(3).unwrap(), b"blue");
            match out.values()[2] {
                Value::Decimal(n) => assert_eq!(n.to_bits(), (-0.0f64).to_bits()),
                _ => panic!(),
            }
        }
        let mut rng = 42u64;
        for _ in 0..1000 {
            let mut bad = bytes.clone();
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let end = bad.len() - 4;
            bad[rng as usize % end] ^= (rng >> 32) as u8 | 1;
            let mut crc = !0u32;
            for &b in &bad[..end] {
                crc ^= u32::from(b);
                for _ in 0..8 {
                    crc = (crc >> 1) ^ (0xedb88320 & 0u32.wrapping_sub(crc & 1));
                }
            }
            bad[end..].copy_from_slice(&(!crc).to_le_bytes());
            if let Ok(archive) = GeneralArchive::open(&bad) {
                if let Ok(reader) = archive.record_reader() {
                    let mut out = Record::default();
                    for row in 0..reader.rows().min(3) {
                        let _ = reader.read(row, &mut out);
                    }
                }
            }
        }
    }
}
