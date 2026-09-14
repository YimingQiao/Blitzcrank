use std::{fs, process::Command};

#[test]
fn cli_roundtrip_and_refuses_overwrites() {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let work =
        std::env::temp_dir().join(format!("blitzcrank-rs-cli-{}-{stamp}", std::process::id()));
    fs::create_dir(&work).unwrap();
    let input = work.join("input.csv");
    let config = work.join("input.config");
    let archive = work.join("archive.bcr");
    let output = work.join("output.csv");
    let data = b"1,red\n2,blue\n3,red\n";
    fs::write(&input, data).unwrap();
    fs::write(&config, b"INTEGER 0\nENUM 2 0\n").unwrap();
    let exe = env!("CARGO_BIN_EXE_blitzcrank-rs");
    assert!(Command::new(exe)
        .arg("compress")
        .args([&input, &config, &archive])
        .output()
        .unwrap()
        .status
        .success());
    assert!(Command::new(exe)
        .arg("decompress")
        .args([&archive, &output])
        .output()
        .unwrap()
        .status
        .success());
    assert_eq!(fs::read(&output).unwrap(), data);
    assert!(!Command::new(exe)
        .arg("decompress")
        .args([&archive, &input])
        .output()
        .unwrap()
        .status
        .success());
    assert_eq!(fs::read(&input).unwrap(), data);
    assert!(!Command::new(exe)
        .arg("compress")
        .args([&input, &config, &input])
        .output()
        .unwrap()
        .status
        .success());
    assert_eq!(fs::read(&input).unwrap(), data);
    let original = fs::read(&archive).unwrap();
    assert!(!Command::new(exe)
        .arg("compress")
        .args([&input, &config, &archive])
        .output()
        .unwrap()
        .status
        .success());
    assert_eq!(fs::read(&archive).unwrap(), original);
    assert!(Command::new(exe)
        .arg("seek-bench")
        .arg(&archive)
        .arg("100")
        .output()
        .unwrap()
        .status
        .success());
    let records = work.join("records.bcr");
    let packed = Command::new(exe)
        .arg("compress-records")
        .args([&input, &config, &records])
        .args(["1", "16", "--json"])
        .output()
        .unwrap();
    assert!(
        packed.status.success(),
        "{}",
        String::from_utf8_lossy(&packed.stdout)
    );
    let result: serde_json::Value = serde_json::from_slice(&packed.stdout).unwrap();
    assert_eq!(result["result"]["profile"], "balanced");
    assert_eq!(fs::read(&records).unwrap()[9] & 4, 0);
    let joint = work.join("joint.bcr");
    let packed = Command::new(exe)
        .arg("compress-records")
        .args([&input, &config, &joint])
        .args(["1", "16", "joint", "--json"])
        .output()
        .unwrap();
    assert!(packed.status.success());
    assert_ne!(fs::read(&joint).unwrap()[9] & 4, 0);
    let simd = work.join("simd.bcr");
    let packed = Command::new(exe)
        .arg("compress-simd")
        .args([&input, &config, &simd])
        .args(["4096", "--json"])
        .output()
        .unwrap();
    assert!(
        packed.status.success(),
        "{}",
        String::from_utf8_lossy(&packed.stdout)
    );
    assert_ne!(fs::read(&simd).unwrap()[8] & 8, 0);
    assert!(Command::new(exe)
        .arg("validate")
        .arg(&simd)
        .arg("--json")
        .output()
        .unwrap()
        .status
        .success());
    let read = Command::new(exe)
        .arg("seek-record-bench")
        .arg(&records)
        .args(["100", "0", "--json"])
        .output()
        .unwrap();
    assert!(
        read.status.success(),
        "{}",
        String::from_utf8_lossy(&read.stdout)
    );
    let result: serde_json::Value = serde_json::from_slice(&read.stdout).unwrap();
    assert_eq!(result["result"]["typed"], true);
    assert_eq!(result["result"]["block_rows"], 1);
    assert!(!Command::new(exe)
        .arg("seek-record-bench")
        .arg(&archive)
        .output()
        .unwrap()
        .status
        .success());
    // This directory was created by this test with create_dir, not reused.
    fs::remove_dir_all(work).unwrap();
}

#[test]
fn machine_contract_and_atomic_failure() {
    use serde_json::Value;
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let work =
        std::env::temp_dir().join(format!("blitzcrank-agent-{}-{stamp}", std::process::id()));
    fs::create_dir(&work).unwrap();
    fs::write(
        work.join("input.csv"),
        "-9223372036854775808,雪\n9223372036854775807,蓝\n",
    )
    .unwrap();
    fs::write(work.join("schema"), "INTEGER 0.5\nENUM 2 0\n").unwrap();
    fs::write(work.join("unsupported"), "UNKNOWN 0\nENUM 2 0\n").unwrap();
    fs::write(work.join("corrupt"), b"not an archive").unwrap();
    let exe = env!("CARGO_BIN_EXE_blitzcrank-rs");
    let invoke = |args: &[&str], code: i32| -> Value {
        let out = Command::new(exe)
            .current_dir(&work)
            .args(args)
            .arg("--json")
            .output()
            .unwrap();
        assert_eq!(
            out.status.code(),
            Some(code),
            "{}",
            String::from_utf8_lossy(&out.stdout)
        );
        assert!(out.stderr.is_empty());
        assert_eq!(out.stdout.iter().filter(|&&b| b == b'\n').count(), 1);
        let json: Value = serde_json::from_slice(&out.stdout).unwrap();
        assert_eq!(json["api_version"], 1);
        assert_eq!(json["ok"], code == 0);
        json
    };
    assert_eq!(invoke(&["capabilities"], 0)["result"]["lossless"], true);
    assert_eq!(invoke(&["wrong-command"], 2)["error"]["code"], "E_USAGE");
    assert_eq!(
        invoke(
            &["compress", "input.csv", "schema", "--delimiter=banana"],
            2
        )["error"]["code"],
        "E_USAGE"
    );
    assert_eq!(invoke(&["inspect", "missing"], 3)["error"]["code"], "E_IO");
    assert_eq!(
        invoke(
            &["compress", "input.csv", "unsupported", "failed-output"],
            2
        )["error"]["code"],
        "E_SCHEMA"
    );
    assert!(!work.join("failed-output").exists());
    assert_eq!(
        invoke(&["decompress", "corrupt", "failed-output"], 4)["error"]["code"],
        "E_FORMAT"
    );
    assert!(!work.join("failed-output").exists());
    invoke(&["compress", "input.csv", "schema", "archive"], 0);
    assert_eq!(
        invoke(&["compress", "input.csv", "schema", "archive"], 5)["error"]["code"],
        "E_EXISTS"
    );
    assert_eq!(invoke(&["inspect", "archive"], 0)["result"]["rows"], 2);
    assert_eq!(
        invoke(&["validate", "archive"], 0)["result"]["entropy_states"],
        "verified"
    );
    let row = invoke(&["decode-row", "archive", "1"], 0);
    assert_eq!(row["result"]["fields"][0]["decimal"], "9223372036854775807");
    assert_eq!(row["result"]["fields"][1]["utf8"], "蓝");
    let batch = invoke(&["decode-rows", "archive", "0", "2"], 0);
    assert_eq!(batch["result"]["count"], 2);
    assert_eq!(
        batch["result"]["rows"][1]["fields"][0]["decimal"],
        "9223372036854775807"
    );
    invoke(&["decode-rows", "archive", "1", "2"], 2);
    invoke(&["decode-rows", "archive", "0", "0"], 2);
    invoke(&["compress", "input.csv", "schema", "records", "1", "1"], 0);
    let batch = invoke(&["decode-rows", "records", "0", "2"], 0);
    assert_eq!(batch["result"]["rows"][1]["fields"][1]["utf8"], "蓝");
    assert_eq!(
        invoke(&["decode-row", "archive", "2"], 2)["error"]["code"],
        "E_USAGE"
    );
    invoke(&["decompress", "archive", "restored"], 0);
    assert_eq!(
        fs::read(work.join("restored")).unwrap(),
        fs::read(work.join("input.csv")).unwrap()
    );
    fs::write(work.join("mixed.csv"), b"1.2300|\"x|y\"\r\nnull|plain\r\n").unwrap();
    fs::write(work.join("mixed.schema"), "DOUBLE 0.01\nSTRING").unwrap();
    invoke(
        &[
            "compress",
            "mixed.csv",
            "mixed.schema",
            "mixed.bcr",
            "--delimiter=pipe",
        ],
        0,
    );
    assert_eq!(
        invoke(&["inspect", "mixed.bcr"], 0)["result"]["format"],
        "BLTZRS02"
    );
    invoke(&["validate", "mixed.bcr"], 0);
    let row = invoke(&["decode-row", "mixed.bcr", "0"], 0);
    assert_eq!(row["result"]["fields"][0]["raw_utf8"], "1.2300");
    assert_eq!(row["result"]["fields"][1]["raw_utf8"], "\"x|y\"");
    let batch = invoke(&["decode-rows", "mixed.bcr", "0", "2"], 0);
    assert_eq!(batch["result"]["rows"][1]["fields"][0]["raw_utf8"], "null");
    invoke(&["decompress", "mixed.bcr", "mixed.restored"], 0);
    assert_eq!(
        fs::read(work.join("mixed.csv")).unwrap(),
        fs::read(work.join("mixed.restored")).unwrap()
    );
    let mut large = Vec::new();
    for _ in 0..9 {
        large.extend(std::iter::repeat_n(b'x', 1 << 20));
        large.push(b'\n');
    }
    fs::write(work.join("large.csv"), large).unwrap();
    fs::write(work.join("large.schema"), "STRING").unwrap();
    invoke(&["compress", "large.csv", "large.schema", "large.bcr"], 0);
    assert_eq!(
        invoke(&["decode-rows", "large.bcr", "0", "9"], 4)["error"]["code"],
        "E_DATA"
    );
    assert!(!fs::read_dir(&work).unwrap().any(|e| e
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with(".blitzcrank-")));
    fs::remove_dir_all(work).unwrap();
}
