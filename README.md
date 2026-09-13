# <div align="center"> Blitzcrank: Fast Semantic Compression for In-memory Online Transaction Processing </div>
## <div align="center"> Introducing Delayed Coding: A Novel Entropy Coding Algorithm  </div>


This repository contains the code for the paper titled "[Blitzcrank: Fast Semantic Compression for In-memory Online Transaction Processing](https://dl.acm.org/doi/10.14778/3675034.3675044)," published in **VLDB'24**.

**Blitzcrank** is a library for compressing row-store OLTP databases. It uses a novel entropy coding algorithm called **Delayed Coding**, which achieves near-entropy compression factors while maintaining fast decompression speeds.

## Clone Instructions

### Experimental standalone Rust encoder

The `improve-delayed-coding` branch can call the separate
[`YimingQiao/delayed-coding`](https://github.com/YimingQiao/delayed-coding) library
once per encoded block. This is **encoder-only**, opt-in, single-state delay-24
integration: the C++ decoder and historical files are retained. Four-state DC is
available in the core, but needs explicit new container metadata before use here.
The default build still uses the original research encoder.

Install Rust 1.88+, GCC/Clang and CMake 3.20+. Obtain the dependency revision named
by `BLITZCRANK_DELAYED_CODING_REVISION` in the top-level CMake file; it may be a
local development checkpoint until pushed. Nothing is downloaded by CMake.

```sh
cmake -S . -B build-rust -DCMAKE_BUILD_TYPE=Release \
  -DBLITZCRANK_RUST_ENCODER=ON \
  -DBLITZCRANK_DELAYED_CODING_DIR=/absolute/path/to/delayed-coding \
  -DDELAYED_CODING_BUILD_TESTS=OFF
cmake --build build-rust -j
```

The adapter imports immutable branch mappings after model construction (simple
pool entries on first use), gathers handles per block and reuses a compressor-owned
Rust workspace. It currently copies words into the existing `BitString` and adds
mapping/handle memory. Do not edit branch mappings after import without clearing
their cached Rust handles; lazy preparation must finish before concurrent sharing.
This is not yet a zero-copy, fully migrated or production-accepted backend.

For the real Census regression, first build the **unmodified** legacy revision
`0ed9c97908c51440b30a2eef3c1b90325dd2c87c` separately. GCC 12 may need
`-DCMAKE_CXX_FLAGS=-include\ cstdint` for that original source. Supply real data,
not LFS pointer files. The test uses the first 20,000 rows and writes only inside
the explicitly supplied, dedicated scratch directory:

```sh
cmake -DDATASET=/absolute/path/to/USCensus1990.dat \
  -DCONFIG=/absolute/path/to/USCensus1990.config \
  -DLEGACY_EXE=/absolute/path/to/legacy-build/tabular_blitzcrank \
  -DRUST_EXE=/absolute/path/to/build-rust/tabular_blitzcrank \
  -DVERIFY_EXE=/absolute/path/to/build-rust/verify_record_seeks \
  -DWORK_DIR=/absolute/path/to/dedicated-regression-scratch \
  -P tests/rust_encoder_roundtrip.cmake
```

At block thresholds 1 and 20,000 this verifies exact reconstruction, identical
payload/model/index bytes and 8,200 shuffled/boundary record seeks across both
backends. It does not establish JSON/time-series compatibility or rANS superiority.
`tests/benchmark_bridge.cmake` accepts `LEGACY_EXE`, `RUST_EXE`, the same `WORK_DIR`,
and `REPORT` CSV path; it runs five alternating samples with CPU 2 affinity.
This measures the bridge against original C++ DC, not against rANS. The initial
Census runs show modest encoding gains but a small retained-C++ decoding regression;
see the standalone library's `benchmarks/BLITZCRANK_FOUR_STATE.md` for all results.

### Research datasets

To clone this project successfully, ensure you have [git-lfs](https://git-lfs.com/) installed. We use it to manage large dataset files. Depending on your internet connection, the clone process may take several minutes to finish.

## Project Structure

The main project structure is as follows. Two versions of Blitzcrank are provided: `Delayed Coding` and `Arithmetic Coding`. You can switch between them by modifying the top-level `CMakeLists.txt`.


```
.
├── CMakeLists.txt
├── README.md
├── LICENSE
├── rapidjson    // it is a third-party library used to parse JSON.
├── delayed_coding
│         ├── CMakeLists.txt
│         ├── include
│         │         ├── base.h
│         │         ├── blitzcrank_exception.h
│         │         ├── categorical_model.h
│         │         ├── categorical_tree_model.h
│         │         ├── compression.h
│         │         ├── data_io.h
│         │         ├── decompression.h
│         │         ├── index.h
│         │         ├── json_base.h
│         │         ├── json_compression.h
│         │         ├── json_decompression.h
│         │         ├── json_model.h
│         │         ├── json_model_learner.h
│         │         ├── model.h
│         │         ├── model_learner.h
│         │         ├── numerical_model.h
│         │         ├── simple_prob_interval_pool.h
│         │         ├── string_model.h
│         │         ├── string_squid.h
│         │         ├── string_tools.h
│         │         ├── timeseries_model.h
│         │         └── utility.h
│         └── src
│             ├── categorical_model.cpp
│             ├── categorical_tree_model.cpp
│             ├── compression.cpp
│             ├── data_io.cpp
│             ├── decompression.cpp
│             ├── json_base.cpp
│             ├── json_compression.cpp
│             ├── json_decompression.cpp
│             ├── json_model.cpp
│             ├── json_model_learner.cpp
│             ├── model.cpp
│             ├── model_learner.cpp
│             ├── numerical_model.cpp
│             ├── simple_prob_interval_pool.cpp
│             ├── string_model.cpp
│             ├── string_squid.cpp
│             ├── string_tools.cpp
│             ├── timeseries_model.cpp
│             └── utility.cpp
├── plain_ra.cpp
├── JSON.cpp
└── tabular.cpp

```


## Build Instructions


Cmake is an open-source, cross-platform family of tools designed to build, test, and package software. A cmake config is provided. It can generate Makefiles or other scripts to create `Blitzcrank` binary.

We build `Blitzcrank` with:

```shell
cd ./build-release
cmake -DCMAKE_BUILD_TYPE=Release ..
make
```


## Compression Instructions

```shell
./tabular_blitzcrank [mode] [dataset] [config] [if use "|" as delimiter] [if skip learning] [block size]
```

- `[mode]`: 
    - `-c` for compression
    - `-d` for decompression
    - `-b` for benchmarking

- `[dataset]`: path to the dataset

- `[config]`: path to the config file

- `[if use "|" as delimiter]`: 
    - 0 for comma
    - 1 for "|"

- `[if skip learning]`: 
    - 0 for learning
    - 1 for skipping learning

- `[block size]`: block size for compression

----

### Example: USCensus1990

Let's use the [USCensus1990](https://archive.ics.uci.edu/ml/datasets/US+Census+Data+(1990)) dataset as an example. You can find this dataset and its configuration file in the [build-release](https://github.com/YimingQiao/Blitzcrank/tree/main/build-release) folder. Alternatively, you can download the dataset directly from this [link](https://drive.google.com/file/d/1Lpo_LcmC0tqR-Gl7yyvPO7xIcwe5ZP9_/view?usp=drive_link). The configuration file is necessary for compressing or decompressing a dataset, as it contains metadata specific to the dataset. We've provided the config file for the USCensus1990 dataset.

#### Benchmark Mode:


    ./tabular_blitzcrank -b USCensus1990.dat USCensus1990.config 0 1 20000
    
    
    Delimiter: ,	Skip Learning: 1	Block Size: 20000	
    [Compression]	Start load data into memory...
    Data loaded.
    Model Size: 3.26465 KB. 
    Throughput:  108.524 MiB/s	Time:  3.15137 s
    [Decompression]	Block Size: 286 tuple
    Throughput:  187.538 MiB/s	Time:  1.82363 s
    [Compression Factor (Origin Size / CompressedSize)]: 11.5654
    Compressed Size: 31030821


#### Compress Mode:

    ./tabular_blitzcrank -c USCensus1990.dat USCensus1990.com USCensus1990.config 0 1 20000


    Delimiter: ,	Skip Learning: 1	Block Size: 20000	
    Start load data into memory...
    Data loaded.
    Iteration 1 Starts
    Model Size: 3.26465 KB. 
    Compressed Size: 31030821
    

#### Decompress Mode

    ./tabular_blitzcrank -d USCensus1990.com USCensus1990.rec USCensus1990.config 0 20000

After the execution, we check the compression correctness:

    diff USCensus1990.dat USCensus1990.rec
