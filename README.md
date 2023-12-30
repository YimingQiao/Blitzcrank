# Blitzcrank - Fast Semantic Compression with Random Access for OLTP workload

__Blitzcrank__, is a fast semantic compression algorithm, supporting random access. It provides better
compression factors than Gzip and [Zstandard](https://github.com/facebook/zstd). It is backed by a new entropy encoding algorithm,
called `Delayed Coding`, which has faster decompression speed.

---

## Clone Instruction
To clone this project successfully, please install [git-lfs](https://git-lfs.com/). We use it to manage large dataset files. The clone will take several minutes to finish, depending on your Internet. 


## Project Structure

Main project structure is shown below. Two version of blitzcrank is provided: `Delayed Coding` and `Arithmetic Coding`, you
can switch between them by changing the top-level `CMakeLists.txt`.


```
.
├── CMakeLists.txt
├── README.md
├── delay_blitzcrank
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

---

## Build instructions

`CMake` is an open-source, cross-platform family of tools designed to build, test and package software.
A `cmake` project generator is provided. It can generate Makefiles or other scripts to create `Blitzcrank` binary.

You can build `Blitzcrank` with:

```shell
cd ./build-release
cmake -DCMAKE_BUILD_TYPE=Release ..
make
```

---

## Compression How To:

### For tabular data set

`./tabular_blitzcrank [mode] [dataset] [config] [if use "|" as delimiter] [if skip learning] [block size]`

    [mode]: -c for compression, -d for decompression, -b for benchmarking
    
    [dataset]: path to the dataset
    
    [config]: path to the config file
    
    [if use "|" as delimiter]: 0 for comma, 1 for "|"
    
    [if skip learning]: 0 for learning, 1 for skipping learning
    
    [block size]: block size for compression

### Example: USCensus1990

Let's take [USCensus1990](https://archive.ics.uci.edu/ml/datasets/US+Census+Data+(1990)) as an example. This dataset and its config are in the [build-release](https://github.com/YimingQiao/Blitzcrank/tree/main/build-release) folder. You can also download this dataset from this [link](https://drive.google.com/file/d/1Lpo_LcmC0tqR-Gl7yyvPO7xIcwe5ZP9_/view?usp=drive_link).

A `config` file is need to compress or decompress a data set, which includes data set metadata. And the config file for
USCensus1990 is provided.

0. Benchmark [Archive mode]:

`./tabular_blitzcrank -b USCensus1990.dat USCensus1990.config 0 1 20000`

Output:
```
Delimiter: ,	Skip Learning: 1	Block Size: 20000	
[Compression]	Start load data into memory...
Data loaded.
Model Size: 3.26465 KB. 
Throughput:  108.524 MiB/s	Time:  3.15137 s
[Decompression]	Block Size: 286 tuple
Throughput:  187.538 MiB/s	Time:  1.82363 s
[Compression Factor (Origin Size / CompressedSize)]: 11.5654
Compressed Size: 31030821
```

### 1. Compress:

    ./tabular_blitzcrank -c USCensus1990.dat USCensus1990.com USCensus1990.config 0 1 20000`

Output:

    Delimiter: ,	Skip Learning: 1	Block Size: 20000	
    Start load data into memory...
    Data loaded.
    Iteration 1 Starts
    Model Size: 3.26465 KB. 
    Compressed Size: 31030821


### 2. Decompress

    ./tabular_blitzcrank -d USCensus1990.com USCensus1990.rec USCensus1990.config 0 20000

After the execution, you could 

    diff USCensus1990.dat USCensus1990.rec
to check the decompression result.

