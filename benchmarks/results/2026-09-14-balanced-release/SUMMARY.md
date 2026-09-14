# Resident independent-record queries

3-run medians of per-run means, not latency percentiles. Setup/CSV/JSON excluded. A one-run result is diagnostic, not stability evidence.

| Dataset | Uniform C++ ns | Uniform Rust ns | Ratio | Zipf C++ ns | Zipf Rust ns | Ratio | Size change |
|---|---:|---:|---:|---:|---:|---:|---:|
| jena_climate | 768.6 | 340.2 | 2.26x | 631.8 | 225.5 | 2.80x | -19.1% |
| covtype | 1089.6 | 454.2 | 2.40x | 935.9 | 409.8 | 2.28x | +31.5% |
| USCensus1990 | 1492.7 | 686.0 | 2.18x | 1295.7 | 633.9 | 2.04x | +1.6% |
| Food | 536.6 | 128.5 | 4.17x | 321.7 | 84.2 | 3.82x | -4.2% |
| Bimbo_1 | 941.9 | 345.2 | 2.73x | 543.1 | 214.3 | 2.53x | +7.9% |
| YaleLanguages | 1115.0 | 504.3 | 2.21x | 918.7 | 364.8 | 2.52x | +16.1% |
| Arade_1 | 759.4 | 463.8 | 1.64x | 492.6 | 338.9 | 1.45x | +65.7% |
| cps | 3804.1 | 1290.0 | 2.95x | 3508.1 | 1235.8 | 2.84x | +27.2% |
