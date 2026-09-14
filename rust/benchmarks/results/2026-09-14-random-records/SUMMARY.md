# Resident independent-record queries

Three-run medians of per-run means, not latency percentiles. Setup/CSV/JSON excluded.

| Dataset | Uniform C++ ns | Uniform Rust ns | Ratio | Zipf C++ ns | Zipf Rust ns | Ratio | Size change |
|---|---:|---:|---:|---:|---:|---:|---:|
| jena_climate | 767.2 | 341.4 | 2.25x | 629.5 | 223.3 | 2.82x | -19.1% |
| covtype | 1091.0 | 186.4 | 5.85x | 932.7 | 141.9 | 6.57x | +31.1% |
| USCensus1990 | 1492.2 | 280.8 | 5.31x | 1287.0 | 228.6 | 5.63x | -10.2% |
| Food | 537.2 | 130.0 | 4.13x | 323.5 | 84.5 | 3.83x | -4.1% |
| Bimbo_1 | 955.2 | 338.6 | 2.82x | 544.0 | 198.9 | 2.74x | +7.1% |
| YaleLanguages | 1115.0 | 374.9 | 2.97x | 917.0 | 256.0 | 3.58x | +8.5% |
| Arade_1 | 765.2 | 259.3 | 2.95x | 494.2 | 153.6 | 3.22x | +39.6% |
| cps | 3804.3 | 550.6 | 6.91x | 3525.3 | 477.3 | 7.39x | +23.5% |
