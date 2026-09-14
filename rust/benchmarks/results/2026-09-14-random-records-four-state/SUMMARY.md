# Resident independent-record queries

One-run diagnostic, not stability evidence or latency percentiles. Setup/CSV/JSON excluded.

| Dataset | Uniform C++ ns | Uniform Rust ns | Ratio | Zipf C++ ns | Zipf Rust ns | Ratio | Size change |
|---|---:|---:|---:|---:|---:|---:|---:|
| jena_climate | 771.4 | 301.1 | 2.56x | 629.2 | 205.8 | 3.06x | -2.4% |
| covtype | 1092.1 | 201.1 | 5.43x | 917.3 | 155.4 | 5.90x | +91.9% |
| USCensus1990 | 1497.5 | 245.5 | 6.10x | 1294.1 | 194.4 | 6.66x | +27.1% |
| Food | 541.3 | 157.8 | 3.43x | 323.9 | 98.4 | 3.29x | +60.5% |
| Bimbo_1 | 964.7 | 309.8 | 3.11x | 535.9 | 172.8 | 3.10x | +44.8% |
| YaleLanguages | 1114.2 | 342.2 | 3.26x | 913.4 | 225.8 | 4.05x | +43.2% |
| Arade_1 | 761.6 | 281.2 | 2.71x | 509.0 | 174.0 | 2.92x | +76.9% |
| cps | 3802.8 | 488.6 | 7.78x | 3515.9 | 428.3 | 8.21x | +36.5% |
