// Same precomputed xorshift64 query sequence and warm-up as Rust seek-bench.
// Run inside an existing C++ record directory with ../input.config and sidecars.
#define main blitzcrank_cli_main
#ifdef BLITZCRANK_SOURCE
#include BLITZCRANK_SOURCE
#else
#include "../../tabular.cpp"
#endif
#undef main

int main(int argc, char** argv) {
    if (argc != 1 && argc != 4) { std::cerr << "cpp_seek [TRACE SCHEMA DELIMITER_FLAG]\n"; return 2; }
    const char* config = argc == 4 ? argv[2] : "../input.config";
    if (std::strlen(config) >= sizeof(config_file_name)) return 2;
    std::strcpy(config_file_name, config);
    if (argc == 4 && std::string(argv[3]) == "1") delimiter = '|';
    LoadConfig(config_file_name);
    db_compress::Read(enum_map);
    db_compress::RelationDecompressor decoder("payload.bin", schema, 1);
    decoder.Init();
    size_t queries = 300000;
    uint64_t rng = 123456;
    std::vector<size_t> ids;
    if (argc == 4) {
        std::ifstream trace(argv[1], std::ios::binary);
        if (!trace) return 2;
        unsigned char word[4];
        while (trace.read(reinterpret_cast<char*>(word), 4)) {
            const uint32_t row = uint32_t(word[0]) | (uint32_t(word[1]) << 8) | (uint32_t(word[2]) << 16) | (uint32_t(word[3]) << 24);
            if (row >= decoder.num_total_tuples_ || ids.size() >= 10000000) return 2;
            ids.push_back(row);
        }
        if (!trace.eof() || trace.gcount() != 0 || ids.empty()) return 2;
        queries = ids.size();
    } else {
        ids.reserve(queries);
        for (size_t i = 0; i < queries; ++i) {
            rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17;
            ids.push_back(rng % decoder.num_total_tuples_);
        }
    }
    db_compress::AttrVector tuple(schema.size());
    decoder.LocateTuple(0);
    while (decoder.HasNext()) decoder.ReadNextTuple(&tuple);
    // Match the typed helper's verification-prefix warm-up, outside timing.
    for (size_t i = 0; i < std::min<size_t>(1000, ids.size()); ++i) {
        decoder.LocateTuple(ids[i]);
        while (decoder.HasNext()) decoder.ReadNextTuple(&tuple);
    }
    const auto start = std::chrono::steady_clock::now();
    for (size_t row : ids) {
        decoder.LocateTuple(row);
        while (decoder.HasNext()) decoder.ReadNextTuple(&tuple);
    }
    const auto end = std::chrono::steady_clock::now();
    // The decoder is in the unchanged, separately compiled non-LTO library;
    // calls materialize tuple fields and are opaque to this translation unit.
    std::cout << "fields=" << schema.size() << " queries=" << queries
              << " mean_ns_per_row="
              << std::chrono::duration<double, std::nano>(end-start).count()/queries << '\n';
}
