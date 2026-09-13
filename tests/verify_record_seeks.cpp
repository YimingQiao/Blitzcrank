// Reuse the CLI's schema/enum parsing, but verify every field after real index seeks.
#define main blitzcrank_cli_main
#include "../tabular.cpp"
#undef main
#include <numeric>

int main(int argc, char** argv) {
    try {
        if (argc != 2) throw std::runtime_error("usage: verify_record_seeks BLOCK_THRESHOLD");
        std::strcpy(config_file_name, "../input.config");
        LoadConfig(config_file_name);
        db_compress::Read(enum_map);
        std::ifstream input("../input.csv");
        std::vector<std::string> rows;
        std::string row;
        while (std::getline(input, row)) rows.push_back(row);
        if (rows.empty()) throw std::runtime_error("empty fixture");
        db_compress::RelationDecompressor decoder("payload.bin", schema, std::stoi(argv[1]));
        decoder.Init();
        std::vector<size_t> ids(rows.size());
        std::iota(ids.begin(), ids.end(), 0);
        std::mt19937 rng(123456);
        std::shuffle(ids.begin(), ids.end(), rng);
        ids.resize(std::min<size_t>(ids.size(), 2048));
        ids.push_back(0); ids.push_back(rows.size() - 1);
        db_compress::AttrVector tuple(schema.size());
        for (size_t id : ids) {
            decoder.LocateTuple(id);
            while (decoder.HasNext()) decoder.ReadNextTuple(&tuple);
            std::ostringstream actual;
            for (size_t field = 0; field < schema.size(); ++field) {
                std::string value;
                ExtractAttr(tuple, schema.attr_type_[field], field, value);
                if (field) actual << ',';
                actual << value;
            }
            if (actual.str() != rows[id]) throw std::runtime_error("seek mismatch at row " + std::to_string(id));
        }
        std::cout << "Verified " << ids.size() << " shuffled/boundary record seeks, all fields\n";
    } catch (const std::exception& error) { std::cerr << error.what() << '\n'; return 1; }
}
