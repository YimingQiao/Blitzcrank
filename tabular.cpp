#include <chrono>
#include <cstring>
#include <iostream>
#include <random>
#include <sstream>
#include <string>

#include <categorical_model.h>
#include <compression.h>
#include <decompression.h>
// #include <markov_model.h>
#include <csignal>
#include <model.h>
#include <numerical_model.h>
#include <string_model.h>
#include <unistd.h>

class SimpleCategoricalInterpreter : public db_compress::AttrInterpreter {
private:
    int cap_;

public:
    explicit SimpleCategoricalInterpreter(int cap) : cap_(cap) {}

    bool EnumInterpretable() const override { return true; }

    int EnumCap() const override { return cap_; }

    size_t EnumInterpret(const db_compress::AttrValue &attr) const override {
        return attr.Int();
    }
};

enum {
    COMPRESS, DECOMPRESS, BENCHMARK, RANDOM_ACCESS
} mode;

// ---------------------------- Global Variables -----------------------------
char input_file_name[100], output_file_name[100], config_file_name[100];
db_compress::Schema schema;
std::vector<db_compress::AttrVector> datasets;
db_compress::CompressionConfig config;
std::vector<db_compress::BiMap> enum_map;

// ----------------------------- Setting ---------------------------------
char delimiter = ',';
bool skip_learning = true;
int block_size = 20000;

// -------------------------- Helper Functions ---------------------------

int EnumTranslate(const std::string &str, int attr) {
    db_compress::BiMap &map = enum_map[attr];
    auto &enum2idx = map.enum2idx;
    auto &enums = map.enums;

    auto it = enum2idx.find(str);
    if (it != enum2idx.end())
        return it->second;
    else {
        enum2idx[str] = enums.size();
        enums.push_back(str);
        return enums.size() - 1;
    }
}

std::ifstream::pos_type filesize(const char *filename) {
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
}

void PrintHelpInfo() {
    std::cout << "Compression How To:\n\n";
    std::cout << "./tabular_blitzcrank [mode] [dataset] [config] [if use \"|\" as delimiter] [if skip learning] [block size]\n\n";
    std::cout << "    [mode]: -c for compression, -d for decompression, -b for benchmarking\n";
    std::cout << "    [dataset]: path to the dataset\n";
    std::cout << "    [config]: path to the config file\n";
    std::cout << "    [if use \"|\" as delimiter]: 0 for comma, 1 for \"|\"\n";
    std::cout << "    [if skip learning]: 0 for learning, 1 for skipping learning\n";
    std::cout << "    [block size]: block size for compression\n";
}

// Read input_file_name, output_file_name, config_file_name and whether to
// compress or decompress.csv. Return false if failed to recognize params.
bool ReadParameter(int argc, char **argv) {
    // mode, input_file_name, output_file_name, config_file_name, tuple_idx_,
    // decompress_num start tuple and decompress_num are for random
    // decompression
    if (strcmp(argv[1], "-c") == 0) {
        mode = COMPRESS;
    } else if (strcmp(argv[1], "-d") == 0) {
        mode = DECOMPRESS;
    } else if (strcmp(argv[1], "-b") == 0) {
        mode = BENCHMARK;
    } else if (strcmp(argv[1], "-ra") == 0) {
        mode = RANDOM_ACCESS;
    } else {
        return false;
    }

    switch (mode) {
        case COMPRESS: {
            if (argc < 5)
                return false;

            strcpy(input_file_name, argv[2]);
            strcpy(output_file_name, argv[3]);
            strcpy(config_file_name, argv[4]);
            int special_del = std::stoi(argv[5]);
            if (special_del == 1)
                delimiter = '|';
            skip_learning = std::stoi(argv[6]);
            block_size = std::stoi(argv[7]);
            std::cout << "Delimiter: " << delimiter << "\t"
                      << "Skip Learning: " << skip_learning << "\t"
                      << "Block Size: " << block_size << "\t" << std::endl;
        }
            break;
        case DECOMPRESS: {
            if (argc < 5)
                return false;
            strcpy(input_file_name, argv[2]);
            strcpy(output_file_name, argv[3]);
            strcpy(config_file_name, argv[4]);
            int special_del = std::stoi(argv[5]);
            if (special_del == 1)
                delimiter = '|';
            block_size = std::stoi(argv[6]);
            std::cout << "Delimiter: " << delimiter << "\t"
                      << "Block Size: " << block_size << "\t" << std::endl;
        }
            break;
        case BENCHMARK: {
            if (argc < 4)
                return false;
            strcpy(input_file_name, argv[2]);
            std::string com = std::to_string(getpid()) + "_file.com";
            strcpy(output_file_name, com.c_str());
            strcpy(config_file_name, argv[3]);
            if (argc == 7) {
                int special_del = std::stoi(argv[4]);
                if (special_del == 1)
                    delimiter = '|';
                skip_learning = std::stoi(argv[5]);
                block_size = std::stoi(argv[6]);
            }
            std::cout << "Delimiter: " << delimiter << "\t"
                      << "Skip Learning: " << skip_learning << "\t"
                      << "Block Size: " << block_size << "\t" << std::endl;
            break;
        }
        case RANDOM_ACCESS: {
            if (argc < 4)
                return false;
            strcpy(input_file_name, argv[2]);
            strcpy(config_file_name, argv[3]);
            std::string com = std::to_string(getpid()) + "_file.com";
            strcpy(output_file_name, com.c_str());
            int special_del = std::stoi(argv[4]);
            if (special_del == 1)
                delimiter = '|';
            skip_learning = std::stoi(argv[5]);
            block_size = std::stoi(argv[6]);
            std::cout << "Delimiter: " << delimiter << "\t"
                      << "Skip Learning: " << skip_learning << "\t"
                      << "Block Size: " << block_size << "\t" << std::endl;
        }
            break;
    }
    return true;
}

void LoadConfig(char *configFileName_) {
    std::ifstream fin(configFileName_);
    if (!fin.is_open()) {
        std::cout << "Cannot open config file " << configFileName_ << std::endl;
        exit(1);
    }
    std::string str;
    std::vector<double> err;
    std::vector<int> attr_type;
    while (std::getline(fin, str)) {
        if (str.back() == '\r')
            str.pop_back();

        std::vector<std::string> vec;
        std::string item;
        std::stringstream sstream(str);
        while (std::getline(sstream, item, ' ')) {
            vec.push_back(item);
        }

        int index = static_cast<int>(attr_type.size());
        if (vec[0] == "ENUM") {
            if (vec.size() != 3) {
                std::cerr << "ENUM config error." << std::endl;
                exit(1);
            }

            RegisterAttrInterpreter(
                    index, new SimpleCategoricalInterpreter(std::stoi(vec[1])));
            err.push_back(std::stod(vec[2]));
            attr_type.push_back(0);
        } else if (vec[0] == "ENUM-MARKOV") {
            if (vec.size() != 2) {
                std::cerr << "ENUM-MARKOV config error." << std::endl;
                exit(1);
            }

            RegisterAttrInterpreter(
                    index, new SimpleCategoricalInterpreter(std::stoi(vec[1])));
            err.push_back(std::stod(vec[1]));
            attr_type.push_back(5);
        } else if (vec[0] == "INTEGER") {
            if (vec.size() != 2) {
                std::cerr << "INTEGER config error." << std::endl;
                exit(1);
            }

            RegisterAttrInterpreter(index, new db_compress::AttrInterpreter());
            err.push_back(std::stod(vec[1]));
            attr_type.push_back(1);
        } else if (vec[0] == "DOUBLE") {
            if (vec.size() != 2) {
                std::cerr << "DOUBLE config error." << std::endl;
                exit(1);
            }

            RegisterAttrInterpreter(index, new db_compress::AttrInterpreter());
            err.push_back(std::stod(vec[1]));
            attr_type.push_back(2);
        } else if (vec[0] == "STRING") {
            if (vec.size() != 1) {
                std::cerr << "STRING config error." << std::endl;
                exit(1);
            }
            RegisterAttrInterpreter(index, new db_compress::AttrInterpreter());
            err.push_back(0);
            attr_type.push_back(3);
        } else if (vec[0] == "TIMESERIES") {
            if (vec.size() != 2) {
                std::cerr << "TIMESERIES config error." << std::endl;
                exit(1);
            }

            RegisterAttrInterpreter(index, new db_compress::AttrInterpreter());
            err.push_back(std::stod(vec[1]));
            attr_type.push_back(4);
        } else {
            std::cerr << "Config File Error!\n";
        }
    }

    // Register attributed model and interpreter
    RegisterAttrModel(0, new db_compress::TableCategoricalCreator());
    RegisterAttrModel(1, new db_compress::TableNumericalIntCreator());
    RegisterAttrModel(2, new db_compress::TableNumericalRealCreator());
    RegisterAttrModel(3, new db_compress::StringModelCreator());
    // RegisterAttrModel(5, new db_compress::TableMarkovCreator());
    // RegisterAttrModel(4, new db_compress::TableTimeSeriesCreator());

    if (attr_type.empty()) {
        std::cerr << "Config File Error!\n";
    }
    schema = db_compress::Schema(attr_type);
    config.allowed_err_ = err;
    config.skip_model_learning_ = skip_learning;

    enum_map.resize(attr_type.size());
}

inline void AppendAttr(db_compress::AttrVector *tuple, const std::string &str,
                       int attr_type_local, int index) {
    try {
        switch (attr_type_local) {
            case 0:
                tuple->attr_[index].value_ = (EnumTranslate(str, index));
                break;
            case 1:
                tuple->attr_[index].value_ = (std::stoi(str));
                break;
            case 2:
                tuple->attr_[index].value_ = (std::stod(str));
                break;
            case 3:
                tuple->attr_[index].value_ = (str);
                break;
            case 4:
                tuple->attr_[index].value_ = (std::stod(str));
            case 5:
                tuple->attr_[index].value_ = (std::stoi(str));
                break;
            default:
                break;
        }
    } catch (std::exception &e) {
        std::cerr << "Error: " << e.what() << "\tCol: " << index
                  << "\tValue: " << str << std::endl;
        exit(1);
    }
}

inline void ExtractAttr(const db_compress::AttrVector &tuple,
                        int attr_type_local, int index, std::string &ret) {
    const db_compress::AttrValue attr = tuple.attr_[index];
    switch (attr_type_local) {
        case 0:
            ret = enum_map[index].enums[attr.Int()];
            break;
        case 1:
            ret = std::to_string(attr.Int());
            break;
        case 2:
            ret = std::to_string(attr.Double());
            break;
        case 3:
            ret = attr.String();
            break;
        case 4:
            ret = std::to_string(attr.Double());
            break;
        case 5:
            ret = std::to_string(attr.Int());
            break;
        default:
            break;
    }
}

int LoadDataSet() {
    std::ios::sync_with_stdio(false);

    std::cout << "Start load data into memory..." << std::endl;

    db_compress::AttrVector tuple(schema.size());
    std::ifstream in_file(input_file_name);
    if (!in_file.is_open()) {
        std::cout << "Cannot open input file " << input_file_name << std::endl;
        exit(1);
    }
    std::string str;
    while (std::getline(in_file, str)) {
        // empty line should be ignored
        if (str.empty()) {
            std::cerr << "File is empty.\n";
            continue;
        }

        if (str.back() == '\r')
            str.pop_back();

        int count = 0;
        std::string item;
        std::stringstream sstream(str);

        while (std::getline(sstream, item, delimiter)) {
            if (delimiter == ',') {
                if (item.size() > 0 && item[0] == '"') {
                    item = item.substr(1);
                    item += ',';
                    std::string item2;
                    if (std::getline(sstream, item2, '"'))
                        item += item2;

                    std::getline(sstream, item2, delimiter);
                }
            }
            AppendAttr(&tuple, item, schema.attr_type_[count], count);
            ++count;
        }

        // The last item might be empty string
        if (str[str.length() - 1] == delimiter) {
            AppendAttr(&tuple, "", schema.attr_type_[count], count);
            ++count;
        }

        if (count != schema.size()) {
            std::string ret =
                    "File Format Error! Got: " + std::to_string(count) + " Wanted: " + std::to_string(schema.size()) +
                    "\n";
            ret += str + "\n";
            throw std::runtime_error(ret);
        }

        datasets.push_back(tuple);
        // std::cout << "Load " << datasets.size() << " tuples\n";
    }

    // write down enum attrs.
    db_compress::Write(enum_map);

    std::cout << "Data loaded.\n";

    return datasets.size();
}

int main(int argc, char **argv) {
    if (argc == 1) {
        PrintHelpInfo();
    } else {
        if (!ReadParameter(argc, argv)) {
            std::cerr << "Bad Parameters.\n";
            return 1;
        }
        std::ios::sync_with_stdio(false);
        LoadConfig(config_file_name);
        switch (mode) {
            case COMPRESS: {
                db_compress::RelationCompressor compressor(output_file_name, schema,
                                                           config, block_size);
                int num_total_tuples = LoadDataSet();
                int iter_cnt = 0;

                // random number
                std::random_device random_device;
                std::mt19937 mt19937(0);
                std::uniform_int_distribution<uint32_t> dist(0, num_total_tuples - 1);
                bool tuning = false;

                // Learning Iterations
                while (true) {
                    std::cout << "Iteration " << ++iter_cnt << " Starts\n";

                    int tuple_cnt = 0;
                    int tuple_random_cnt = 0;
                    int tuple_idx;

                    while (tuple_cnt < num_total_tuples) {
                        if (tuple_random_cnt < kNumEstSample) {
                            tuple_idx = static_cast<int>(dist(mt19937));
                            tuple_random_cnt++;
                        } else {
                            tuple_idx = tuple_cnt;
                            tuple_cnt++;
                        }

                        db_compress::AttrVector &tuple = datasets[tuple_idx];
                        { compressor.LearnTuple(tuple); }
                        if (tuple_cnt >= kNonFullPassStopPoint &&
                            !compressor.RequireFullPass()) {
                            break;
                        }
                    }
                    compressor.EndOfLearning();

                    if (!tuning && compressor.RequireFullPass()) {
                        tuning = true;
                    }

                    if (!compressor.RequireMoreIterationsForLearning()) {
                        break;
                    }
                }

                // Compression iteration
                // std::cout << "Compression Iteration " << ++iter_cnt << " Starts\n";
                for (int i = 0; i < num_total_tuples; ++i) {
                    db_compress::AttrVector &tuple = datasets[i];
                    compressor.CompressTuple(tuple);
                }
                compressor.EndOfCompress();
                std::cout << "Compressed Size: " << filesize(output_file_name) << "\n";

//                std::string index_file_name = std::to_string(getpid()) + "_temp.index";
//                std::string enum_file_name = std::to_string(getpid()) + "_enum.dat";
//                remove(index_file_name.c_str());
//                remove(enum_file_name.c_str());
            }
                break;
            case DECOMPRESS: {
                // Load enum values
                db_compress::Read(enum_map);

                // Decompress
                db_compress::RelationDecompressor decompressor(input_file_name, schema,
                                                               block_size);
                std::ofstream out_file(output_file_name);
                std::string str;
                decompressor.Init();
                db_compress::AttrVector tuple(static_cast<int>(schema.size()));

                while (decompressor.HasNext()) {
                    decompressor.ReadNextTuple(&tuple);

                    // Write record into file.
                    for (size_t i = 0; i < schema.size(); ++i) {
                        ExtractAttr(tuple, schema.attr_type_[i], static_cast<int>(i), str);
                        bool has_del = false;
                        for (size_t j = 0; j < str.size(); ++j) {
                            if (str[j] == ',') {
                                out_file << '\"';
                                has_del = true;
                                break;
                            }
                        }
                        out_file << str;
                        if (has_del)
                            out_file << '\"';
                        out_file << (i == schema.size() - 1 ? '\n' : delimiter);
                    }
                }
                out_file.close();
            }
                break;
            case BENCHMARK: {
                int origin_size = filesize(input_file_name);
                {
                    // Compress
                    std::cout << "[Compression]\t";
                    db_compress::RelationCompressor compressor(output_file_name, schema,
                                                               config, block_size);
                    int num_total_tuples = LoadDataSet();

                    // random number
                    std::random_device random_device;
                    std::mt19937 mt19937(0);
                    std::uniform_int_distribution<uint32_t> dist(0, num_total_tuples - 1);
                    bool tuning = false;

                    // Learning Iterations
                    while (true) {
                        int tuple_cnt = 0;
                        int tuple_random_cnt = 0;
                        int tuple_idx;

                        while (tuple_cnt < num_total_tuples) {
                            if (tuple_random_cnt < kNumEstSample) {
                                tuple_idx = static_cast<int>(dist(mt19937));
                                tuple_random_cnt++;
                            } else {
                                tuple_idx = tuple_cnt;
                                tuple_cnt++;
                            }

                            db_compress::AttrVector &tuple = datasets[tuple_idx];
                            { compressor.LearnTuple(tuple); }
                            if (tuple_cnt >= kNonFullPassStopPoint &&
                                !compressor.RequireFullPass()) {
                                break;
                            }
                        }
                        compressor.EndOfLearning();

                        if (!tuning && compressor.RequireFullPass()) {
                            tuning = true;
                        }

                        if (!compressor.RequireMoreIterationsForLearning()) {
                            break;
                        }
                    }

                    // Compression iteration
                    // std::cout << "Compression Iteration " << ++iter_cnt << " Starts\n";
                    auto compression_start = std::chrono::system_clock::now();
                    for (int i = 0; i < num_total_tuples; ++i) {
                        db_compress::AttrVector &tuple = datasets[i];
                        compressor.CompressTuple(tuple);
                    }
                    compressor.EndOfCompress();
                    auto compression_end = std::chrono::system_clock::now();
                    auto compression_duration =
                            std::chrono::duration_cast<std::chrono::microseconds>(
                                    compression_end - compression_start);
                    std::cout << "Throughput:  "
                              << origin_size / 1024 / 1024 /
                                 (static_cast<double>(compression_duration.count()) *
                                  std::chrono::microseconds::period::num /
                                  std::chrono::microseconds::period::den)
                              << " MiB/s\t";
                    std::cout << "Time:  "
                              << static_cast<double>(compression_duration.count()) *
                                 std::chrono::microseconds::period::num /
                                 std::chrono::microseconds::period::den
                              << " s\n";
                }
                {
                    // Decompress
                    std::cout << "[Decompression]\t";
                    db_compress::RelationDecompressor decompressor(output_file_name, schema,
                                                                   block_size);
                    decompressor.Init();
                    db_compress::AttrVector tuple(static_cast<int>(schema.size()));
                    auto decompress_start = std::chrono::system_clock::now();
                    while (decompressor.HasNext())
                        decompressor.ReadNextTuple(&tuple);

                    auto decompress_end = std::chrono::system_clock::now();
                    auto decompress_duration =
                            std::chrono::duration_cast<std::chrono::microseconds>(
                                    decompress_end - decompress_start);
                    std::cout << "Throughput:  "
                              << origin_size / 1024 / 1024 /
                                 (static_cast<double>(decompress_duration.count()) *
                                  std::chrono::microseconds::period::num /
                                  std::chrono::microseconds::period::den)
                              << " MiB/s\t";
                    std::cout << "Time:  "
                              << static_cast<double>(decompress_duration.count()) *
                                 std::chrono::microseconds::period::num /
                                 std::chrono::microseconds::period::den
                              << " s\n";
                }
                int compressed_size = filesize(output_file_name);
                std::cout << "[Compression Factor (Origin Size / CompressedSize)]: "
                          << origin_size / (double) compressed_size << "\n";
                std::cout << "Compressed Size: " << compressed_size << "\n";
                remove(output_file_name);
//                std::string index_file_name = std::to_string(getpid()) + "_temp.index";
//                std::string enum_file_name = std::to_string(getpid()) + "_enum.dat";
                std::string index_file_name = "_temp.index";
                std::string enum_file_name = "_enum.dat";
                remove(index_file_name.c_str());
                remove(enum_file_name.c_str());
            }
                break;
            case RANDOM_ACCESS: {
                //
                {
                    // compress first
                    db_compress::RelationCompressor compressor(output_file_name, schema,
                                                               config, block_size);
                    int num_total_tuples = LoadDataSet();
                    int iter_cnt = 0;

                    // random number
                    std::random_device random_device;
                    std::mt19937 mt19937(0);
                    std::uniform_int_distribution<uint32_t> dist(0, num_total_tuples - 1);
                    bool tuning = false;

                    // Learning Iterations
                    while (true) {
                        std::cout << "Iteration " << ++iter_cnt << " Starts\n";

                        int tuple_cnt = 0;
                        int tuple_random_cnt = 0;
                        int tuple_idx;

                        while (tuple_cnt < num_total_tuples) {
                            if (tuple_random_cnt < kNumEstSample) {
                                tuple_idx = static_cast<int>(dist(mt19937));
                                tuple_random_cnt++;
                            } else {
                                tuple_idx = tuple_cnt;
                                tuple_cnt++;
                            }

                            db_compress::AttrVector &tuple = datasets[tuple_idx];
                            { compressor.LearnTuple(tuple); }
                            if (tuple_cnt >= kNonFullPassStopPoint &&
                                !compressor.RequireFullPass()) {
                                break;
                            }
                        }
                        compressor.EndOfLearning();

                        if (!tuning && compressor.RequireFullPass()) {
                            tuning = true;
                        }

                        if (!compressor.RequireMoreIterationsForLearning()) {
                            break;
                        }
                    }

                    // Compression iteration
                    // std::cout << "Compression Iteration " << ++iter_cnt << " Starts\n";
                    for (int i = 0; i < num_total_tuples; ++i) {
                        db_compress::AttrVector &tuple = datasets[i];
                        compressor.CompressTuple(tuple);
                    }
                    compressor.EndOfCompress();
                    std::cout << "Compressed Size: " << filesize(output_file_name) << "\n";
                }

                // Random Access Test
                // InitModels decompressor
                std::cout << "[Random Access Test]\t";
                std::cout << "Note that in this test, number of tuple in a block should "
                             "be only ONE.\n";
                // Load enum values
                db_compress::Read(enum_map);
                db_compress::RelationDecompressor decompressor(output_file_name, schema,
                                                               block_size);
                decompressor.Init();
                db_compress::AttrVector tuple(static_cast<int>(schema.size()));

#if DEBUG == 1
                int size = 5;
                std::vector<uint32_t> tuple_indices(size);
                for (int i = 0; i < size; i++)
                  tuple_indices[i] = 2 * i;
#else
                // Seed Generator
                std::random_device random_device;
                std::mt19937 mt19937(0);
                std::uniform_int_distribution<uint32_t> dist(
                        0, decompressor.num_total_tuples_ - 1);
                size_t size = 300000;
                std::vector<uint32_t> tuple_indices(size);
                for (size_t i = 0; i < size; i++) {
                    uint32_t idx = dist(mt19937);
                    tuple_indices[i] = idx;
                }
#endif

                auto start = std::chrono::system_clock::now();
                for (int idx: tuple_indices) {
#if DEBUG == 0
                    // decompressor.ReadTargetTuple(idx, &tuple);
                    decompressor.LocateTuple(idx);
                    while (decompressor.HasNext())
                        decompressor.ReadNextTuple(&tuple);
#else
                    decompressor.LocateTuple(idx);
                    while (decompressor.HasNext())
                      decompressor.ReadNextTuple(&tuple);
#endif

#if DEBUG
                    for (size_t i = 0; i < schema.size(); ++i) {
                      std::string str;
                      ExtractAttr(tuple, schema.attr_type_[i], i, str);
                      std::cout << str << (i == schema.size() - 1 ? '\n' : ',');
                    }
#endif
                }
                auto end = std::chrono::system_clock::now();
                auto duration =
                        std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                std::cout << "Time:  "
                          << static_cast<double>(duration.count()) *
                             std::chrono::microseconds::period::num /
                             std::chrono::microseconds::period::den / (int) size * 1e6
                          << " us\n";
                std::cout << "-------------------------------------------------------" << std::endl;
                std::string index_file_name = std::to_string(getpid()) + "_temp.index";
                std::string enum_file_name = std::to_string(getpid()) + "_enum.dat";
                remove(index_file_name.c_str());
                remove(enum_file_name.c_str());
                remove(output_file_name);
            }
                break;
        }
    }
    return 0;
}
