#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>

#include <filereadstream.h>
#include <json_base.h>
#include <json_compression.h>
#include <json_decompression.h>
#include <timeseries_model.h>

using namespace rapidjson;

bool is_compress;
char input_file_name[100], output_file_name[100], config_file_name[100];
const int kDatasetSize = 50000;

class PokemonNumericalInterpreter : public db_compress::AttrInterpreter {
private:
  int cap_ = 2;

public:
  explicit PokemonNumericalInterpreter() {}
  bool EnumInterpretable() const override { return true; }
  int EnumCap() const override { return cap_; }
  size_t EnumInterpret(const db_compress::AttrValue &attr) const override {
    return attr.Int() > 100 ? 1 : 0;
  }
};

void PrintHelpInfo() {
  std::cout << "Usage:\n";
  std::cout << "Compression: sample -c input_file output_file\n";
  std::cout << "Decompression: sample -d input_file output_file\n";
}

bool ReadParameter(int argc, char **argv) {
  // mode, input_file_name, output_file_name, config_file_name, tuple_index,
  // decompress_num start tuple and decompress_num are for random
  // decompression
  if (argc < 5)
    return false;
  if (strcmp(argv[1], "-c") == 0)
    is_compress = true;
  else if (strcmp(argv[1], "-d") == 0) {
    is_compress = false;
  } else
    return false;

  strcpy(input_file_name, argv[2]);
  strcpy(output_file_name, argv[3]);
  strcpy(config_file_name, argv[4]);
  return true;
}

/**
 * Register attribute type creator and attribute interpreter, from the json
 * schema.
 *
 * @param schema The json schema.
 */
void RegisterJSONSchema(db_compress::JSONSchema schema) {
  // register attribute interpreter per attribute
  for (int i = 0; i < schema.path_type_.size(); ++i) {
    db_compress::NodeType type =
        db_compress::Num2NodeType(schema.path_type_[i]);
    switch (type) {
    case db_compress::NodeType::kStringType:
      db_compress::RegisterAttrInterpreter(i,
                                           new db_compress::AttrInterpreter());
      break;
    case db_compress::NodeType::kNumberType:
      db_compress::RegisterAttrInterpreter(i,
                                           new db_compress::AttrInterpreter());
      break;
    case db_compress::NodeType::kDoubleType:
      db_compress::RegisterAttrInterpreter(i,
                                           new db_compress::AttrInterpreter());
      break;
    default:
      break;
    }
  }

  // register attribute model per type
  RegisterAttrModel(0, new db_compress::TableCategoricalCreator());
  RegisterAttrModel(5, new db_compress::StringModelCreator());
  RegisterAttrModel(6, new db_compress::TableNumericalIntCreator());
  RegisterAttrModel(7, new db_compress::TableTimeSeriesCreator());
  RegisterAttrModel(8, new db_compress::TableNumericalRealCreator());
}

/**
 * Generate compression/decompression config automatically, this config can be
 * modified by user.
 *
 * @return compression/decompression config.
 */
db_compress::CompressionConfig GenerateConfig() {
  db_compress::CompressionConfig config;
  config.skip_model_learning_ = false;
  return config;
}

FileReadStream GetStream(FILE *file_ptr, char *read_buffer) {
  clearerr(file_ptr);
  fseek(file_ptr, 0, SEEK_SET);
  FileReadStream in_stream =
      FileReadStream(file_ptr, read_buffer, sizeof(read_buffer));
  return in_stream;
}

int main(int argc, char **argv) {
  if (argc == 1) {
    PrintHelpInfo();
  } else {
    if (!ReadParameter(argc, argv)) {
      std::cerr << "Bad Parameters.\n";
      return 1;
    }
    if (is_compress) {
      std::cout << "Json Name: " << input_file_name << std::endl;

      // 1. Config File.
      // 1-1. check existence of config file, if not exist, generate one
      if (!fopen(config_file_name, "r")) {
        db_compress::JSONSchemaGenerator generator(config_file_name);
        db_compress::JSONSchema json_schema =
            generator.GenerateSchema(input_file_name);
        json_schema.WriteJSONSchema();
      }

      // 1-2. Load json schema and register it
      db_compress::JSONSchema json_schema(config_file_name);
      RegisterJSONSchema(json_schema);

      // 2. Compress
      FILE *file_ptr = fopen(input_file_name, "r");
      if (file_ptr == nullptr) {
        std::cout << "File does not exist - " << input_file_name << "\n";
      }
      char read_buffer[65536];
      Document json;

      db_compress::JSONCompressor compressor(
          output_file_name, json_schema, kBlockSize, GenerateConfig(),
          db_compress::JSONModel::CreateJSONTree(json_schema));
      int iter_cnt = 0;

      // random number
      std::random_device random_device;
      std::mt19937 mt19937(0);
      std::uniform_real_distribution<double> dist(0, 1);

      // 2-1. Learning (random sample), record learning time
      auto learning_start = std::chrono::system_clock::now();
      while (true) {
        std::cout << "Iteration " << ++iter_cnt << "\n";
        int object_random_cnt = 0;
        bool has_parse_error = false;

        FileReadStream in_stream = GetStream(file_ptr, read_buffer);
        while (true) {
          has_parse_error = json.ParseStream<kParseStopWhenDoneFlag>(in_stream)
                                .HasParseError();
          if (has_parse_error &&
              object_random_cnt < kNumEstSample) {
            in_stream = GetStream(file_ptr, read_buffer);
            json.ParseStream<kParseStopWhenDoneFlag>(in_stream);
          }

          if (static_cast<double>(kNonFullPassStopPoint) / kDatasetSize <
              dist(mt19937))
            continue;

          Value &obj = json;
          compressor.LearnNode(obj);
          if (++object_random_cnt >= kNonFullPassStopPoint)
            break;
        }

        if (compressor.RequireFullPass())
          break;
        compressor.EndOfLearning();
      }
      auto learning_end = std::chrono::system_clock::now();
      auto learning_duration =
          std::chrono::duration_cast<std::chrono::microseconds>(learning_end -
                                                                learning_start);
      double learning_time = static_cast<double>(learning_duration.count()) *
                             std::chrono::microseconds ::period ::num /
                             std::chrono::microseconds::period::den;

      // 2-2. Full Pass, record tuning time
      auto tuning_start = std::chrono::system_clock::now();
      compressor.PrepareFullPass();
      std::cout << "Full Pass Learning Iteration " << ++iter_cnt << " Starts\n";
      FileReadStream in_stream = GetStream(file_ptr, read_buffer);
      while (!json.ParseStream<kParseStopWhenDoneFlag>(in_stream)
                  .HasParseError()) {
        Value &obj = json;
        compressor.LearnNode(obj);
      }
      compressor.EndOfLearning();
      auto tuning_end = std::chrono::system_clock::now();
      auto tuning_duration =
          std::chrono::duration_cast<std::chrono::microseconds>(tuning_end -
                                                                tuning_start);
      double tuning_time = static_cast<double>(tuning_duration.count()) *
                           std::chrono::microseconds ::period ::num /
                           std::chrono::microseconds::period::den;

      // 2-3. Compression Iteration, record compression total time
      std::cout << "Compression Iteration " << ++iter_cnt << " Starts\n";
      auto compress_start = std::chrono::system_clock::now();
      in_stream = GetStream(file_ptr, read_buffer);
      while (!json.ParseStream<kParseStopWhenDoneFlag>(in_stream)
                  .HasParseError()) {
        Value &obj = json;
        compressor.CompressNode(obj);
      }
      compressor.EndOfCompress();

      auto compress_end = std::chrono::system_clock::now();
      auto compress_duration =
          std::chrono::duration_cast<std::chrono::microseconds>(compress_end -
                                                                compress_start);
      double compression_time = static_cast<double>(compress_duration.count()) *
                                std::chrono::microseconds ::period ::num /
                                std::chrono::microseconds::period::den;

      // Record IO time - Read
      auto start_io = std::chrono::system_clock::now();
      in_stream = GetStream(file_ptr, read_buffer);
      while (
          !json.ParseStream<kParseStopWhenDoneFlag>(in_stream).HasParseError())
        Value &obj = json;
      auto end_io = std::chrono::system_clock::now();
      auto duration_io = std::chrono::duration_cast<std::chrono::microseconds>(
          end_io - start_io);
      double io_time = static_cast<double>(duration_io.count()) *
                       std::chrono::microseconds ::period ::num /
                       std::chrono::microseconds::period::den;

      double ratio = static_cast<double>(kDatasetSize) / kNonFullPassStopPoint;
      std::cout << "Learning Time:  " << (learning_time - io_time * ratio)
                << "s\n";
      std::cout << "Tuning Time: " << (tuning_time - io_time) << "s\n";
      std::cout << "Compression Time and write:  "
                << (compression_time - io_time) << "s\n";
      std::cout << "IO - Read Time: " << io_time << "s\n";

      fclose(file_ptr);
    } else {
      // check existence of config file, if not exist, generate one
      if (!fopen(config_file_name, "r"))
        throw db_compress::IOException("Config file does not exist.\n");

      double decompress_io_time;
      double decompress_time;
      {
        // Load and register json schema
        db_compress::JSONSchema json_schema(config_file_name);
        RegisterJSONSchema(json_schema);

        // Decompress
        std::ofstream output_file(output_file_name);
        StringBuffer buffer;
        Writer<StringBuffer> writer(buffer);
        Document::AllocatorType allocator = Document().GetAllocator();
        db_compress::JSONDecompressor decompressor(allocator, input_file_name,
                                                   json_schema, kBlockSize);
        decompressor.Init();
        int node_count = 0;

        auto decompress_start = std::chrono::system_clock::now();
        while (decompressor.HasNext()) {
          rapidjson::Value &node = decompressor.ReadNextNode();

          node.Accept(writer);
          output_file << buffer.GetString() << "\n";
          buffer.Clear();

          // Reset writer
          writer.Reset(buffer);

          node_count++;
        }
        output_file.close();
        auto decompress_end = std::chrono::system_clock::now();
        auto decompress_duration =
            std::chrono::duration_cast<std::chrono::microseconds>(
                decompress_end - decompress_start);
        decompress_io_time = static_cast<double>(decompress_duration.count()) *
                             std::chrono::microseconds ::period ::num /
                             std::chrono::microseconds::period::den;
      }

      {
        // Load and register json schema
        db_compress::JSONSchema json_schema(config_file_name);
        RegisterJSONSchema(json_schema);

        // Decompress
        Document::AllocatorType allocator = Document().GetAllocator();
        db_compress::JSONDecompressor decompressor(allocator, input_file_name,
                                                   json_schema, kBlockSize);
        decompressor.Init();

        auto decompress_start = std::chrono::system_clock::now();
        while (decompressor.HasNext()) {
          rapidjson::Value &node = decompressor.ReadNextNode();
        }
        auto decompress_end = std::chrono::system_clock::now();
        auto decompress_duration =
            std::chrono::duration_cast<std::chrono::microseconds>(
                decompress_end - decompress_start);
        decompress_time = static_cast<double>(decompress_duration.count()) *
                          std::chrono::microseconds ::period ::num /
                          std::chrono::microseconds::period::den;
      }

      std::cout << "Decompression Time:  " << (decompress_time) << "s\n";
      std::cout << "IO - Write : " << (decompress_io_time - decompress_time)
                << "s\n";
    }
  }

  return 0;
}
