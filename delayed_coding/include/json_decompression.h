/**
 * @file json_decompression.h
 * @brief The header of Decompressor for JSON-style files.
 */

#ifndef DB_COMPRESS_JSON_DECOMPRESSION_H
#define DB_COMPRESS_JSON_DECOMPRESSION_H

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "base.h"
#include "categorical_model.h"
#include "data_io.h"
#include "index.h"
#include "json_base.h"
#include "json_model.h"
#include "model.h"
#include "numerical_model.h"

namespace db_compress {

/**
 * String buffer pool_.
 */
class StringBufferPool {
 public:
  StringBufferPool(size_t string_buffer_size)
      : string_buffer_index_(0), string_buffer_(string_buffer_size) {}

  /**
   * String buffer pool_, it is used to reduce the times of string memory
   * allocation.
   *
   * @param str the string to buffer
   * @return buffered string
   */
  const char *BufferString(const std::string &str);

  void Clear() { string_buffer_index_ = 0; }

 private:
  std::vector<std::unique_ptr<std::string>> string_buffer_;
  int string_buffer_index_;
};

/**
 * This decompressor is for json file.
 */
class JSONDecompressor {
 public:
  /**
   * Create a json decompressor.
   *
   * @param allocator A concept in rapidjson, it is used to memory allocation
   * @param compressed_file_name compressed file address
   * @param json_schema json schema contains all path of terminal nodes and
   * attributes type
   * @param block_size once decompressed tuple number is larger than block size,
   * reset decoder
   */
  JSONDecompressor(rapidjson::Document::AllocatorType &allocator, const char *compressed_file_name,
                   JSONSchema json_schema, int block_size);
  /**
   * Init a decompressor. including loading sketch tree from disks.
   */
  void Init();

  /**
   * Check if next tuple is existed.
   *
   * @return return true if next tuple exists; or return false
   */
  bool HasNext() const;

  /**
   * Decompress next tuple, existence of next tuple should be checked before
   * calling this function.
   *
   * @param[out] tuple decompressed result
   */
  rapidjson::Value &ReadNextNode();

 private:
  // initialization
  JSONSchema json_schema_;
  rapidjson::Document::AllocatorType &allocator_;
  int num_total_nodes_, num_converted_nodes_;

  // record attribute values for learning
  AttrVector attr_record_;

  // sketch tree and real tree
  std::unique_ptr<JSONModel> sketch_root_;
  rapidjson::Value real_json_root_;

  // string buffer pool_
  StringBufferPool string_buffer_pool_;

  const int kBlockSizeThreshold;
  ByteReader byte_reader_;
  Decoder decoder_;

  std::vector<JSONModel *> sketch_stack_;
  std::vector<rapidjson::Value *> node_stack_;

  /**
   * Decompress current node in a json tree. Decompression of json is a
   * recursive process. Sketch node and real node are one-to-one. We traverse
   * the sketch tree, and put decompressed result in real tree.
   *
   * @param sketch_root sketch node, it contains all learned information
   * @param sample decompressed result
   */
  void DecompressNode(JSONModel *sketch_root, rapidjson::Value &sample);
};

}  // namespace db_compress
#endif  // DB_COMPRESS_JSON_DECOMPRESSION_H
