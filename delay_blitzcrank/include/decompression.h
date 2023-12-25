/**
 * @file decompression.h
 * @brief The decompression process class header
 */

#ifndef DECOMPRESSION_H
#define DECOMPRESSION_H

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include "base.h"
#include "categorical_model.h"
#include "data_io.h"
#include "index.h"
#include "markov_model.h"
#include "model.h"
#include "numerical_model.h"
#include "string_model.h"
#include "string_tools.h"

namespace db_compress {

/**
 * It is used to recovered information from compressed binary file.
 * First, it needs to know where to start decompression; Then, decompress.csv next
 * tuple with ReadNextTuple function until all needed tuples have been given.
 */
class RelationDecompressor {
 public:
  int num_total_tuples_;

  /**
   * Create a decompressor.
   *
   * @param compressed_file_name compressed file address
   * @param schema schema contains attributes types and ordering
   * @param tuple_idx it indicates where to start decompress.csv dataset
   * @param decompress_num in indicates how many tuples to be decompress.csv
   * @param block_size once decompressed tuple number is larger than block size,
   * decoder need initialization.
   */
  RelationDecompressor(const char *compressed_file_name, Schema schema, int block_size);

  /**
   * Init a decompressor. including loading squid models from disks and finding
   * the position of decompression starts (random access).
   */
  void Init();

  /**
   * Random Access. Locate tuple position.
   *
   * @param tuple_idx index of accessed tuple
   */
  void LocateTuple(uint32_t tuple_idx);

  /**
   * Decompress next tuple, existence of next tuple should be checked before
   * calling this function.
   *
   * @param[out] tuple decompressed result
   */
  void ReadNextTuple(AttrVector *tuple);

  /**
   * Random Access.
   *
   * @param[out] tuple decompressed result
   */
  void ReadTargetTuple(size_t tuple_idx, AttrVector *tuple);

  /**
   * Check if next tuple is existed.
   *
   * @return return true if next tuple exists; or return false
   */
  bool HasNext() const { return num_converted_tuples_ < num_todo_tuples_; }

  /**
   * Get the index of last decompressed tuple in a dataset
   *
   * @return return the index of last tuple
   */
  int GetCurrentPosition() const { return tuple_idx_ + num_converted_tuples_ - 1; }

 private:
  Schema schema_;
  IndexReader index_reader_;

  int num_converted_tuples_, num_todo_tuples_;

  // where to start decompress.csv, and how many tuples needed
  uint32_t tuple_idx_;

  const int kBlockSizeThreshold;
  ByteReader byte_reader_;
  std::vector<std::unique_ptr<SquIDModel> > model_;
  std::vector<size_t> attr_order_;
  Decoder decoder_;

  int data_pos_;
  uint32_t num_bytes_;
};

}  // namespace db_compress

#endif
