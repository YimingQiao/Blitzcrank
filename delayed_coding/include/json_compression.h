/**
 * @file json_compression.h
 * @brief The header of compressor for JSON-style files.
 */

#ifndef DB_COMPRESS_JSON_COMPRESSION_H
#define DB_COMPRESS_JSON_COMPRESSION_H
#include <memory>
#include <vector>

#include "base.h"
#include "data_io.h"
#include "index.h"
#include "json_base.h"
#include "json_model.h"
#include "json_model_learner.h"
#include "model.h"
#include "simple_prob_interval_pool.h"

namespace db_compress {
/**
 * The compressor for JSON-style files.
 */
class JSONCompressor {
 public:
  /**
   * Create a new json compressor.
   *
   * @param output_file output_file address of compressed file, it is user
   * specified
   * @param json_schema leaf node path and types are recorded in schema
   * @param block_size once probability intervals number is larger than block
   * size, flush them
   * @param config learning config
   * @param root[out] compressed result can be transferred with root.
   */
  JSONCompressor(const char *output_file, const JSONSchema &json_schema, int block_size,
                 const CompressionConfig &config, JSONModel *root);

  /**
   * Once the structure of attributes are learned, or enter compression stage, a
   * full dataset scan is necessary.
   *
   * @return whether a full dataset scan is needed.
   */
  bool RequireFullPass() { return (compressor_stage_ > 0 || learner_->RequireFullPass()); };

  /**
   * If compressor needs a full scan of dataset when learning, return true. That
   * is, structure of basic network is learned, but a full scan is still needed
   * to estimate parameters.
   *
   * @return whether a full dataset scan is needed.
   */
  bool RequireMoreIterationsForLearning() const { return compressor_stage_ == 0; }

  /**
   * Learn a node.
   *
   * @param node the node to be learned.
   */
  void LearnNode(const rapidjson::Value &node) {
    num_nodes_++;
    learner_->FeedNode(node, attr_record_);

    if (num_nodes_ % 100000 == 0) {
      std::cout << "Node number " << num_nodes_ << "\n";
    }
  }

  /**
   * Compress a node.
   *
   * @param node the node to be compressed.
   */
  void CompressNode(const rapidjson::Value &node) {
    num_nodes_++;
    GetProbInterval(root_.get(), node, attr_record_, prob_intervals_, prob_intervals_index_);
    if (prob_intervals_index_ > prob_intervals_.size()) {
      throw BufferOverflowException(
          "JSONCompressor::ReadNode::Need larger buffer for probability intervals. Prob "
          "Intervals Index: " +
          std::to_string(prob_intervals_index_) +
          "\tBlock Size: " + std::to_string(kBlockSizeThreshold) + "\n");
    }
    if (prob_intervals_index_ > kBlockSizeThreshold / 10) WriteProbInterval();

    if (num_nodes_ % 100000 == 0) {
      std::cout << "Node number " << num_nodes_ << "\n";
    }
  }

  /**
   * End of learning.
   */
  void EndOfLearning();

  /**
   * End of compression.
   */
  void EndOfCompress();

  /**
   * Prepare for full pass.
   */
  void PrepareFullPass() { num_nodes_ = 0; }

 private:
  std::unique_ptr<JSONModel> root_;
  std::unique_ptr<JSONModelLearner> learner_;
  int num_nodes_;
  JSONSchema json_schema_;
  std::string output_file_;
  AttrVector attr_record_;

  // IO
  std::unique_ptr<SequenceByteWriter> byte_writer_;
  BitString bit_string_;

  int compressor_stage_;

  // delayed coding
  std::vector<Branch *> prob_intervals_;
  int prob_intervals_index_;
  std::vector<bool> is_virtual_;
  const int kBlockSizeThreshold;

  /**
   * Once probability intervals number is larger than block size, flush and
   * encode them.
   */
  void WriteProbInterval();
};
}  // namespace db_compress
#endif  // DB_COMPRESS_JSON_COMPRESSION_H
