// The compression process class header

#ifndef COMPRESSION_H
#define COMPRESSION_H

#include <memory>
#include <vector>

#include "base.h"
#include "data_io.h"
#include "index.h"
#include "model.h"
#include "model_learner.h"

namespace db_compress {

/**
 * The meaning of stages are as follows:
 *  0: Model Learning Phase (multiple rounds)
 *  1: Writing
 *  2: End of Compression
 */
class RelationCompressor {
 public:
  /**
   * Create a new Compressor.
   *
   * @param output_file address of compressed file, it is user specified
   * @param schema attributes types and ordering are recorded in schema
   * @param config learning config
   * @param block_size once probability intervals number is larger than block
   * size, flush them
   */
  RelationCompressor(const char *outputFile, const Schema &schema, const CompressionConfig &config,
                     int block_size);

  /**
   * Once the structure of attributes are learned, or enter compression stage, a
   * full dataset scan is necessary.
   *
   * @return whether a full dataset scan is needed.
   */
  bool RequireFullPass() const { return (compressor_stage_ > 0 || learner_->RequireFullPass()); }

  /**
   * If compressor needs a full scan of dataset when learning, return true. That
   * is, structure of basic network is learned, but a full scan is still needed
   * to estimate parameters.
   *
   * @return whether a full dataset scan is needed.
   */
  bool RequireMoreIterationsForLearning() const { return compressor_stage_ != 1; }

  /**
   * This function is for structure learning. In the learning stage, we need
   * randomly select some tuples for estimating distribution of data.
   *
   * @param tuple basic unit of structural dataset, i.e. tuple.
   */
  void LearnTuple(const AttrVector &tuple) {
    learner_->FeedTuple(tuple);
    num_tuples_++;
  }

  /**
   * This function is for compressing tuple.
   *
   * @param tuple basic unit of structure dataset
   */
  void CompressTuple(AttrVector &tuple);

  /**
   * Learning stage ends, write down models.
   */
  void EndOfLearning();

  /**
   * Compression ends, write down left probability intervals.
   */
  void EndOfCompress();

 private:
  Schema schema_;
  std::unique_ptr<ModelLearner> learner_;
  size_t num_tuples_;
  IndexCreator index_creator_;
  std::string output_file_;
  const int kBlockSizeThreshold_;
  int compressor_stage_;

  // IO
  std::unique_ptr<SequenceByteWriter> byte_writer_;
  BitString bit_string_;

  // arithmetic coding
  std::vector<ProbInterval> prob_intervals_;
  int prob_intervals_index_;
  int block_size_;
  int block_bitstring_length_;
  std::vector<unsigned char> emit_byte_;
  BitString cat_;

  // model leaning
  std::vector<std::unique_ptr<SquIDModel> > model_;
  std::vector<size_t> attr_order_;

  /**
   * Once there are enough probability intervals, encode them with arithmetic
   * coding
   */
  void CheckBlockInfo();
};
}  // namespace db_compress

#endif
