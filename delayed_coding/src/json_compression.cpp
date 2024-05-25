//
// Created by Qiao Yiming on 2022/2/16.
//

#include "../include/json_compression.h"

#include <memory>

#include "../include/blitzcrank_exception.h"
#include "../include/json_base.h"
#include "../include/json_model.h"
#include "../include/json_model_learner.h"
#include "../include/utility.h"

namespace db_compress {
JSONCompressor::JSONCompressor(const char *output_file, const JSONSchema &json_schema,
                               int block_size, const CompressionConfig &config, JSONModel *root)
    : root_(root),
      output_file_(output_file),
      kBlockSizeThreshold(block_size),
      learner_(std::make_unique<JSONModelLearner>(json_schema, config, root_.get())),
      bit_string_(block_size << 1),
      num_nodes_(0),
      json_schema_(json_schema),
      attr_record_(static_cast<int>(json_schema.path_order_.size())),
      compressor_stage_(0),
      prob_intervals_index_(0) {
  // Once prob_interval_index > kBlockSizeThreshold_, it will be flushed. Then, it is safe to resize
  // prob_intervals_ as kBlockSizeThreshold_ * 2. Note that one probability intervals equals to two
  // bytes at most.
  prob_intervals_.resize(block_size << 1);
  is_virtual_.resize(block_size << 1);
}

void JSONCompressor::WriteProbInterval() {
  DelayedCoding(prob_intervals_, prob_intervals_index_, &bit_string_, is_virtual_);
  bit_string_.Finish(byte_writer_.get());
  prob_intervals_index_ = 0;
}
void JSONCompressor::EndOfLearning() {
  learner_->EndOfData();
  if (!learner_->RequireMoreIterations()) {
    compressor_stage_ = 1;
    learner_ = nullptr;

    // Initialize compressed file
    byte_writer_ = std::make_unique<SequenceByteWriter>(output_file_);

    // Write models
    byte_writer_->Write32Bit(num_nodes_);

    byte_writer_->ClearNumBits();
    root_->WriteModel(byte_writer_.get());
    uint64_t num_bits = byte_writer_->GetNumBits();
    // stats
    std::cout << "JSON Model Size: " << (num_bits >> 13) << " KB. \n\n";

    num_nodes_ = 0;
  } else {
    // Reset the number of tuples, compute it again in the new
    // round.
    num_nodes_ = 0;
  }
}
void JSONCompressor::EndOfCompress() {
  compressor_stage_ = 2;
  // write down the last bitString even in the case bit_string_ is
  // empty
  WriteProbInterval();
  root_ = nullptr;
  byte_writer_ = nullptr;
}
}  // namespace db_compress