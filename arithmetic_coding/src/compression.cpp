#include "../include/compression.h"

#include <memory>

#include "../include/base.h"
#include "../include/categorical_model.h"
#include "../include/model.h"
#include "../include/model_learner.h"
#include "../include/numerical_model.h"
#include "../include/squish_exception.h"
#include "../include/string_model.h"
#include "../include/utility.h"

namespace db_compress {
/**
 * Write bit_string_ with byte_writer.
 */
void WriteBitString(SequenceByteWriter *byte_writer, const BitString &bit_string) {
  int written_length = 0;
  while (written_length < bit_string.length_) {
    if (bit_string.length_ - written_length >= 8) {
      unsigned char byte = GetByte(bit_string.bits_[written_length >> 5], written_length & 31);
      byte_writer->WriteByte(byte);
      written_length += 8;
    } else {
      const int left_length = bit_string.length_ - written_length;
      const unsigned char byte =
          GetByte(bit_string.bits_[written_length >> 5], written_length & 31);
      byte_writer->WriteLess(byte >> (8 - left_length), left_length);
      break;
    }
  }
}

RelationCompressor::RelationCompressor(const char *outputFile, const Schema &schema,
                                       const CompressionConfig &config, int block_size)
    : output_file_(outputFile),
      kBlockSizeThreshold_(block_size),
      schema_(schema),
      learner_(new ModelLearner(schema, config)),
      num_tuples_(0),
      compressor_stage_(0),
      prob_intervals_index_(0),
      block_size_(0),
      block_bitstring_length_(0) {
  prob_intervals_.resize(block_size << 1);
  emit_byte_.resize(block_size << 1);
}

void RelationCompressor::CheckBlockInfo() {
  block_size_++;
  block_bitstring_length_ += bit_string_.length_;
  if (block_size_ == kBlockSizeThreshold_) {
    index_creator_.WriteBlockInfo(block_bitstring_length_, num_tuples_);
    block_bitstring_length_ = 0;
    block_size_ = 0;
  }
}

void RelationCompressor::CompressTuple(AttrVector &tuple) {
  num_tuples_++;

  bit_string_.Clear();
  prob_intervals_index_ = 0;
  for (size_t attr_index : attr_order_) {
    switch (schema_.attr_type_[attr_index]) {
      case 0: {
        // attribute types are recorded in schema, thus dynamic_cast is not
        // needed, and we are sure the static_cast result is correct.
        auto *model = static_cast<TableCategorical *>(model_[attr_index].get());
        CategoricalSquID *squid = model->GetSquID(tuple);
        squid->GetProbIntervals(prob_intervals_, prob_intervals_index_,
                                tuple.attr_[model->GetTargetVar()]);
        break;
      }
      case 1: {
        auto *model = static_cast<TableNumerical *>(model_[attr_index].get());
        NumericalSquID *squid = model->GetSquID(tuple);
        squid->GetProbIntervals(prob_intervals_, prob_intervals_index_,
                                tuple.attr_[model->GetTargetVar()]);
        break;
      }
      case 2: {
        auto *model = static_cast<TableNumerical *>(model_[attr_index].get());
        NumericalSquID *squid = model->GetSquID(tuple);
        squid->GetProbIntervals(prob_intervals_, prob_intervals_index_,
                                tuple.attr_[model->GetTargetVar()]);
        break;
      }
      case 3: {
        auto *model = static_cast<StringModel *>(model_[attr_index].get());
        StringSquID *squid = model->GetSquID(tuple);
        squid->GetProbIntervals(prob_intervals_, prob_intervals_index_,
                                tuple.attr_[model->GetTargetVar()]);
        break;
      }
    }
  }

  if (prob_intervals_index_ != 0) {
    int emit_byte_index = 0;
    const ProbInterval prob =
        ReducePIProduct(prob_intervals_, &emit_byte_, emit_byte_index, prob_intervals_index_);

    if (prob_intervals_index_ > prob_intervals_.size()) {
      throw BufferOverflowException(
          "Compressor::ReadNode::Need larger buffer for probability intervals. Buffer size should "
          "be larger than: " +
          std::to_string(prob_intervals_index_) + "\n");
    }

    // concatenate emit_byte_ to bit_string_
    for (int i = 0; i < emit_byte_index; ++i) {
      bit_string_.StrCat(emit_byte_[i]);
    }
    GetBitStringFromProbInterval(&cat_, prob);
    bit_string_.StrCat(cat_);
  }

  CheckBlockInfo();
  WriteBitString(byte_writer_.get(), bit_string_);
}
void RelationCompressor::EndOfLearning() {
  learner_->EndOfData();
  if (!learner_->RequireMoreIterations()) {
    compressor_stage_ = 1;
    model_.resize(schema_.attr_type_.size());
    for (size_t i = 0; i < schema_.attr_type_.size(); i++) {
      std::unique_ptr<SquIDModel> ptr(learner_->GetModel(i));
      model_[i] = std::move(ptr);
    }
    attr_order_ = learner_->GetOrderOfAttributes();
    learner_ = nullptr;

    // Initialize Compressed File
    byte_writer_ = std::make_unique<SequenceByteWriter>(output_file_);
    // Write Models
    // Randomly sampled tuples should not be counted.
    byte_writer_->WriteUnsigned(num_tuples_ - kNumEstSample);
    for (uint64_t attr_idx : attr_order_) byte_writer_->Write16Bit(attr_idx);

    for (size_t i = 0; i < schema_.attr_type_.size(); ++i) {
      model_[i]->WriteModel(byte_writer_.get());
    }
  }
  // Reset the number of tuples, compute it again in the new
  // round.
  num_tuples_ = 0;
}
void RelationCompressor::EndOfCompress() {
  compressor_stage_ = 2;
  // Byte writer must be deleted before calling index creator to write block
  // info and end. Otherwise, the index creator cannot write block info into
  // compressed file.
  byte_writer_ = nullptr;
  index_creator_.WriteBlockInfo(block_bitstring_length_, num_tuples_);
  index_creator_.End(output_file_);
}

}  // namespace db_compress
