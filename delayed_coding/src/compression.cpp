#include "compression.h"

#include <memory>

#include "base.h"
#include "blitzcrank_exception.h"
#include "categorical_model.h"
#include "markov_model.h"
#include "model.h"
#include "model_learner.h"
#include "numerical_model.h"
#include "string_model.h"
#include "timeseries_model.h"
#include "utility.h"

namespace db_compress {
void RelationCompressor::WriteProbInterval() {
  DelayedCoding(prob_intervals_, prob_intervals_index_, &bit_string_, is_virtual_);
  bit_string_.Finish(byte_writer_.get());
  index_creator_.WriteBlockInfo(bit_string_.num_, num_tuples_);
  prob_intervals_index_ = 0;
}

RelationCompressor::RelationCompressor(const char *output_file, const Schema &schema,
                                       const CompressionConfig &config, const int block_size)
    : output_file_(output_file),
      schema_(schema),
      kBlockSizeThreshold_(block_size),
      learner_(new RelationModelLearner(schema, config)),
      bit_string_((block_size << 8) + kIntervalSize),
      num_tuples_(0),
      compressor_stage_(0),
      prob_intervals_index_(0) {
  prob_intervals_.resize((block_size << 8) + kIntervalSize);
  is_virtual_.resize((block_size << 8) + kIntervalSize);
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
    byte_writer_->ClearNumBits();
    // Write Models
    // Randomly sampled tuples should not be counted.
    byte_writer_->Write32Bit(num_tuples_ - kNumEstSample);
    for (uint64_t attr : attr_order_) byte_writer_->Write16Bit(attr);

    for (size_t i = 0; i < schema_.attr_type_.size(); ++i)
      model_[i]->WriteModel(byte_writer_.get());
    uint64_t num_bits = byte_writer_->GetNumBits();
    // stats
    std::cout << "Model Size: " << (num_bits / double(1 << 13)) << " KB. \n";
  }
  // Reset the number of tuples, compute it again in the new
  // round.
  num_tuples_ = 0;

  // reset markov
  for (size_t attr_index : attr_order_) {
    if (schema_.attr_type_[attr_index] == 5) {
      auto *model = static_cast<TableMarkov *>(model_[attr_index].get());
      model->SetState(0);
    }
  }
}

void RelationCompressor::EndOfCompress() {
  compressor_stage_ = 2;
  // write down the last bitString even though bit_string_ is empty
  WriteProbInterval();
  byte_writer_ = nullptr;
  index_creator_.End();
}

void RelationCompressor::CompressTuple(AttrVector &tuple) {
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
      case 5: {
        auto *model = static_cast<TableMarkov *>(model_[attr_index].get());
        CategoricalSquID *squid = model->GetSquID(tuple);
        squid->GetProbIntervals(prob_intervals_, prob_intervals_index_,
                                tuple.attr_[model->GetTargetVar()]);
        model->SetState(tuple.attr_[model->GetTargetVar()].Int());
        break;
      }
      default:
        std::cerr << "Unsupported Data Attribute.\n";
    }
  }
  if (prob_intervals_index_ > prob_intervals_.size()) {
    throw BufferOverflowException(
        "Compressor::ReadNode::Need larger buffer or smaller block for "
        "probability intervals. Prob Intervals Index: " +
        std::to_string(prob_intervals_index_) +
        "\tkBlockSizeThreshold: " + std::to_string(prob_intervals_.size()) + "\n");
  }

  num_tuples_++;

  // if there are enough probability intervals, write them down.
  if (prob_intervals_index_ > kBlockSizeThreshold_) {
    WriteProbInterval();

    // reset markov
    //    for (size_t attr_index : attr_order_) {
    //      if (schema_.attr_type_[attr_index] == 5) {
    //        auto *model = static_cast<TableMarkov *>(model_[attr_index].get());
    //        model->SetState(0);
    //      }
    //    }
  }
}
}  // namespace db_compress
