#include "../include/categorical_model.h"

#include <cmath>
#include <vector>

#include "../include/base.h"
#include "../include/model.h"
#include "../include/utility.h"

namespace db_compress {
void CategoricalSquID::Decompress(Decoder *decoder, ByteReader *byte_reader) {
  unsigned two_bytes = decoder->Read16Bits(byte_reader);
  // P
  unsigned high_bits = two_bytes >> (16 - coding_params_->num_represent_bits_);
  // Q
  unsigned low_bits = two_bytes & ((1 << (16 - coding_params_->num_represent_bits_)) - 1);
  bool flag = (low_bits < (coding_params_->segment_left_branches_)[high_bits].first);
  // character
  choice_ = flag ? (coding_params_->segment_left_branches_)[high_bits].second
                 : (coding_params_->segment_right_branches_)[high_bits].second;
  // k
  unsigned denominator = coding_params_->branches_[choice_].total_weights_;
  unsigned index = (high_bits << 1) + static_cast<unsigned>(!flag);
  unsigned numerator = two_bytes - coding_params_->numerator_helper_[index];
  decoder->Update(denominator, numerator);

  // if we come across a rare branch
  if (choice_ == coding_params_->branches_.size() - 1) {
    two_bytes = decoder->Read16Bits(byte_reader);
    int branch_idx = two_bytes / rare_branch_handler_->weight_;
    choice_ = rare_branch_handler_->idx2branch_[branch_idx];
    denominator = rare_branch_handler_->weight_;
    numerator = two_bytes - (branch_idx * rare_branch_handler_->weight_);
    decoder->Update(denominator, numerator);
  }
}
void CategoricalSquID::GetProbIntervals(std::vector<Branch *> &prob_intervals,
                                        int &prob_intervals_index, const AttrValue &value) const {
  //  if (prob_intervals_index + 2 >= prob_intervals.size()) {
  //    std::cout << "prob_intervals is too small" << std::endl;
  //    prob_intervals.resize((prob_intervals_index | 1) << 1);
  //  }

  if (coding_params_->branches_[value.Int()].total_weights_ != 0) {
    prob_intervals[prob_intervals_index++] = &coding_params_->branches_[value.Int()];
  } else {
    int branch_idx = static_cast<int>(coding_params_->branches_.size() - 1);
    prob_intervals[prob_intervals_index++] = &coding_params_->branches_[branch_idx];
    prob_intervals[prob_intervals_index++] = GetSimpleBranch(
        rare_branch_handler_->weight_, rare_branch_handler_->branch2idx_[value.Int()]);
  }
}

TableCategorical::TableCategorical(const std::vector<int> &attr_type,
                                   const std::vector<size_t> &predictor_list, size_t target_var)
    : SquIDModel(predictor_list, target_var),
      predictor_interpreter_(predictor_list.size()),
      dynamic_list_(GetPredictorCap(predictor_list)),
      dynamic_list_index_(predictor_list.size()),
      target_range_(0),
      model_cost_(0) {
  for (size_t i = 0; i < predictor_list_size_; ++i)
    predictor_interpreter_[i] = GetAttrInterpreter(predictor_list[i]);
}

CategoricalSquID *TableCategorical::GetSquID(const AttrVector &tuple) {
  if (dynamic_list_index_.empty()) return &base_squid_;

  GetDynamicListIndex(tuple);
  CategoricalStats &stats = dynamic_list_[dynamic_list_index_];
  squid_.Init(stats);
  return &squid_;
}

void TableCategorical::GetDynamicListIndex(const AttrVector &tuple) {
  for (size_t i = 0; i < predictor_list_size_; ++i) {
    const AttrValue &attr = tuple.attr_[predictor_list_[i]];
    size_t val = predictor_interpreter_[i]->EnumInterpret(attr);
    dynamic_list_index_[i] = val;
  }
}

void TableCategorical::FeedAttrs(const AttrVector &attrs, int count) {
  const AttrValue &attr = attrs.attr_[target_var_];
  size_t target_val = attr.Int();

  if (target_val >= target_range_) {
    target_range_ = target_val + 1;
  }

  GetDynamicListIndex(attrs);
  CategoricalStats &vec = dynamic_list_[dynamic_list_index_];
  if (vec.count_.size() <= target_val) {
    vec.count_.resize(target_val + 1);
    vec.count_.shrink_to_fit();
  }
  vec.count_.at(target_val) += count;
}

void TableCategorical::FeedAttrs(const AttrValue &attr_val, int count) {
  int value = attr_val.Int();
  if (value >= target_range_) target_range_ = value + 1;

  CategoricalStats &vec = dynamic_list_[0];
  if (vec.count_.size() <= value) {
    vec.count_.resize(value + 1);
    vec.count_.shrink_to_fit();
  }
  vec.count_.at(value) += count;
}

void TableCategorical::EndOfData() {
  // Determine cell size
  for (int i = 0; i < static_cast<int>(dynamic_list_.Size()); ++i) {
    CategoricalStats &stats = dynamic_list_[i];
    // Extract counts vector
    std::vector<int> counts;
    counts.swap(stats.count_);

    // We might need to resize counts
    counts.resize(target_range_);

    // We add extra branch to represent the second level
    stats.weight_.resize(counts.size() + 1);
    int sum_count = 0;
    int index_max_weight = 0;
    unsigned left_weight = (1 << 16);
    bool zero_weight_exist = false;
    for (int count : counts) sum_count += count;

    if (sum_count == 0) {
      // If there is no branch, zero weight does not exist.
      if (!counts.empty()) zero_weight_exist = true;
    } else {
      for (int j = 0; j < counts.size(); ++j) {
        stats.weight_[j] = static_cast<uint64_t>(counts[j]) * (1 << 16) / sum_count;
        left_weight -= stats.weight_[j];

        // check zero weight existence
        if (!zero_weight_exist && stats.weight_[j] == 0) zero_weight_exist = true;

        // find branch with the largest weight
        if (stats.weight_[index_max_weight] < stats.weight_[j]) index_max_weight = j;
      }
    }

    // allocate left weight
    if (zero_weight_exist) {
      // if there is no left weight, we borrow ''one'' from the branch which has at least one
      // unit weight.
      if (left_weight == 0) {
        left_weight = 1;
        stats.weight_[index_max_weight] -= 1;
      }
      // allocate weight for the extra branch
      stats.weight_[stats.weight_.size() - 1] = left_weight;
      stats.rare_branch_handler_.Init(stats.weight_);
    } else
      stats.weight_[index_max_weight] += left_weight;

    InitDelayedCodingParams(stats.weight_, stats.coding_params_);

    // update model cost
    for (size_t j = 0; j < counts.size(); j++) {
      if (stats.weight_[j] > 0) {
        double res_prob = stats.weight_[j] / 65536.0;
        model_cost_ += counts[j] * (-log2(res_prob));
      }
    }
  }
  base_squid_.Init(dynamic_list_[0]);
  // Add model description length to model cost
  model_cost_ += GetModelDescriptionLength();
}

int TableCategorical::GetModelDescriptionLength() const {
  size_t table_size = dynamic_list_.Size();
  // See WriteModel function for details of model description.
  return static_cast<int>(table_size * (target_range_ - 1) * 16 + predictor_list_size_ * 16 + 32);
}

void TableCategorical::WriteModel(SequenceByteWriter *byte_writer) {
  // Write Model Description Prefix
  byte_writer->WriteByte(predictor_list_size_);
  for (size_t i = 0; i < predictor_list_size_; ++i) byte_writer->Write16Bit(predictor_list_[i]);
  byte_writer->Write16Bit(target_range_);

  // Write Model Parameters
  size_t table_size = dynamic_list_.Size();
  for (int i = 0; i < static_cast<int>(table_size); ++i) {
    std::vector<unsigned> weights = dynamic_list_[i].weight_;
    for (size_t j = 0; j < weights.size(); ++j) {
      // 65536 cannot be represented by 16 bits. Once there is a 65536 weight branch, then all left
      // weights are zero. So, we use 65535 to represent 65536.
      if (weights[j] == 65536)
        byte_writer->Write16Bit(65535);
      else
        byte_writer->Write16Bit(weights[j]);
    }
  }
}

TableCategorical *TableCategorical::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                              size_t index) {
  size_t predictor_size = byte_reader->ReadByte();
  std::vector<size_t> predictor_list(predictor_size);
  for (size_t i = 0; i < predictor_size; ++i) {
    size_t pred = byte_reader->Read16Bit();
    predictor_list[i] = pred;
  }

  TableCategorical *model = new TableCategorical(schema.attr_type_, predictor_list, index);

  size_t target_range = byte_reader->Read16Bit();
  model->target_range_ = target_range;

  // Read Model Parameters
  size_t table_size = model->dynamic_list_.Size();
  for (int i = 0; i < static_cast<int>(table_size); ++i) {
    CategoricalStats &stats = model->dynamic_list_[i];
    stats.weight_.resize(target_range + 1);

    // Read weights
    size_t only_value = -1;
    unsigned sum_weights = 0;
    for (size_t j = 0; j < stats.weight_.size(); ++j) {
      stats.weight_[j] = byte_reader->Read16Bit();
      sum_weights += stats.weight_[j];

      // label branch of weight 65535, there could be only one 65535 weight branch.
      if (stats.weight_[j] == 65535) only_value = j;
    }
    // If sum of weights is not 65536, it means ''only value'' exists.
    if (sum_weights != 65536) {
      stats.weight_[only_value] = 65536;
      stats.only_value_ = only_value;
    }

    // init rare branch handler
    if (stats.weight_[stats.weight_.size() - 1] != 0 &&
        stats.weight_[stats.weight_.size() - 1] != 65536)
      stats.rare_branch_handler_.Init(stats.weight_);

    // Preparation for delayed coding
    InitDelayedCodingParams(stats.weight_, stats.coding_params_);
  }

  model->base_squid_.Init(model->dynamic_list_[0]);

  return model;
}

SquIDModel *TableCategorical::ReadModel(ByteReader *byte_reader) {
  return TableCategorical::ReadModel(byte_reader, Schema(), 0);
}

SquIDModel *TableCategoricalCreator::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                               size_t index) {
  return TableCategorical::ReadModel(byte_reader, schema, index);
}

SquIDModel *TableCategoricalCreator::CreateModel(const std::vector<int> &attr_type,
                                                 const std::vector<size_t> &predictor, size_t index,
                                                 double err) {
  // filter illegal model, i.e. all predictors should have a non-zero capacity.
  size_t table_size = 1;
  for (int attr : predictor) {
    if (!GetAttrInterpreter(attr)->EnumInterpretable()) return nullptr;
    table_size *= GetAttrInterpreter(attr)->EnumCap();
  }
  if (table_size > kMaxTableSize) return nullptr;

  return new TableCategorical(attr_type, predictor, index);
}

void ZeroBranchHandler::Init(std::vector<unsigned int> weights) {
  for (unsigned weight : weights)
    if (weight == 0) map_size_++;

  weight_ = 65536 / map_size_;
  idx2branch_.resize(map_size_);

  for (int i = 0; i < weights.size(); ++i) {
    if (weights[i] == 0) {
      int idx = branch2idx_.size();
      idx2branch_[idx] = i;
      branch2idx_[i] = idx;
    }
  }
}
}  // namespace db_compress
