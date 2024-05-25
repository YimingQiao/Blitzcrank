#include "../include/categorical_model.h"

#include <cmath>
#include <vector>

#include "../include/base.h"
#include "../include/model.h"
#include "../include/utility.h"

namespace db_compress {

void CategoricalSquID::Decompress(Decoder *decoder, ByteReader *byte_reader) {
  branch_left_ = 0;
  branch_right_ = static_cast<int>(prob_segments_->size()) - 1;
  if (decoder->GetPIb().exp_ < 40) decoder->FeedByte(byte_reader->ReadByte());
  prob_pit_pib_ratio_ = decoder->GetPIbPItRatio();

  while (branch_left_ != branch_right_) {
    branch_mid_ = (branch_left_ + branch_right_) / 2;
    prob_right_ = static_cast<int>((*prob_segments_)[branch_mid_ + 1]);
    if (prob_right_ > prob_pit_pib_ratio_) {
      branch_right_ = branch_mid_;
    } else {
      branch_left_ = branch_mid_ + 1;
    }
  }
  prob_interval_.left_prob_ = (*prob_segments_)[branch_left_];
  prob_interval_.right_prob_ = (*prob_segments_)[branch_left_ + 1];
  decoder->UpdatePIt(prob_interval_);
  choice_ = branch_left_;
}
void CategoricalSquID::GetProbIntervals(std::vector<ProbInterval> &prob_intervals,
                                        int &prob_intervals_index, const AttrValue &attr) {
  const int branch = attr.Int();
  prob_intervals[prob_intervals_index++] =
      ProbInterval((*prob_segments_)[branch], (*prob_segments_)[branch + 1]);
  choice_ = branch;
}

TableCategorical::TableCategorical(const std::vector<int> &attr_type,
                                   const std::vector<size_t> &predictor_list, size_t target_var,
                                   double err)
    : SquIDModel(predictor_list, target_var),
      predictor_interpreter_(predictor_list_size_),
      dynamic_list_(GetPredictorCap(predictor_list)),
      dynamic_list_index_(predictor_list.size()),
      target_range_(0),
      cell_size_(0),
      model_cost_(0) {
  for (size_t i = 0; i < predictor_list_size_; ++i) {
    predictor_interpreter_[i] = GetAttrInterpreter(attr_type[predictor_list[i]]);
  }
}

CategoricalSquID *TableCategorical::GetSquID(const AttrVector &tuple) {
  GetDynamicListIndex(tuple);
  std::vector<Prob> &prob_segs = dynamic_list_[dynamic_list_index_].prob_;
  squid_.Init(prob_segs);
  return &squid_;
}

void TableCategorical::GetDynamicListIndex(const AttrVector &tuple) {
  for (size_t i = 0; i < predictor_list_size_; ++i) {
    const AttrValue &attr = tuple.attr_[predictor_list_[i]];
    size_t val = predictor_interpreter_[i]->EnumInterpret(attr);
    dynamic_list_index_[i] = val;
  }
}
void TableCategorical::FeedTuple(const AttrVector &tuple) {
  GetDynamicListIndex(tuple);
  const AttrValue &attr = tuple.attr_[target_var_];
  size_t target_val = attr.Int();
  if (target_val >= target_range_) {
    target_range_ = target_val + 1;
  }

  CategoricalStats &vec = dynamic_list_[dynamic_list_index_];
  if (vec.count_.size() <= target_val) {
    vec.count_.resize(target_val + 1);
    vec.count_.shrink_to_fit();
  }
  ++vec.count_.at(target_val);
}
void TableCategorical::EndOfData() {
  // Determine cell size
  cell_size_ = (target_range_ > 100 ? 16 : 8);
  for (size_t i = 0; i < dynamic_list_.Size(); ++i) {
    CategoricalStats &stats = dynamic_list_[i];
    // Extract count vector
    std::vector<int> count;
    count.swap(stats.count_);

    // We might need to resize count vector
    count.resize(target_range_);
    std::vector<Prob> prob;

    // Quantization
    Quantization(&prob, count, cell_size_);

    // Update model cost
    for (size_t j = 0; j < count.size(); j++) {
      if (count[j] > 0) {
        Prob p = (j == count.size() - 1 ? GetOneProb() : prob[j]) -
                 (j == 0 ? GetZeroProb() : prob[j - 1]);
        model_cost_ += count[j] * (-log2(CastDouble(p)));
      }
    }
    // Write back to dynamic list
    stats.prob_.resize(prob.size() + 2);
    stats.prob_[0] = GetZeroProb();
    stats.prob_[prob.size() + 1] = GetOneProb();
    for (int k = 1; k < prob.size() + 1; ++k) {
      stats.prob_[k] = prob[k - 1];
    }
  }
  // Add model description length to model cost
  model_cost_ += GetModelDescriptionLength();
}
int TableCategorical::GetModelDescriptionLength() const {
  size_t table_size = dynamic_list_.Size();
  // See WriteModel function for details of model description.
  return table_size * (target_range_ - 1) * cell_size_ + predictor_list_size_ * 16 + 32;
}
void TableCategorical::WriteModel(SequenceByteWriter *byte_writer) const {
  // Write Model Description Prefix
  byte_writer->WriteByte(predictor_list_size_);
  byte_writer->WriteByte(cell_size_);
  for (size_t i = 0; i < predictor_list_size_; ++i) {
    byte_writer->Write16Bit(predictor_list_[i]);
  }
  byte_writer->Write16Bit(target_range_);

  // Write Model Parameters
  size_t table_size = dynamic_list_.Size();
  for (size_t i = 0; i < table_size; ++i) {
    std::vector<Prob> prob_segs = dynamic_list_[i].prob_;
    // The first and the last prob_ of prob_segs are not saved.
    for (size_t j = 1; j < prob_segs.size() - 1; ++j) {
      int code = CastInt(prob_segs[j], cell_size_);
      if (cell_size_ == 16) {
        byte_writer->Write16Bit(code);
      } else {
        byte_writer->WriteByte(code);
      }
    }
  }
}
SquIDModel *TableCategorical::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                        size_t index) {
  size_t predictor_size = byte_reader->ReadByte();
  size_t cell_size = byte_reader->ReadByte();
  std::vector<size_t> predictor_list(predictor_size);
  for (size_t i = 0; i < predictor_size; ++i) {
    size_t pred = byte_reader->Read16Bit();
    predictor_list[i] = pred;
  }
  // set err to 0 because err is only used in training
  TableCategorical *model = new TableCategorical(schema.attr_type_, predictor_list, index, 0);

  size_t target_range = byte_reader->Read16Bit();
  model->target_range_ = target_range;

  // Read Model Parameters
  size_t table_size = model->dynamic_list_.Size();
  for (size_t i = 0; i < table_size; ++i) {
    std::vector<Prob> &prob_segs = model->dynamic_list_[i].prob_;
    prob_segs.resize(target_range + 1);
    prob_segs[0] = GetZeroProb();
    prob_segs[target_range] = GetOneProb();
    for (size_t j = 1; j < target_range; ++j) {
      if (cell_size == 16) {
        prob_segs[j] = GetProb(byte_reader->Read16Bit(), 16);
      } else {
        prob_segs[j] = GetProb(byte_reader->ReadByte(), 8);
      }
    }
  }

  return model;
}
SquIDModel *TableCategoricalCreator::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                               size_t index) {
  return TableCategorical::ReadModel(byte_reader, schema, index);
}
SquIDModel *TableCategoricalCreator::CreateModel(const std::vector<int> &attr_type,
                                                 const std::vector<size_t> &predictors,
                                                 size_t index, double err) {
  size_t table_size = 1;
  for (uint64_t predictor_idx : predictors) {
    if (!GetAttrInterpreter(predictor_idx)->EnumInterpretable()) return nullptr;
    table_size *= GetAttrInterpreter(predictor_idx)->EnumCap();
  }
  if (table_size > kMaxTableSize) return nullptr;

  return new TableCategorical(attr_type, predictors, index, err);
}

}  // namespace db_compress
