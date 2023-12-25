#include "../include/numerical_model.h"

#include <algorithm>
#include <cmath>
#include <cstddef>

#include "../include/base.h"
#include "../include/model.h"
#include "../include/utility.h"

namespace db_compress {

namespace {
const double kEulerConstant = std::exp(1.0);
}  // anonymous namespace

void NumericalStats::InitHistogramStructure() {
  sort(values_.begin(), values_.begin() + v_count_);
  double max_v = values_[static_cast<int>(v_count_ * 0.95)];
  double min_v = values_[static_cast<int>(v_count_ * 0.05)];

  // Distribution Params: mean
  mid_est_ = (min_v + max_v) / 2;
  if (bin_size_ == 1) mid_est_ = static_cast<int>(mid_est_);
  QuantizationToFloat32Bit(&mid_est_);

  // Distribution Params: size of branch (#bins in a branch)
  double total_range = (max_v - min_v) * 1.5;
  branch_bins_est_ = static_cast<uint64_t>(total_range / bin_size_) / (kNumBranch - 2);
  if (branch_bins_est_ == 0)
    branch_bins_est_ = 1;
  else {
    branch_bins_est_ = 1 << p2ge(branch_bins_est_);
  }

  // Helper: minimum and maximum
  int32_t half_num_branch = ((kNumBranch - 2) >> 1);
  minimum_ = branch_bins_est_ * (-half_num_branch - 1);
  maximum_ = branch_bins_est_ * half_num_branch;

  is_estimated_ = true;
}

void NumericalStats::PushValue(double value) {
  if (!is_estimated_) {
    values_[v_count_] = value;
    if (++v_count_ >= kNumEstSample) InitHistogramStructure();
  } else {
    ++v_count_;
    int64_t idx = floor((value - mid_est_) / bin_size_);
    uint16_t interval;
    if (idx <= minimum_ + branch_bins_est_)
      interval = 0;
    else if (idx >= maximum_)
      interval = kNumBranch - 1;
    else {
      assert(idx / branch_bins_est_ + ((kNumBranch - 2) >> 1) + 1 >= 0 &&
             idx / branch_bins_est_ + ((kNumBranch - 2) >> 1) + 1 < kNumBranch);
      interval = idx / branch_bins_est_ + ((kNumBranch - 2) >> 1) + 1;
    }
    ++v_freq_[interval];
    sum_abs_dev_ += fabs(value - mid_est_);
  }
}

void NumericalStats::End() {
  // no data, no model.
  if (v_count_ == 0) return;

  if (!is_estimated_) InitHistogramStructure();

  // Distribution Params: abs_dev
  if (sum_abs_dev_ < bin_size_)
    mean_abs_dev_ = 0;
  else
    mean_abs_dev_ = sum_abs_dev_ / v_count_;
  QuantizationToFloat32Bit(&mean_abs_dev_);

  // Distribution Params: Branch weights
  //
  // Calculate weight of each branch, make sure: 1) total of weights is 65536; 2) any possible
  // branch has a positive weight.
  uint32_t total = 0;
  branch_weights_.resize(kNumBranch);
  for (uint32_t num_value : v_freq_) total += num_value;

  // 1. ensure the second condition
  for (size_t i = 0; i < branch_weights_.size(); ++i) {
    // variable total cannot be zero, since size of histogram is
    // kNumBranch1stLayer and each branch weight is larger than zero.
    branch_weights_[i] = v_freq_[i] * 65536 / total;
    // maintain the second condition, branch is possible but has zero weight.
    if (v_freq_[i] > 0 && branch_weights_[i] == 0) branch_weights_[i] = 1;
  }
  // 2. maintain the first condition
  uint32_t posterior_total = 0;
  uint32_t index_max = 0;
  for (size_t i = 0; i < branch_weights_.size(); ++i) {
    posterior_total += branch_weights_[i];
    if (branch_weights_[i] > branch_weights_[index_max]) index_max = i;
  }
  branch_weights_[index_max] += (65536 - posterior_total);

  // Delayed Coding Params
  InitDelayedCodingParams(branch_weights_, coding_params_);
  Prepare();
}

void NumericalStats::WriteStats(SequenceByteWriter *byte_writer) const {
  unsigned char bytes[4];
  ConvertSinglePrecision(mid_est_, bytes);
  byte_writer->Write32Bit(bytes);
  ConvertSinglePrecision(mean_abs_dev_, bytes);
  byte_writer->Write32Bit(bytes);
  byte_writer->WriteUint64(branch_bins_est_);
  for (int i = 0; i < kNumBranch; ++i) byte_writer->Write32Bit(branch_weights_[i]);
}

void NumericalStats::ReadStats(ByteReader *byte_reader) {
  unsigned char bytes[4];
  byte_reader->Read32Bit(bytes);
  mid_est_ = ConvertSinglePrecision(bytes);
  byte_reader->Read32Bit(bytes);
  mean_abs_dev_ = ConvertSinglePrecision(bytes);
  branch_bins_est_ = byte_reader->ReadUint64();

  branch_weights_.resize(kNumBranch);
  for (int i = 0; i < kNumBranch; ++i) branch_weights_[i] = byte_reader->ReadUint32();

  InitDelayedCodingParams(branch_weights_, coding_params_);
  Prepare();
}

void NumericalStats::Prepare() {
  step_ = ceil(mean_abs_dev_ / bin_size_);
  num_layer_ = 0;
  int64_t branch_bins = branch_bins_est_;
  while (branch_bins > 65536) {
    num_layer_++;
    branch_bins >>= 16;
  }
  mask_last_layer_ = p2ge(branch_bins);
  weight_branch_last_layer_ = 65536 / branch_bins;

  int half_num_branch = ((kNumBranch - 2) >> 1);
  minimum_ = branch_bins_est_ * (-half_num_branch - 1);
  maximum_ = branch_bins_est_ * half_num_branch;

  assert(weight_branch_last_layer_ != 0);
}

NumericalSquID::NumericalSquID(double bin_size, bool target_int)
    : target_int_(target_int), bin_size_(bin_size) {
  // It is very important.
  QuantizationToFloat32Bit(&bin_size_);

  if (target_int) {
    if (bin_size_ < 1) std::cout << "Bin size of double cannot be zero or larger than 1.\n";
    decimal_places_ = 0;
    while (bin_size / 10 >= 1) {
      bin_size /= 10;
      decimal_places_--;
    }
  } else {
    if (bin_size_ >= 1 || bin_size_ <= 0)
      std::cout << "Bin size of double cannot be zero or larger than 1.\n";
    decimal_places_ = 0;
    while (bin_size * 10 < 1) {
      bin_size *= 10;
      decimal_places_++;
    }
  }
}

void NumericalSquID::Init(NumericalStats &stats) {
  // Distribution Params
  mean_ = stats.mid_est_;
  dev_ = stats.mean_abs_dev_;
  coding_params_ = &stats.coding_params_;
  branch_bins_ = stats.branch_bins_est_;

  // Helper
  step_ = stats.step_;
  num_layer_ = stats.num_layer_;
  mask_last_layer_ = stats.mask_last_layer_;
  weight_branch_last_layer_ = stats.weight_branch_last_layer_;
  minimum_ = stats.minimum_;
  maximum_ = stats.maximum_;

  l_ = r_ = 0;
  l_inf_ = r_inf_ = true;
}

void NumericalSquID::GetProbIntervals(std::vector<Branch *> &prob_intervals,
                                      int &prob_intervals_index, const AttrValue &attr_value) {
  double value;
  if (target_int_)
    value = attr_value.Int();
  else
    value = attr_value.Double();

  int64_t idx = GetBinIndex(value);
  // 2022.11.23
  if (idx > minimum_ + branch_bins_ && idx < maximum_) {
    // histogram part
    GetHistogramProbIntervals(prob_intervals, prob_intervals_index, idx);
  } else {
    // most left or right part
    GetExpProbIntervals(prob_intervals, prob_intervals_index, idx);
  }

  Reset();
}

void NumericalSquID::GetExpProbIntervals(std::vector<Branch *> &prob_intervals,
                                         int &prob_intervals_index, int64_t idx) {
  int64_t branch;
  if (idx <= minimum_ + branch_bins_)
    branch = 0;
  else
    branch = kNumBranch - 1;

  prob_intervals[prob_intervals_index++] = &coding_params_->branches_[branch];

  if (branch == 0)
    // value lie in the most left branch
    SetRight(minimum_ + branch_bins_);
  else
    // value lie in the most right branch, maximum value in
    // histogram range is the negative of minimum value
    SetLeft(maximum_);

  while (HasNextBranch()) {
    // get branch
    int64_t mid;
    if (!l_inf_ && !r_inf_)
      mid = (r_ - l_ + 1) / 2;
    else
      mid = step_;

    // Reversed
    if (l_inf_)
      mid_ = r_ - mid;
    else
      mid_ = l_ + mid - 1;

    // (value - mean_) / bin_size_ + 0.5 > mid_
    branch = idx > mid_;
    // branch = static_cast<uint64_t>(DoubleGreaterEqualThan(idx, mean_ + (mid_ + 0.5) *
    // bin_size_));
    prob_intervals[prob_intervals_index++] = GetSimpleBranch(32768, branch);

    // choose branch
    if (branch == 1)
      SetLeft(mid_ + 1);
    else
      SetRight(mid_);
  }
}

void NumericalSquID::GetHistogramProbIntervals(std::vector<Branch *> &prob_intervals,
                                               int &prob_intervals_index, int64_t idx) {
  int64_t branch = idx / branch_bins_;
  int32_t low_bits = idx - branch * branch_bins_;
  if (low_bits < 0) {
    low_bits += branch_bins_;
    branch -= 1;
  }

  assert(branch + ((kNumBranch - 2) >> 1) + 1 > 0 &&
         branch + ((kNumBranch - 2) >> 1) + 1 < kNumBranch);
  prob_intervals[prob_intervals_index++] =
      &coding_params_->branches_[branch + ((kNumBranch - 2) >> 1) + 1];

  SetLeft(branch * branch_bins_);
  SetRight((branch + 1) * branch_bins_);

  // remaining layers
  if (num_layer_ != 0) {
    for (int i = num_layer_; i > 0; --i) {
      unsigned mask_shift = mask_last_layer_ + (i - 1) * 16;
      unsigned layer_branch = (low_bits >> mask_shift) & 0xffff;

      prob_intervals[prob_intervals_index++] = GetSimpleBranch(1, layer_branch);
    }
  }

  // last layers
  low_bits = low_bits & ((1 << mask_last_layer_) - 1);
  prob_intervals[prob_intervals_index++] = GetSimpleBranch(weight_branch_last_layer_, low_bits);
}

void NumericalSquID::Decompress(Decoder *decoder, ByteReader *byte_reader) {
  Reset();

  unsigned two_bytes = decoder->Read16Bits(byte_reader);
  unsigned high_bits = two_bytes >> (16 - coding_params_->num_represent_bits_);
  unsigned low_bits = two_bytes & ((1 << (16 - coding_params_->num_represent_bits_)) - 1);
  bool flag = (low_bits < (coding_params_->segment_left_branches_)[high_bits].first);
  int64_t branch = flag ? (coding_params_->segment_left_branches_)[high_bits].second
                        : (coding_params_->segment_right_branches_)[high_bits].second;
  unsigned denominator = coding_params_->branches_[branch].total_weights_;
  unsigned index = (high_bits << 1) + static_cast<unsigned>(!flag);
  unsigned numerator = two_bytes - coding_params_->numerator_helper_[index];
  decoder->Update(denominator, numerator);

  if (branch != 0 && branch != kNumBranch - 1) {
    // histogram part
    branch -= ((kNumBranch - 2) >> 1) + 1;
    SetLeft(branch * branch_bins_);
    SetRight((branch + 1) * branch_bins_);

    HistogramDecompress(decoder, byte_reader);
  } else {
    // the most left or right part
    if (branch == 0)
      SetRight(minimum_ + branch_bins_);
    else
      SetLeft(maximum_);

    ExpDecompress(decoder, byte_reader);
  }
}

void NumericalSquID::ExpDecompress(Decoder *decoder, ByteReader *byte_reader) {
  unsigned two_bytes;
  unsigned numerator;
  unsigned branch;

  while (HasNextBranch()) {
    two_bytes = decoder->Read16Bits(byte_reader);
    branch = two_bytes >> 15;
    numerator = two_bytes & 32767;

    // left boundary and right boundary are determined.
    int64_t mid;
    if (!l_inf_ && !r_inf_)
      mid = (r_ - l_ + 1) / 2;
    else
      mid = step_;

    // right boundary is determined but left is not.
    if (l_inf_)
      mid_ = r_ - mid;
    else
      mid_ = l_ + mid - 1;

    if (branch == 0)
      SetRight(mid_);
    else
      SetLeft(mid_ + 1);

    decoder->Update(32768, numerator);
  }
}

void NumericalSquID::HistogramDecompress(Decoder *decoder, ByteReader *byte_reader) {
  int64_t branch = 0;
  if (num_layer_ != 0) {
    for (int i = num_layer_; i > 0; --i) branch = (branch << 16) | decoder->Read16Bits(byte_reader);
    const int64_t l_record = l_;
    SetLeft(l_record + branch * (1 << mask_last_layer_));
    SetRight(l_record + (branch + 1) * (1 << mask_last_layer_));
  }

  unsigned two_bytes = decoder->Read16Bits(byte_reader);
  branch = two_bytes / weight_branch_last_layer_;
  unsigned denominator = weight_branch_last_layer_;
  unsigned numerator = two_bytes - (branch * weight_branch_last_layer_);
  const int64_t l_record = l_;
  SetLeft(l_record + branch);
  SetRight(l_record + branch);
  decoder->Update(denominator, numerator);
}

AttrValue &NumericalSquID::GetResultAttr(bool round) {
  if (!HasNextBranch()) {
    if (target_int_) {
      attr_.value_ = (static_cast<int>(Round(mean_ + l_ * bin_size_, decimal_places_)));
    } else {
      if (round)
        attr_.value_ = (Round(mean_ + l_ * bin_size_, decimal_places_));
      else
        attr_.value_ = (mean_ + l_ * bin_size_);
    }
    return attr_;
  }

  std::cout << "NumericalSquID::GetResultAttr() should not be called when next branch exists."
            << std::endl;
  return attr_;
}

bool NumericalSquID::HasNextBranch() const {
  if (dev_ < 1e-8) return false;
  // if (is_outlier_) return false;

  return !(r_ == l_ && !l_inf_ && !r_inf_);
}

TableNumerical::TableNumerical(const std::vector<int> &attr_type,
                               const std::vector<size_t> &predictor_list, size_t target_var,
                               double bin_size, bool target_int)
    : SquIDModel(predictor_list, target_var),
      predictor_interpreter_(predictor_list_size_),
      target_int_(target_int),
      bin_size_(bin_size),
      dynamic_list_(GetPredictorCap(predictor_list)),
      dynamic_list_index_(predictor_list.size()),
      squid_(bin_size_, target_int_),
      base_squid_(bin_size, target_int_),
      model_cost_(0) {
  for (int i = 0; i < dynamic_list_.Size(); ++i) dynamic_list_[i].SetBinSize(bin_size_);
  for (size_t i = 0; i < predictor_list_size_; ++i)
    predictor_interpreter_[i] = GetAttrInterpreter(predictor_list_[i]);
}

NumericalSquID *TableNumerical::GetSquID(const AttrVector &tuple) {
  if (dynamic_list_index_.empty()) return &base_squid_;

  GetDynamicListIndex(tuple);
  NumericalStats &stats = dynamic_list_[dynamic_list_index_];
  squid_.Init(stats);
  return &squid_;
}

NumericalSquID *TableNumerical::GetSquID() {
  squid_.Init(dynamic_list_[0]);
  return &squid_;
}

void TableNumerical::GetDynamicListIndex(const AttrVector &tuple) {
  for (size_t i = 0; i < predictor_list_size_; ++i) {
    const AttrValue &attr = tuple.attr_[predictor_list_[i]];
    size_t val = predictor_interpreter_[i]->EnumInterpret(attr);
    if (target_var_ == 0 && predictor_list_.size() == 1 && predictor_list_[0] == 54 && val == 7) {
      int a = 1;
    }
    dynamic_list_index_[i] = val;
  }
}

void TableNumerical::FeedAttrs(const AttrVector &attrs, int count) {
  GetDynamicListIndex(attrs);
  NumericalStats &stat = dynamic_list_[dynamic_list_index_];
  const AttrValue &attr = attrs.attr_[target_var_];
  if (target_int_)
    for (int i = 0; i < count; ++i) stat.PushValue(attr.Int());
  else
    for (int i = 0; i < count; ++i) stat.PushValue(attr.Double());
}

void TableNumerical::FeedAttrs(const AttrValue &integer, int count) {
  NumericalStats &stat = dynamic_list_[0];
  if (target_int_)
    for (int i = 0; i < count; ++i) stat.PushValue(integer.Int());
  else
    for (int i = 0; i < count; ++i) stat.PushValue(integer.Double());
}

void TableNumerical::EndOfData() {
  for (size_t i = 0; i < dynamic_list_.Size(); ++i) {
    NumericalStats &stat = dynamic_list_[i];
    stat.End();

    if (stat.mean_abs_dev_ != 0) {
      model_cost_ +=
          stat.v_count_ * (log2(stat.mean_abs_dev_) + 1 + log2(kEulerConstant) - log2(bin_size_));
    }
  }
  base_squid_.Init(dynamic_list_[0]);
  model_cost_ += GetModelDescriptionLength();
}

int TableNumerical::GetModelDescriptionLength() const {
  size_t table_size = dynamic_list_.Size();
  // See WriteModel function for details of model description.
  return table_size * (32 * (4 + kNumBranch + 1)) + predictor_list_size_ * 16 + 40;
}

void TableNumerical::WriteModel(SequenceByteWriter *byte_writer) {
  unsigned char bytes[4];
  byte_writer->WriteByte(predictor_list_size_);
  for (size_t i = 0; i < predictor_list_size_; ++i) {
    byte_writer->Write16Bit(predictor_list_[i]);
  }

  ConvertSinglePrecision(bin_size_, bytes);
  byte_writer->Write32Bit(bytes);

  // Write Model Parameters
  size_t table_size = dynamic_list_.Size();
  for (size_t i = 0; i < table_size; ++i) {
    NumericalStats &stat = dynamic_list_[i];
    // suppose this stat does not have any data, we do not write it and use the first stat instead.
    if (stat.v_count_ == 0) stat = dynamic_list_[0];
    stat.WriteStats(byte_writer);
  }
}

TableNumerical *TableNumerical::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                          size_t index, bool target_int) {
  size_t predictor_size = byte_reader->ReadByte();
  std::vector<size_t> predictor_list;
  for (size_t i = 0; i < predictor_size; ++i) predictor_list.push_back(byte_reader->Read16Bit());

  unsigned char bytes[4];
  byte_reader->Read32Bit(bytes);
  double bin_size = ConvertSinglePrecision(bytes);
  TableNumerical *model =
      new TableNumerical(schema.attr_type_, predictor_list, index, bin_size, target_int);

  // Read Model Parameters
  size_t table_size = model->dynamic_list_.Size();
  for (size_t i = 0; i < table_size; ++i) {
    NumericalStats &stat = model->dynamic_list_[i];
    stat.ReadStats(byte_reader);
  }

  model->base_squid_.Init(model->dynamic_list_[0]);
  return model;
}

SquIDModel *TableNumericalRealCreator::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                                 size_t index) {
  return TableNumerical::ReadModel(byte_reader, schema, index, false);
}
SquIDModel *TableNumericalRealCreator::CreateModel(const std::vector<int> &attr_type,
                                                   const std::vector<size_t> &predictor_list,
                                                   size_t target_var, double err) {
  size_t table_size = 1;
  for (int attr : predictor_list) {
    if (!GetAttrInterpreter(attr)->EnumInterpretable()) {
      return nullptr;
    }
    table_size *= GetAttrInterpreter(attr)->EnumCap();
  }
  if (table_size > kMaxTableSize) {
    return nullptr;
  }
  return new TableNumerical(attr_type, predictor_list, target_var, err * 2, false);
}

SquIDModel *TableNumericalIntCreator::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                                size_t index) {
  return TableNumerical::ReadModel(byte_reader, schema, index, true);
}
SquIDModel *TableNumericalIntCreator::CreateModel(const std::vector<int> &attr_type,
                                                  const std::vector<size_t> &predictor_list,
                                                  size_t target_var, double err) {
  // filter illegal model, i.e. all predictors should have a non-zero capacity.
  size_t table_size = 1;
  for (int attr : predictor_list) {
    if (!GetAttrInterpreter(attr)->EnumInterpretable()) return nullptr;
    table_size *= GetAttrInterpreter(attr)->EnumCap();
  }
  if (table_size > kMaxTableSize) return nullptr;
  return new TableNumerical(attr_type, predictor_list, target_var,
                            std::max(1, static_cast<int>(floor(2 * err))), true);
}

}  // namespace db_compress
