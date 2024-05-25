#include "../include/numerical_model.h"

#include <algorithm>
#include <cmath>
#include <vector>

#include "../include/base.h"
#include "../include/model.h"
#include "../include/squish_exception.h"
#include "../include/utility.h"

namespace db_compress {

namespace {
const double kEulerConstant = std::exp(1.0);

}  // anonymous namespace

void NumericalStats::InitHistogramStructure() {
  if (count_ == 0) {
    // no data, so no histogram
    return;
  }

  sort(values_.begin(), values_.begin() + count_);
  median_ = values_[count_ / 2];

  // estimate deviation
  double dev_est = 0;
  for (int i = 0; i < count_; ++i) dev_est += fabs(values_[i] - median_);

  dev_est /= count_;

  // 2022.02.11. Now, numerical model supports multi-layer histogram. Except the first layer, each
  // layer is a uniform distribution. The maximum number of layer is limited by int64_t.
  double total_range = 2 * kNumSigma * dev_est;
  uint64_t num_total_bins = static_cast<uint64_t>(total_range / bin_size_);
  uint64_t num_bins_each_branch = num_total_bins / (kNumBranch1stLayer - 2);
  if (num_bins_each_branch == 0) num_bins_each_branch = 1;
  while (num_bins_each_branch > 65536) {
    num_bins_each_branch = (num_bins_each_branch >> 16);
    if (num_bins_each_branch != 65536) num_bins_each_branch++;
    num_layers_++;
  }
  num_bits_last_layer_ = RoundToNearest2Exp(num_bins_each_branch);

  // determine the range of layer one: minimum and maximum.
  uint64_t size_branch_1st_layer = (1 << num_bits_last_layer_);
  for (int i = 0; i < num_layers_; ++i) size_branch_1st_layer *= 65536;
  length_branch_1st_layer_ = size_branch_1st_layer * bin_size_;

  int half_num_branch_layer1 = ((kNumBranch1stLayer - 2) >> 1);
  minimum_ = median_ - length_branch_1st_layer_ * half_num_branch_layer1;
  maximum_ = median_ + length_branch_1st_layer_ * half_num_branch_layer1;

  // get weight of effective branch in last layer (uniform distribution). Effective branch means
  // non-zero weight branch.
  weight_branch_last_layer_ = 65536 / num_bins_each_branch;

  histogram_structure_estimated_ = true;
  count_ = 0;
}

void NumericalStats::PushValue(double value) {
  if (!histogram_structure_estimated_) {
    values_[count_] = value;
    if (++count_ >= kNumEstSample) {
      InitHistogramStructure();
    }
  } else {
    ++count_;
    int interval_index;
    if (value <= minimum_) {
      interval_index = 0;
    } else if (value >= maximum_) {
      interval_index = kNumBranch1stLayer - 1;
    } else {
      interval_index = static_cast<int>((value - minimum_) / length_branch_1st_layer_) + 1;
    }

    ++histogram_[interval_index];
    sum_abs_dev_ += fabs(value - median_);
  }
}

void NumericalStats::End() {
  if (!histogram_structure_estimated_) {
    InitHistogramStructure();
  }
  if (sum_abs_dev_ < bin_size_) {
    mean_abs_dev_ = 0;
  } else {
    mean_abs_dev_ = sum_abs_dev_ / count_;
  }

  QuantizationToFloat32Bit(&mean_abs_dev_);
  step_ = ceil(mean_abs_dev_ / bin_size_);

  // Calculate weight of each branch, make sure: 1) total of weights is
  // 65536; 2) any possible branch has a positive weight.
  unsigned total = 0;
  std::vector<unsigned> weights_1st_layer(histogram_.size());
  for (unsigned num_value : histogram_) {
    total += num_value;
  }
  // ensure the second condition
  for (int i = 0; i < static_cast<int>(weights_1st_layer.size()); ++i) {
    // variable total cannot be zero, since size of histogram is
    // kNumBranch1stLayer and each branch weight is larger than zero.
    weights_1st_layer[i] = histogram_[i] * 65536 / total;
    // maintain the second condition, branch is possible but has zero weight.
    if (histogram_[i] > 0 && weights_1st_layer[i] == 0) {
      weights_1st_layer[i] = 1;
    }
  }

  // maintain the first condition
  unsigned posterior_total = 0;
  unsigned index_max = 0;
  for (int i = 0; i < weights_1st_layer.size(); ++i) {
    posterior_total += weights_1st_layer[i];
    if (weights_1st_layer[i] > weights_1st_layer[index_max]) {
      index_max = i;
    }
  }
  weights_1st_layer[index_max] += (65536 - posterior_total);

  // generate cdf from weights of first layer
  unsigned cdf_sum = 0;
  cdf_1st_layer_.resize(histogram_.size() + 1, 0);
  for (int i = 0; i < histogram_.size(); ++i) {
    cdf_sum += weights_1st_layer[i];
    cdf_1st_layer_[i + 1] = cdf_sum;
  }
}

NumericalSquID::NumericalSquID(double bin_size, bool target_int)
    : target_int_(target_int), bin_size_(bin_size) {}

void NumericalSquID::Init(NumericalStats &stats) {
  mean_ = stats.median_;
  dev_ = stats.mean_abs_dev_;
  step_ = stats.step_;
  num_bits_last_layer_ = stats.num_bits_last_layer_;
  weight_branch_last_layer_ = stats.weight_branch_last_layer_;
  num_layer_ = stats.num_layers_;
  minimum_ = stats.minimum_;
  maximum_ = stats.maximum_;

  cdf_base16_ = &stats.cdf_1st_layer_;

  // get size of each branch in first layer, i.e. how many bins it contains.
  size_branch_1st_layer_ = (1 << num_bits_last_layer_);
  for (int i = 0; i < num_layer_; ++i) size_branch_1st_layer_ <<= 16;

  l_ = r_ = 0;
  l_inf_ = r_inf_ = true;

  // Total number of branch is kNumBranch1stLayer. All real number is grouped
  // into many branches, we want to get the left boundary of histogram, i.e. the
  // bin index where minimum value of the second left branch locates.
  //
  //     most left branch | ... ... median ... ... | most right branch
  //                      *
  //            bin index of value we want.
  int64_t index_second_left = -((kNumBranch1stLayer - 2) >> 1);
  hist_left_boundary_ = index_second_left * size_branch_1st_layer_;
}

void NumericalSquID::GetProbIntervals(std::vector<ProbInterval> &prob_intervals,
                                      int &prob_intervals_index, const AttrValue &attr_value) {
  double value;
  if (target_int_)
    value = attr_value.Int();
  else
    value = attr_value.Double();

  if (value >= minimum_ && value < maximum_)
    // histogram part
    GetHistogramProbIntervals(prob_intervals, prob_intervals_index, value);
  else
    // most left or right part
    GetExpProbIntervals(prob_intervals, prob_intervals_index, value);
}

void NumericalSquID::GetExpProbIntervals(std::vector<ProbInterval> &prob_intervals,
                                         int &prob_intervals_index, double value) {
  uint64_t branch;
  if (value < minimum_)
    branch = 0;
  else
    branch = kNumBranch1stLayer - 1;

  prob_intervals[prob_intervals_index++] =
      ProbInterval((*cdf_base16_)[branch], (*cdf_base16_)[branch + 1]);

  if (branch == 0)
    // value lie in the most left branch
    SetRight(hist_left_boundary_);
  else
    // value lie in the most right branch, maximum value in
    // histogram range is the negative of minimum value
    SetLeft(-hist_left_boundary_);

  while (HasNextBranch()) {
    int64_t exp_mid;
    if (!l_inf_ && !r_inf_)
      exp_mid = (r_ - l_ + 1) / 2;
    else
      exp_mid = step_;

    if (l_inf_)
      // Reversed
      mid_ = r_ - exp_mid;
    else
      mid_ = l_ + exp_mid - 1;

    branch = (value - mean_ >= (static_cast<double>(mid_) + 0.5) * bin_size_);
    if (branch == 1) {
      prob_intervals[prob_intervals_index++] = ProbInterval(32768, 65536);
      SetLeft(mid_ + 1);
    } else {
      prob_intervals[prob_intervals_index++] = ProbInterval(0, 32768);
      SetRight(mid_);
    }
  }
}

void NumericalSquID::GetHistogramProbIntervals(std::vector<ProbInterval> &prob_intervals,
                                               int &prob_intervals_index, double value) {
  // first layer
  // variable 'branch' should be 64 bits. Although there are only kNumBranch branch in the first
  // layer, but this variable is also used in the following layers.
  uint64_t branch =
      static_cast<unsigned>((value - minimum_) / (size_branch_1st_layer_ * bin_size_)) + 1;
  prob_intervals[prob_intervals_index++] =
      ProbInterval((*cdf_base16_)[branch], (*cdf_base16_)[branch + 1]);
  SetLeft(hist_left_boundary_ + (branch - 1) * size_branch_1st_layer_);
  SetRight(hist_left_boundary_ + branch * size_branch_1st_layer_);

  // remaining layers
  branch = GetRelativeBinIndex(value);
  if (num_layer_ != 0) {
    for (int i = num_layer_; i > 0; --i) {
      unsigned mask_shift = num_bits_last_layer_ + (i - 1) * 16;
      unsigned layer_branch = (branch >> mask_shift) & 0xffff;
      prob_intervals[prob_intervals_index++] = ProbInterval(layer_branch, layer_branch + 1);
    }
  }

  // last layers
  branch = branch & ((1 << num_bits_last_layer_) - 1);
  prob_intervals[prob_intervals_index++] =
      ProbInterval(branch * weight_branch_last_layer_, (branch + 1) * weight_branch_last_layer_);
}

void NumericalSquID::Decompress(Decoder *decoder, ByteReader *byte_reader) {
  while (decoder->GetPIb().exp_ < 40) decoder->FeedByte(byte_reader->ReadByte());
  prob_pit_pib_ratio_ = decoder->GetPIbPItRatio();
  branch_left_ = 0;
  branch_right_ = kNumBranch1stLayer - 1;
  while (branch_left_ != branch_right_) {
    branch_mid_ = (branch_left_ + branch_right_) / 2;
    prob_right_ = (*cdf_base16_)[branch_mid_ + 1];
    if (prob_right_ > prob_pit_pib_ratio_) {
      branch_right_ = branch_mid_;
    } else {
      branch_left_ = branch_mid_ + 1;
    }
  }
  unsigned branch = branch_left_;
  decoder->UpdatePIt({(*cdf_base16_)[branch], (*cdf_base16_)[branch + 1]});

  if (branch != 0 && branch != kNumBranch1stLayer - 1) {
    // histogram part
    SetLeft(hist_left_boundary_ + (branch - 1) * size_branch_1st_layer_);
    SetRight(hist_left_boundary_ + branch * size_branch_1st_layer_);

    HistogramDecompress(decoder, byte_reader);
  } else {
    // the most left or right part
    if (branch == 0)
      SetRight(hist_left_boundary_);
    else
      SetLeft(-hist_left_boundary_);

    ExpDecompress(decoder, byte_reader);
  }
}

void NumericalSquID::HistogramDecompress(Decoder *decoder, ByteReader *byte_reader) {
  uint64_t branch;
  if (num_layer_ != 0) {
    for (int i = num_layer_; i > 0; --i) {
      while (decoder->GetPIb().exp_ < 40) decoder->FeedByte(byte_reader->ReadByte());
      branch = decoder->GetPIbPItRatio();
      decoder->UpdatePIt({branch, branch + 1});
      // Second layer, histogram part, branch represents the number of bins away
      // from the left boundary. WARNING: l_ changes after calling SetLeft(),
      // thus it is necessary to record l_ at first.
      const int64_t l_record = l_;
      SetLeft(l_record + branch * (1 << (num_bits_last_layer_ + (i - 1) * 16)));
      SetRight(l_record + (branch + 1) * (1 << (num_bits_last_layer_ + (i - 1) * 16)));
    }
  }

  // last layer
  while (decoder->GetPIb().exp_ < 40) decoder->FeedByte(byte_reader->ReadByte());
  prob_pit_pib_ratio_ = decoder->GetPIbPItRatio();
  branch = prob_pit_pib_ratio_ / weight_branch_last_layer_;
  decoder->UpdatePIt(
      {branch * weight_branch_last_layer_, (branch + 1) * weight_branch_last_layer_});
  const int64_t l_record = l_;
  SetLeft(l_record + branch);
  SetRight(l_record + branch);
}

void NumericalSquID::ExpDecompress(Decoder *decoder, ByteReader *byte_reader) {
  unsigned branch;
  unsigned exp_prob;

  while (HasNextBranch()) {
    while (decoder->GetPIb().exp_ < 40) decoder->FeedByte(byte_reader->ReadByte());
    prob_pit_pib_ratio_ = decoder->GetPIbPItRatio();
    // get branch
    branch = prob_pit_pib_ratio_ >= 32768 ? 1 : 0;

    int64_t exp_mid;
    if (!l_inf_ && !r_inf_)
      exp_mid = (r_ - l_ + 1) / 2;
    else
      exp_mid = step_;

    if (l_inf_)
      // Reversed
      mid_ = r_ - exp_mid;
    else
      mid_ = l_ + exp_mid - 1;

    if (branch == 1) {
      decoder->UpdatePIt({32768, 65536});
      SetLeft(mid_ + 1);
    } else {
      decoder->UpdatePIt({0, 32768});
      SetRight(mid_);
    }
  }
}

AttrValue &NumericalSquID::GetResultAttr() {
  if (!HasNextBranch()) {
    if (target_int_) {
      attr_.Set(static_cast<int>(floor(mean_ + static_cast<double>(l_) * bin_size_ + 0.5)));
    } else {
      attr_.Set(mean_ + static_cast<double>(l_) * bin_size_);
    }
    return attr_;
  }
  std::cout << "NumericalSquID::GetResultAttr() is not called when HasNextBranch() is true."
            << std::endl;
  return attr_;
}

int64_t NumericalSquID::GetRelativeBinIndex(double value) const {
  int64_t bin_index = static_cast<int64_t>(floor((value - mean_) / bin_size_ + 0.5));
  uint64_t relative_bin_index = bin_index - l_;
  // for some cases, branch may equal to the right boundary.
  if (relative_bin_index >= size_branch_1st_layer_) relative_bin_index--;
  return relative_bin_index;
}

bool NumericalSquID::HasNextBranch() const {
  if (dev_ < 1e-6) {
    return false;
  }

  return !(r_ == l_ && !l_inf_ && !r_inf_);
}

TableNumerical::TableNumerical(const std::vector<int> &attr_type,
                               const std::vector<size_t> &predictor_list, size_t target_var,
                               double err, bool target_int)
    : SquIDModel(predictor_list, target_var),
      predictor_interpreter_(predictor_list_size_),
      target_int_(target_int),
      bin_size_((target_int_ ? floor(err) * 2 + 1 : err * 2)),
      dynamic_list_(GetPredictorCap(predictor_list)),
      dynamic_list_index_(predictor_list.size()),
      squid_(bin_size_, target_int_),
      model_cost_(0) {
  for (int i = 0; i < dynamic_list_.Size(); ++i) {
    dynamic_list_[i].SetBinSize(bin_size_);
  }
  for (size_t i = 0; i < predictor_list_size_; ++i) {
    predictor_interpreter_[i] = GetAttrInterpreter(attr_type[predictor_list_[i]]);
  }
}
NumericalSquID *TableNumerical::GetSquID(const AttrVector &tuple) {
  GetDynamicListIndex(tuple);
  NumericalStats &stats = dynamic_list_[dynamic_list_index_];
  squid_.Init(stats);
  return &squid_;
}
void TableNumerical::GetDynamicListIndex(const AttrVector &tuple) {
  for (size_t i = 0; i < predictor_list_size_; ++i) {
    const AttrValue &attr = tuple.attr_[predictor_list_[i]];
    size_t val = predictor_interpreter_[i]->EnumInterpret(attr);
    dynamic_list_index_[i] = val;
  }
}
void TableNumerical::FeedTuple(const AttrVector &tuple) {
  GetDynamicListIndex(tuple);
  double target_val;
  const AttrValue &attr = tuple.attr_[target_var_];
  if (target_int_) {
    target_val = attr.Int();
  } else {
    target_val = attr.Double();
  }
  NumericalStats &stat = dynamic_list_[dynamic_list_index_];
  stat.PushValue(target_val);
}
void TableNumerical::EndOfData() {
  for (size_t i = 0; i < dynamic_list_.Size(); ++i) {
    NumericalStats &stat = dynamic_list_[i];
    stat.End();

    if (stat.mean_abs_dev_ != 0) {
      model_cost_ +=
          stat.count_ * (log2(stat.mean_abs_dev_) + 1 + log2(kEulerConstant) - log2(bin_size_));
    }
  }
  model_cost_ += GetModelDescriptionLength();
}
int TableNumerical::GetModelDescriptionLength() const {
  size_t table_size = dynamic_list_.Size();
  // See WriteModel function for details of model description.
  return table_size * (32 * (4 + kNumBranch1stLayer + 1)) + predictor_list_size_ * 16 + 40;
}
void TableNumerical::WriteModel(SequenceByteWriter *byte_writer) const {
  unsigned char bytes[4];
  byte_writer->WriteByte(predictor_list_size_);
  for (size_t i = 0; i < predictor_list_size_; ++i) {
    byte_writer->Write16Bit(predictor_list_[i]);
  }

  ConvertSinglePrecision(bin_size_, bytes);
  byte_writer->Write32Bit(bytes);

  // Write Model Parameters
  // 32 * (6 + branch + 1)
  size_t table_size = dynamic_list_.Size();
  for (size_t i = 0; i < table_size; ++i) {
    const NumericalStats &stat = dynamic_list_[i];
    ConvertSinglePrecision(stat.median_, bytes);
    byte_writer->Write32Bit(bytes);
    ConvertSinglePrecision(stat.mean_abs_dev_, bytes);
    byte_writer->Write32Bit(bytes);
    ConvertSinglePrecision(stat.count_, bytes);
    byte_writer->Write32Bit(bytes);
    ConvertSinglePrecision(stat.step_, bytes);
    byte_writer->Write32Bit(bytes);
    ConvertSinglePrecision(stat.num_bits_last_layer_, bytes);
    byte_writer->Write32Bit(bytes);
    ConvertSinglePrecision(stat.weight_branch_last_layer_, bytes);
    byte_writer->Write32Bit(bytes);
    ConvertSinglePrecision(stat.num_layers_, bytes);
    byte_writer->Write32Bit(bytes);
    for (unsigned param : stat.cdf_1st_layer_) {
      ConvertSinglePrecision(param, bytes);
      byte_writer->Write32Bit(bytes);
    }
  }
}
SquIDModel *TableNumerical::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                      size_t target_var, bool target_int) {
  size_t predictor_size = byte_reader->ReadByte();
  std::vector<size_t> predictor_list;
  for (size_t i = 0; i < predictor_size; ++i) {
    predictor_list.push_back(byte_reader->Read16Bit());
  }
  TableNumerical *model =
      new TableNumerical(schema.attr_type_, predictor_list, target_var, 0, target_int);
  unsigned char bytes[4];
  byte_reader->Read32Bit(bytes);
  model->bin_size_ = ConvertSinglePrecision(bytes);
  model->squid_ = NumericalSquID(model->bin_size_, target_int);

  // Write Model Parameters
  size_t table_size = model->dynamic_list_.Size();
  for (size_t i = 0; i < table_size; ++i) {
    NumericalStats &stat = model->dynamic_list_[i];
    byte_reader->Read32Bit(bytes);
    stat.median_ = ConvertSinglePrecision(bytes);
    byte_reader->Read32Bit(bytes);
    stat.mean_abs_dev_ = ConvertSinglePrecision(bytes);
    byte_reader->Read32Bit(bytes);
    stat.count_ = ConvertSinglePrecision(bytes);
    byte_reader->Read32Bit(bytes);
    stat.step_ = ConvertSinglePrecision(bytes);
    byte_reader->Read32Bit(bytes);
    stat.num_bits_last_layer_ = ConvertSinglePrecision(bytes);
    byte_reader->Read32Bit(bytes);
    stat.weight_branch_last_layer_ = ConvertSinglePrecision(bytes);
    byte_reader->Read32Bit(bytes);
    stat.num_layers_ = ConvertSinglePrecision(bytes);
    stat.cdf_1st_layer_.resize(kNumBranch1stLayer + 1);
    for (int j = 0; j < kNumBranch1stLayer + 1; ++j) {
      byte_reader->Read32Bit(bytes);
      stat.cdf_1st_layer_[j] = ConvertSinglePrecision(bytes);
    }
  }

  return model;
}

SquIDModel *TableNumericalRealCreator::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                                 size_t index) {
  return TableNumerical::ReadModel(byte_reader, schema, index, false);
}
SquIDModel *TableNumericalRealCreator::CreateModel(const std::vector<int> &attr_type,
                                                   const std::vector<size_t> &predictors,
                                                   size_t index, double err) {
  size_t table_size = 1;
  for (int attr : predictors) {
    if (!GetAttrInterpreter(attr)->EnumInterpretable()) {
      return nullptr;
    }
    table_size *= GetAttrInterpreter(attr)->EnumCap();
  }
  if (table_size > kMaxTableSize) {
    return nullptr;
  }
  return new TableNumerical(attr_type, predictors, index, err, false);
}
SquIDModel *TableNumericalIntCreator::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                                size_t index) {
  return TableNumerical::ReadModel(byte_reader, schema, index, true);
}
SquIDModel *TableNumericalIntCreator::CreateModel(const std::vector<int> &attr_type,
                                                  const std::vector<size_t> &predictors,
                                                  size_t index, double err) {
  size_t table_size = 1;
  for (int attr : predictors) {
    if (!GetAttrInterpreter(attr)->EnumInterpretable()) return nullptr;
    table_size *= GetAttrInterpreter(attr)->EnumCap();
  }
  if (table_size > kMaxTableSize) {
    return nullptr;
  }
  return new TableNumerical(attr_type, predictors, index, err, true);
}
}  // namespace db_compress
