//
// Created by Qiao Yiming on 2022/5/8.
//

#include "../include/timeseries_model.h"
#include "../include/utility.h"
namespace db_compress {
namespace {
const double kEulerConstant = std::exp(1.0);
}
void db_compress::TimeSeriesStats::End() { res_stats_.End(); }

void db_compress::TimeSeriesStats::WriteStats(db_compress::SequenceByteWriter *byte_writer) const {
  res_stats_.WriteStats(byte_writer);
}

void db_compress::TimeSeriesStats::ReadStats(db_compress::ByteReader *byte_reader) {
  res_stats_.ReadStats(byte_reader);
}

void TimeSeriesStats::PushValues(std::vector<double> &values_buffer, int length) {
  // Calculate Parameters
  std::vector<double> coefficients(degree_);
  //  double mean =
  //      ar::AutoRegression(values_buffer, (length > 5000) ? 5000 : length, degree_, coefficients);
  double mean = 0;
  QuantizationToFloat32Bit(&mean);
  for (int i = 0; i < degree_; ++i) QuantizationToFloat32Bit(&coefficients[i]);

  // Train Residual
  double est = 0;
  for (int i = 0; i < length; ++i) {
    est = mean;
    if (i >= degree_)
      for (int j = 0; j < degree_; ++j) est += coefficients[j] * values_buffer[i - j - 1];

    res_stats_.PushValue(values_buffer[i] - est);
  }
}

// TimeSeries SquID
void db_compress::TimeSeriesSquID::Init(db_compress::TimeSeriesStats &stats) {
  degree_ = stats.degree_;
  history_.resize(degree_);
  coefficients_.resize(degree_);
  numerical_squid_.Init(stats.res_stats_);

  ts_index_ = 0;
}

const db_compress::AttrValue &db_compress::TimeSeriesSquID::GetResultAttr() {
  AttrValue &ans = numerical_squid_.GetResultAttr(false);
  double res = ans.Double();
  if (target_int_)
    ans.value_ = (static_cast<int>(res + GetEstValue()));
  else
    ans.value_ = (Round(res + GetEstValue(), decimal_places_));
  RecordHistory(ans.Double());
  return ans;
}

void db_compress::TimeSeriesSquID::GetProbIntervals(std::vector<Branch *> &prob_intervals,
                                                    int &prob_intervals_index,
                                                    std::vector<double> &time_series, int length) {
  // mean_ = ar::AutoRegression(time_series, (length > 5000) ? 5000 : length, degree_,
  // coefficients_);
  mean_ = 0;
  QuantizationToFloat32Bit(&mean_);
  for (int i = 0; i < degree_; ++i) QuantizationToFloat32Bit(&coefficients_[i]);

  WriteARParams(prob_intervals, prob_intervals_index);

  double est = 0;
  for (int i = 0; i < length; ++i) {
    est = mean_;
    if (i >= degree_)
      for (int j = 0; j < degree_; ++j) est += coefficients_[j] * time_series[i - j - 1];

    numerical_squid_.GetProbIntervals(prob_intervals, prob_intervals_index,
                                      AttrValue(time_series[i] - est));

    rms_ += (time_series[i] - est) * (time_series[i] - est);
    rms_count_++;
  }
}

TimeSeriesSquID::~TimeSeriesSquID() {
  if (rms_count_ > 0) {
    std::cout << "Coefficient: ";
    for (double coe : coefficients_) std::cout << coe << "\t";
    std::cout << "\n";
    std::cout << "RMS: " << sqrt(rms_ / rms_count_) << "\n";
  }
}

TimeSeriesSquID::TimeSeriesSquID(double bin_size, bool target_int)
    : target_int_(target_int), numerical_squid_(bin_size, false) {
  if (!target_int_) {
    if (bin_size >= 1 || bin_size == 0)
      std::cout << "Bin size of double cannot be zero or larger than 1.\n";
    decimal_places_ = 0;
    while (bin_size * 10 < 1) {
      bin_size *= 10;
      decimal_places_++;
    }
  }
}
double TimeSeriesSquID::GetEstValue() {
  double est_value = mean_;

  if (ts_index_ >= degree_) {
    for (int i = 0; i < degree_; ++i) {
      int idx = (history_index_ - i - 1);
      if (idx < 0) idx += 5;
      est_value += coefficients_[i] * history_[idx];
    }
  }

  return est_value;
}
void TimeSeriesSquID::Decompress(Decoder *decoder, ByteReader *byte_reader,
                                 std::vector<double> &time_series, int length) {
  LoadARParams(decoder, byte_reader);
  for (ts_index_ = 0; ts_index_ < time_series.size(); ++ts_index_) {
    numerical_squid_.Decompress(decoder, byte_reader);
    time_series[ts_index_] = GetResultAttr().Double();
  }
}
void TimeSeriesSquID::WriteARParams(std::vector<Branch *> &prob_intervals,
                                    int &prob_intervals_index) {
  unsigned char bytes[4];
  ConvertSinglePrecision(mean_, bytes);
  for (int i = 0; i < 4; ++i) prob_intervals[prob_intervals_index++] = GetSimpleBranch(1, bytes[i]);
  for (int j = 0; j < degree_; ++j) {
    ConvertSinglePrecision(coefficients_[j], bytes);
    for (int i = 0; i < 4; ++i)
      prob_intervals[prob_intervals_index++] = GetSimpleBranch(1, bytes[i]);
  }
}
void TimeSeriesSquID::LoadARParams(Decoder *decoder, ByteReader *byte_reader) {
  unsigned char bytes[4];
  for (int i = 0; i < 4; ++i) bytes[i] = decoder->Read16Bits(byte_reader);
  mean_ = ConvertSinglePrecision(bytes);

  for (int j = 0; j < degree_; ++j) {
    for (int i = 0; i < 4; ++i) bytes[i] = decoder->Read16Bits(byte_reader);
    coefficients_[j] = ConvertSinglePrecision(bytes);
  }
}

// TimeSeries SquIDModel
db_compress::TableTimeSeries::TableTimeSeries(const std::vector<int> &attr_type,
                                              const std::vector<size_t> &predictor_list,
                                              size_t target_var, double bin_size, bool target_int)
    : SquIDModel(predictor_list, target_var),
      predictor_interpreter_(predictor_list_size_),
      target_int_(target_int),
      bin_size_(bin_size),
      dynamic_list_(GetPredictorCap(predictor_list)),
      dynamic_list_index_(predictor_list.size()),
      squid_(bin_size, target_int_),
      model_cost_(0) {
  QuantizationToFloat32Bit(&bin_size_);
  for (int i = 0; i < dynamic_list_.Size(); ++i) {
    dynamic_list_[i].SetBinSize(bin_size_);
  }
  for (size_t i = 0; i < predictor_list_size_; ++i) {
    predictor_interpreter_[i] = GetAttrInterpreter(attr_type[predictor_list_[i]]);
  }
}

db_compress::TimeSeriesSquID *db_compress::TableTimeSeries::GetSquID(
    const db_compress::AttrVector &tuple) {
  GetDynamicListIndex(tuple);
  TimeSeriesStats &stats = dynamic_list_[dynamic_list_index_];
  squid_.Init(stats);
  return &squid_;
}

void db_compress::TableTimeSeries::EndOfData() {
  for (size_t i = 0; i < dynamic_list_.Size(); ++i) {
    TimeSeriesStats &ts_stat = dynamic_list_[i];
    ts_stat.End();

    NumericalStats &stat = ts_stat.res_stats_;

    if (stat.mean_abs_dev_ != 0) {
      model_cost_ +=
          stat.v_count_ * (log2(stat.mean_abs_dev_) + 1 + log2(kEulerConstant) - log2(bin_size_));
    }
  }
  model_cost_ += GetModelDescriptionLength();
}

void db_compress::TableTimeSeries::WriteModel(db_compress::SequenceByteWriter *byte_writer) {
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
    const TimeSeriesStats &stat = dynamic_list_[i];
    stat.WriteStats(byte_writer);
  }
}

db_compress::TableTimeSeries *db_compress::TableTimeSeries::ReadModel(
    db_compress::ByteReader *byte_reader, const db_compress::Schema &schema, size_t index,
    bool target_int) {
  size_t predictor_size = byte_reader->ReadByte();
  std::vector<size_t> predictor_list;
  for (size_t i = 0; i < predictor_size; ++i) predictor_list.push_back(byte_reader->Read16Bit());

  unsigned char bytes[4];
  byte_reader->Read32Bit(bytes);
  double bin_size = ConvertSinglePrecision(bytes);
  TableTimeSeries *model =
      new TableTimeSeries(schema.attr_type_, predictor_list, index, bin_size, target_int);

  // Read Model Parameters
  size_t table_size = model->dynamic_list_.Size();
  for (size_t i = 0; i < table_size; ++i) {
    TimeSeriesStats &stat = model->dynamic_list_[i];
    stat.ReadStats(byte_reader);
  }

  return model;
}

void TableTimeSeries::GetDynamicListIndex(const AttrVector &tuple) {
  for (size_t i = 0; i < predictor_list_size_; ++i) {
    const AttrValue &attr = tuple.attr_[predictor_list_[i]];
    size_t val = predictor_interpreter_[i]->EnumInterpret(attr);
    dynamic_list_index_[i] = val;
  }
}

int TableTimeSeries::GetModelDescriptionLength() const {
  size_t table_size = dynamic_list_.Size();
  // See WriteModel function for details of model description.
  return table_size * (32 * (4 + kNumBranch + 1)) + predictor_list_size_ * 16 + 40;
}

SquIDModel *TableTimeSeriesCreator::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                              size_t index) {
  return TableTimeSeries::ReadModel(byte_reader, schema, index, false);
}

SquIDModel *TableTimeSeriesCreator::CreateModel(const std::vector<int> &attr_type,
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
  return new TableTimeSeries(attr_type, predictor_list, target_var, err * 2, false);
}
}  // namespace db_compress
