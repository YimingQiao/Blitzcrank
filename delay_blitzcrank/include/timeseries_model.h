/**
 * @file time-series_model.h
 * @brief This is a header for times series squID and squIDModel
 */

#ifndef DB_COMPRESS_TIMESERIES_MODEL_H
#define DB_COMPRESS_TIMESERIES_MODEL_H

#include <cmath>
#include <vector>

#include "numerical_model.h"
#include "simple_prob_interval_pool.h"

namespace db_compress {
const double kTimeSeriesPrecision = 0.00499;
/**
 * Stats for times series, it is a wrapper of numerical model.
 */
class TimeSeriesStats {
 public:
  // AR model
  int degree_{5};

  // residual
  NumericalStats res_stats_;

  void SetBinSize(double bin_size) { res_stats_.SetBinSize(bin_size); }

  void End();

  void WriteStats(SequenceByteWriter *byte_writer) const;

  void ReadStats(ByteReader *byte_reader);

  void PushValues(std::vector<double> &values_buffer, int length);
};

class TimeSeriesSquID {
 public:
  TimeSeriesSquID(double bin_size, bool target_int);

  ~TimeSeriesSquID();

  void Init(db_compress::TimeSeriesStats &stats);

  const AttrValue &GetResultAttr();

  void GetProbIntervals(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                        std::vector<double> &time_series, int length);

  void Decompress(Decoder *decoder, ByteReader *byte_reader, std::vector<double> &time_series,
                  int length);

 private:
  int degree_;
  bool target_int_;
  int decimal_places_;
  double mean_;
  std::vector<double> coefficients_;

  NumericalSquID numerical_squid_;

  // history
  int ts_index_{0};
  int history_index_{0};
  std::vector<double> history_;

  // rms_
  double rms_{0};
  int rms_count_{0};

  double GetEstValue();

  void RecordHistory(double value) {
    history_[history_index_] = Round(value, decimal_places_);
    history_index_ = (history_index_ + 1) % 5;
  }

  void WriteARParams(std::vector<Branch *> &prob_intervals, int &prob_intervals_index);

  void LoadARParams(Decoder *decoder, ByteReader *byte_reader);
};

class TableTimeSeries : public SquIDModel {
 public:
  std::vector<double> time_series_buffer_;

  TableTimeSeries(const std::vector<int> &attr_type, const std::vector<size_t> &predictor_list,
                  size_t target_var, double bin_size, bool target_int);

  void FeedAttrs(const AttrVector &attrs, int count) override {}

  void FeedTimeSeries(const AttrVector &tuple, int length) {
    GetDynamicListIndex(tuple);
    TimeSeriesStats &stat = dynamic_list_[dynamic_list_index_];
    stat.PushValues(time_series_buffer_, length);
  }

  TimeSeriesSquID *GetSquID(const AttrVector &tuple);

  int GetModelCost() const override { return static_cast<int>(model_cost_); }

  void EndOfData() override;

  void WriteModel(SequenceByteWriter *byte_writer) override;

  static TableTimeSeries *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index,
                                    bool target_int);

 private:
  std::vector<const AttrInterpreter *> predictor_interpreter_;
  bool target_int_;
  double bin_size_;
  double model_cost_;
  DynamicList<TimeSeriesStats> dynamic_list_;
  std::vector<size_t> dynamic_list_index_;
  TimeSeriesSquID squid_;

  void GetDynamicListIndex(const AttrVector &tuple);

  int GetModelDescriptionLength() const override;
};

class TableTimeSeriesCreator : public ModelCreator {
 public:
  SquIDModel *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index) override;
  SquIDModel *CreateModel(const std::vector<int> &attr_type,
                          const std::vector<size_t> &predictor_list, size_t target_var,
                          double err) override;

 private:
  const size_t kMaxTableSize = 1000;
};
}  // namespace db_compress

#endif  // DB_COMPRESS_TIMESERIES_MODEL_H
