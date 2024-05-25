/**
 * @file numerical_model.h
 * @brief The numerical SquID and SquIDModel header
 */

#ifndef NUMERICAL_MODEL_H
#define NUMERICAL_MODEL_H

#include <vector>

#include "base.h"
#include "model.h"
#include "simple_prob_interval_pool.h"
#include "utility.h"

namespace db_compress {

/**
 * Statistic of one specific attribute. It is a histogram except the most
 * left and left, which are exponential. The exponential distribution is
 * necessary for correctness of squish; since most numerical values are located
 * in the histogram,squish is efficient in most cases.
 */
class NumericalStats {
 public:
  // distribution params
  double mid_est_{};
  int64_t branch_bins_est_{};
  double mean_abs_dev_{};
  std::vector<uint32_t> branch_weights_;

  // delayed coding params
  DelayedCodingParams coding_params_;

  // help
  bool is_estimated_ = false;
  int32_t v_count_ = 0;
  std::vector<double> values_;
  std::vector<uint32_t> v_freq_;

  int64_t minimum_ = 0;
  int64_t maximum_ = 0;
  double sum_abs_dev_ = 0;

  uint64_t step_;
  uint32_t num_layer_;
  uint32_t mask_last_layer_;
  uint32_t weight_branch_last_layer_;

  // setting
  double bin_size_ = 0;

  /**
   * Create a new Numerical statistic. Squish is designed to run in an online
   * fashion, any branch is possible to be selected in the future, so the
   * initial value of histogram is 1.
   */
  NumericalStats() : values_(kNumEstSample), v_freq_(kNumBranch, 1) {}

  /**
   * Set bin size.
   *
   * @param bin_size bin size, it is user specified
   */
  void SetBinSize(double bin_size) { bin_size_ = bin_size; }

  /**
   * Push a value into the histogram.
   *
   * @param value attribute value
   */
  void PushValue(double value);

  /**
   * End of learning from sample.
   */
  void End();

  /**
   * write it down.
   */
  void WriteStats(SequenceByteWriter *byte_writer) const;

  /**
   * Read stats from disk/
   */
  void ReadStats(ByteReader *byte_reader);

 private:
  void InitHistogramStructure();

  void Prepare();
};

/**
 * The decision tree for a numerical attribute. First, it finds the value
 * generally (bin index), then applies a binary search within this bin to locate
 * the corresponding branch.
 */
class NumericalSquID {
 public:
  /**
   * Create a new numerical squid, bin size and target attribute type should be
   * given.
   *
   * @param bin_size bin size is twice allowed error
   * @param target_int target_int is 1 if target is a integer; otherwise
   * target_int is 0 (double)
   */
  NumericalSquID(double bin_size, bool target_int);

  /**
   * Load numerical statistic. One squid may have many optional statistic.
   *
   * @param stats numerical statistic
   */
  void Init(NumericalStats &stats);

  /**
   * Get result of decompressed attribute value.
   *
   * @return decompressed attribute value
   */
  AttrValue &GetResultAttr(bool round);

  /**
   * Generate probability intervals for given attr_value based on learned structure.
   * A index is used to indicate the size of existed probability intervals, to
   * avoid the utilization of 'push_back' and unnecessary memory allocation.
   * That is, we allocate memory in the very beginning, once index is larger
   * than a threshold, delayed coding is performed and flush probability
   * intervals.
   *
   * @param[out] prob_intervals generated probability intervals are added here
   * @param[out] prob_intervals_index index of probability intervals
   * @param attr_value the attr_value to be encoded
   */
  void GetProbIntervals(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                        const AttrValue &attr_value);
  /**
   * Squid reads 16 bits from a byte reader and updates decompression states,
   * then it advances to the next layer of this decision tree. The result is
   * saved by class member variable choice_. Notice that fast decoding is
   * only used in the first layer.
   *
   * @param decoder it maintains the decompression process of delayed coding,
   * some variables are saved here.
   * @param byte_reader it reads bytes from compressed binary file
   */
  void Decompress(Decoder *decoder, ByteReader *byte_reader);

 private:
  double mean_, dev_;
  int64_t minimum_, maximum_;
  uint64_t step_;

  uint32_t mask_last_layer_;
  uint32_t weight_branch_last_layer_;
  uint32_t num_layer_;
  int64_t branch_bins_;

  // l_, r_, mid_: index of bin.
  int64_t l_, r_, mid_;

  // whether left boundary and right boundary can be determined.
  bool l_inf_, r_inf_;

  bool target_int_;
  double bin_size_;
  // precision_ = 10^(#decimal places), it is used to round number in GetResultAttr().
  int decimal_places_{0};

  AttrValue attr_;
  DelayedCodingParams *coding_params_;

  void SetLeft(int64_t l) {
    l_ = l;
    l_inf_ = false;
  }
  void SetRight(int64_t r) {
    r_ = r;
    r_inf_ = false;
  }
  void Reset() {
    l_inf_ = true;
    r_inf_ = true;
  }

  int64_t GetBinIndex(double value) const { return floor((value - mean_) / bin_size_); }

  // helper function for GetProbIntervals
  bool HasNextBranch() const;

  // compression functions for histogram and exponential part
  void GetHistogramProbIntervals(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                                 int64_t idx);

  void GetExpProbIntervals(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                           int64_t idx);

  // decompression functions for histogram and exponential part
  void HistogramDecompress(Decoder *decoder, ByteReader *byte_reader);

  void ExpDecompress(Decoder *decoder, ByteReader *byte_reader);
};

/**
 * It is a squid model for numerical attribute.
 */
class TableNumerical : public SquIDModel {
 public:
  bool target_int_;
  NumericalSquID base_squid_;

  /**
   * Create a simple numerical model.
   */
  explicit TableNumerical(bool target_int, double bin_size)
      : SquIDModel(std::vector<size_t>(), 0),
        predictor_interpreter_(0),
        dynamic_list_(std::vector<size_t>()),
        dynamic_list_index_(0),
        bin_size_(bin_size),
        squid_(bin_size, target_int),
        base_squid_(bin_size, target_int),
        target_int_(target_int),
        model_cost_(0) {
    dynamic_list_[0].SetBinSize(bin_size_);
  };

  /**
   * For simple numerical model. Feed values to simple numerical model.
   *
   * @param integer input integer
   */
  void FeedAttrs(const AttrValue &integer, int count);

  TableNumerical(const std::vector<int> &attr_type, const std::vector<size_t> &predictor_list,
                 size_t target_var, double bin_size, bool target_int);

  NumericalSquID *GetSquID(const AttrVector &tuple);

  int GetModelCost() const override { return static_cast<int>(model_cost_); }

  void FeedAttrs(const AttrVector &attrs, int count) override;

  void EndOfData() override;

  void WriteModel(SequenceByteWriter *byte_writer) override;

  static TableNumerical *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index,
                                   bool target_int);

  NumericalSquID *GetSquID();

 private:
  std::vector<const AttrInterpreter *> predictor_interpreter_;
  double bin_size_;
  double model_cost_;
  DynamicList<NumericalStats> dynamic_list_;
  std::vector<size_t> dynamic_list_index_;
  NumericalSquID squid_;

  void GetDynamicListIndex(const AttrVector &tuple);

  int GetModelDescriptionLength() const override;
};

class TableNumericalRealCreator : public ModelCreator {
 public:
  SquIDModel *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index) override;
  SquIDModel *CreateModel(const std::vector<int> &attr_type,
                          const std::vector<size_t> &predictor_list, size_t target_var,
                          double err) override;

 private:
  const size_t kMaxTableSize = 1000;
};

class TableNumericalIntCreator : public ModelCreator {
 public:
  SquIDModel *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index) override;
  SquIDModel *CreateModel(const std::vector<int> &attr_type,
                          const std::vector<size_t> &predictor_list, size_t target_var,
                          double err) override;

 private:
  const size_t kMaxTableSize = 1000;
};

}  // namespace db_compress

#endif
