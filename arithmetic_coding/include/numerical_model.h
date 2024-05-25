// The numerical SquID and SquIDModel header

#ifndef NUMERICAL_MODEL_H
#define NUMERICAL_MODEL_H

#include <vector>

#include "base.h"
#include "model.h"
#include "utility.h"

namespace db_compress {

const int kNumBranch1stLayer = 256;
const int kNumSigma = 6;

/**
 * Statistic of one specific attribute. It is a histogram except the most
 * left and left, which are exponential. The exponential distribution is
 * necessary for correctness of squish; since most numerical values are located
 * in the histogram,squish is efficient in most cases.
 */
class NumericalStats {
 public:
  double median_;
  double mean_abs_dev_;
  uint64_t step_;
  int num_bits_last_layer_, weight_branch_last_layer_, num_layers_;
  double minimum_, maximum_;
  int count_;

  std::vector<unsigned> cdf_1st_layer_;

  /**
   * Create a new Numerical statistic. Squish is designed to run in an online
   * fashion, any branch is possible to be selected in the future, so the
   * initial value of histogram is 1.
   */
  NumericalStats()
      : histogram_(kNumBranch1stLayer, 1),
        values_(kNumEstSample),
        num_layers_(0),
        count_(0),
        sum_abs_dev_(0),
        histogram_structure_estimated_(false) {}

  /**
   * Set bin size.
   *
   * @param bin_size bin size, it is user specified
   */
  void SetBinSize(double bin_size) { bin_size_ = bin_size; }

  /**
   * Push a value into the histogram.
   *
   * @param value attribute value, a sample
   */
  void PushValue(double value);

  /**
   * End of learning from sample.
   */
  void End();

 private:
  double bin_size_;
  double sum_abs_dev_;
  bool histogram_structure_estimated_;
  std::vector<double> values_;
  std::vector<unsigned> histogram_;
  double length_branch_1st_layer_;

  void InitHistogramStructure();
};

/**
 * The decision tree for a numerical attribute. First, it finds the value
 * generally (bin index), then applies a binary search within this bin to locate
 * the corresponding branch.
 */
class NumericalSquID : public SquID {
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
  AttrValue &GetResultAttr();

  /**
   * Generate probability intervals for given attr_value based on learned structure.
   * A prob_intervals_index is used to indicate the size of existed probability intervals, to
   * avoid the utilization of 'push_back' and unnecessary memory allocation.
   * That is, we allocate memory in the very beginning, once prob_intervals_index is larger
   * than a threshold, delayed coding is performed and flush probability
   * intervals.
   *
   * @param[out] prob_intervals generated probability intervals are added here
   * @param[out] prob_intervals_index prob_intervals_index of probability intervals
   * @param attr_value the attr_value to be encoded
   */
  void GetProbIntervals(std::vector<ProbInterval> &prob_intervals, int &prob_intervals_index,
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
  double mean_, dev_, minimum_, maximum_;
  uint64_t step_;

  int num_bits_last_layer_, weight_branch_last_layer_, num_layer_;
  uint64_t size_branch_1st_layer_;

  std::vector<unsigned> *cdf_base16_;

  // l_, r_, mid_: index of bin.
  int64_t l_, r_, mid_;

  // whether left boundary and right boundary can be determined.
  bool l_inf_, r_inf_;
  int64_t hist_left_boundary_;

  bool target_int_;
  double bin_size_;

  AttrValue attr_;

  void SetLeft(int64_t l) {
    l_ = l;
    l_inf_ = false;
  }
  void SetRight(int64_t r) {
    r_ = r;
    r_inf_ = false;
  }

  /**
   * This function is to generate branch of uniform parts. Find a bin which occupies the given
   * value, and return it index relative to the l_.
   *
   * @param value a real number
   * @return bin index relative to l_.
   */
  int64_t GetRelativeBinIndex(double value) const;

  // helper function for GetProbIntervals
  bool HasNextBranch() const;

  // compression functions for histogram and exponential part
  void GetHistogramProbIntervals(std::vector<ProbInterval> &prob_intervals,
                                 int &prob_intervals_index, double value);
  void GetExpProbIntervals(std::vector<ProbInterval> &prob_intervals, int &prob_intervals_index,
                           double value);

  // decompression functions for histogram and exponential part
  void HistogramDecompress(Decoder *decoder, ByteReader *byte_reader);
  void ExpDecompress(Decoder *decoder, ByteReader *byte_reader);
};

/**
 * It is a squid model for numerical attribute.
 */
class TableNumerical : public SquIDModel {
 public:
  TableNumerical(const std::vector<int> &attr_type, const std::vector<size_t> &predictor_list, size_t target_var,
                 double err, bool target_int);
  NumericalSquID *GetSquID(const AttrVector &tuple);
  int GetModelCost() const override { return model_cost_; }
  void FeedTuple(const AttrVector &tuple) override;
  void EndOfData() override;
  int GetModelDescriptionLength() const override;
  void WriteModel(SequenceByteWriter *byte_writer) const override;
  static SquIDModel *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index,
                               bool target_int);

 private:
  std::vector<const AttrInterpreter *> predictor_interpreter_;
  bool target_int_;
  double bin_size_;
  double model_cost_;
  DynamicList<NumericalStats> dynamic_list_;
  std::vector<size_t> dynamic_list_index_;
  NumericalSquID squid_;

  void GetDynamicListIndex(const AttrVector &tuple);
};

class TableNumericalRealCreator : public ModelCreator {
 public:
  SquIDModel *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index) override;
  SquIDModel *CreateModel(const std::vector<int> &attr_type, const std::vector<size_t> &predictors,
                          size_t target_var, double err) override;

 private:
  const size_t kMaxTableSize = 1000;
};

class TableNumericalIntCreator : public ModelCreator {
 public:
  SquIDModel *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index) override;
  SquIDModel *CreateModel(const std::vector<int> &attr_type, const std::vector<size_t> &predictors,
                          size_t target_var, double err) override;

 private:
  const size_t kMaxTableSize = 1000;
};

}  // namespace db_compress

#endif
