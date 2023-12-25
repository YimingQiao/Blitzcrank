/**
 * @file categorical_model.h
 * @brief The header file for categorical SquID and SquIDModel
 */

#ifndef CATEGORICAL_MODEL_H
#define CATEGORICAL_MODEL_H

#include <map>
#include <vector>

#include "base.h"
#include "model.h"
#include "simple_prob_interval_pool.h"
#include "utility.h"

namespace db_compress {
/**
 * Rare Branch handling.
 */
struct ZeroBranchHandler {
  // bi-directed map
  std::vector<int> idx2branch_;
  std::map<int, int> branch2idx_;

  // bi-directed map size
  int map_size_;

  int weight_;

  void Init(std::vector<unsigned> weights);
};

/**
 * Statistic of one specific categorical attribute, consists of a histogram and
 * delayed coding components.
 */
struct CategoricalStats {
  // histogram
  std::vector<int> count_;
  // histogram after normalization
  std::vector<unsigned int> weight_;

  // if attribute only has one possible value, that is saved by only_value_;
  // otherwise, only_value_ = 65535
  unsigned only_value_{65535};

  DelayedCodingParams coding_params_;

  // rare branch handle
  ZeroBranchHandler rare_branch_handler_;
};

/**
 * Squid of categorical attribute is a simple one-layer decision tree.
 * CategoricalSquID has two main functions: 1) Transform a value into
 * corresponding probability intervals; 2) Given compressed bits, return a
 * decompressed attribute value.
 */
class CategoricalSquID {
 public:
  /**
   * Initialize the squid for categorical attribute with a categorical statistic
   * table. Also, member variable choice_ needs initialization.
   *
   * @param stats a statistic table for categorical table
   */
  inline void Init(CategoricalStats &stats) {
    choice_ = -1;
    coding_params_ = &stats.coding_params_;
    rare_branch_handler_ = &stats.rare_branch_handler_;
  }

  /**
   * Return the decompressed result value.
   *
   * @return a const pointer to attribute value
   */
  const AttrValue &GetResultAttr() {
    attr_.value_ = choice_;
    return attr_;
  }

  /**
   * Generate probability intervals for given value based on learned structure.
   * A index is used to indicate the size of existed probability intervals, to
   * avoid the utilization of 'push_back' and unnecessary memory allocation.
   * That is, we allocate memory in the very beginning, once index is larger
   * than a threshold, delayed coding is performed and empty probability
   * intervals.
   *
   * @param[out] prob_intervals generated probability intervals are added here
   * @param[out] prob_intervals_index index of probability intervals
   * @param value the value to be encoded
   */
  void GetProbIntervals(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                        const AttrValue &value) const;

  /**
   * Squid reads 16 bits from a byte reader and updates decompression states,
   * then it advances to the next layer of this decision tree. The result is
   * saved by class member variable choice_.
   *
   * @param decoder it maintains the decompression process of delayed coding,
   * some variables are saved here.
   * @param byte_reader it reads bytes from compressed binary file
   */
  void Decompress(Decoder *decoder, ByteReader *byte_reader);

 private:
  // member variables
  int choice_;
  AttrValue attr_;
  DelayedCodingParams *coding_params_;
  ZeroBranchHandler *rare_branch_handler_;
};

/**
 * TableCategorical is utilized to determine which Categorical squid should be
 * produced.
 */
class TableCategorical : public SquIDModel {
 public:
  CategoricalSquID base_squid_;

  /**
   * Create a new TableCategorical from given dependency between current
   * categorical attribute (target attribute) and others. The ordering of all
   * attributes is determined, then target_var can be used to find the
   * categorical attribute for this squid.
   *
   * @param attr_type attribute type of all attributes in a dataset
   * @param predictor_list current attribute is dependent on all attributes in
   * predictor_list
   * @param target_var a index of current attribute.
   */
  TableCategorical(const std::vector<int> &attr_type, const std::vector<size_t> &predictor_list,
                   size_t target_var);

  /**
   * For simple categorical attribute.
   *
   * Creates a new simple TableCategorical
   */
  TableCategorical()
      : SquIDModel(std::vector<size_t>(), 0),
        predictor_interpreter_(0),
        dynamic_list_(std::vector<size_t>()),
        dynamic_list_index_(0),
        target_range_(0) {}

  /**
   * For simple categorical attribute.
   *
   * Return the single value of target squid. In some case, attribute has only
   * one possible value, we call it single value here.
   *
   * @return the single value of default statistic table, whose index in dynamic
   * list is 0.
   */
  int GetSimpleSquidValue() const { return dynamic_list_[0].only_value_; }

  /**
   * Given a tuple, the value of target attribute and all dependent attributes
   * are extracted. A statistic table is selected according to predictors, and
   * squid is initialized with it.
   *
   * @param tuple values of all attributes in a dataset
   * @return an initialized squid
   */
  CategoricalSquID *GetSquID(const AttrVector &tuple);

  /**
   * For simple categorical attribute.
   *
   * Return a default squid. This function is useful when target attribute is
   * not dependent on any other attributes, since there is only one statistic
   * table in this case.
   *
   * @return an initialized squid
   */
  CategoricalSquID *GetSquID() {
    squid_.Init(dynamic_list_[0]);
    return &squid_;
  }

  /**
   * Get model cost of current model, model cost is estimated from statistic
   * table.
   *
   * @return the model cost of model
   */
  int GetModelCost() const override { return static_cast<int>(model_cost_); }

  /**
   * In the learning stage, model needs to scan datasets to generated statistic
   * tables. This function feeds one sample attributes to model.
   *
   * @param attrs attributes value of one sample
   */
  void FeedAttrs(const AttrVector &attrs, int count) override;

  /**
   * For simple categorical attribute.
   *
   * Feed values to simple TableCategorical
   *
   * @param attr_val
   */
  void FeedAttrs(const AttrValue &attr_val, int count);

  /**
   * End of Learning, update model cost, prepare for delayed coding.
   */
  void EndOfData() override;

  /**
   * Write down parameters of a model, including config information
   * and statistic tables in the dynamic list.
   *
   * @param byte_writer is used to write down bytes in sequence
   */
  void WriteModel(SequenceByteWriter *byte_writer) override;

  /**
   * Read parameters of a model, it is the inverse process of function
   * WriteModel().
   *
   * @param byte_reader is used to read bytes in sequence
   * @param schema can help determine the number of attributes, it is helpful
   * when generating attributes ordering.
   * @param index index of target attribute in attributes ordering.
   * @return a learned squid model for target attributes.
   */
  static TableCategorical *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index);

  /**
   * Read simple categorical model
   *
   * @param byte_reader read bytes in sequence
   * @return a learned simple squid model
   */
  static SquIDModel *ReadModel(ByteReader *byte_reader);

  /**
   * Get size of model description length in bytes, i.e. the overhead of a
   * model.
   *
   * @return length of model description
   */
  int GetModelDescriptionLength() const override;

 private:
  // member variables
  std::vector<const AttrInterpreter *> predictor_interpreter_;
  size_t target_range_;
  double model_cost_;

  // this squid can be initialized with different statistic table, it depends on
  // the values of predictors.
  CategoricalSquID squid_;

  // Each vector consists of k-1 probability segment boundary
  DynamicList<CategoricalStats> dynamic_list_;
  std::vector<size_t> dynamic_list_index_;

  void GetDynamicListIndex(const AttrVector &tuple);
};

/**
 * Extract categorical squid models from disk. Within a model creator, some
 * check can be performed, compared to call model constructor function directly.
 */
class TableCategoricalCreator : public ModelCreator {
 public:
  /**
   * Read model from disk. This function is called in decompression stage.
   *
   * @param byte_reader is used to read bytes in sequence
   * @param schema describes the structure of dataset
   * @param index index of target attribute
   * @return a categorical model
   */
  SquIDModel *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index) override;

  /**
   * In learning stage, this function is called to create models.
   *
   * @param attr_type all attribute types of dataset
   * @param predictor attributes which target attribute depends on.
   * @param index inde xof target attribute
   * @param err error tolerance for target attribute.
   * @return a categorical model
   */
  SquIDModel *CreateModel(const std::vector<int> &attr_type, const std::vector<size_t> &predictor,
                          size_t index, double err) override;

 private:
  // maximum size of dynamic list. Too many statistic table leads to very large
  // overhead.
  const size_t kMaxTableSize = 1000;
};

}  // namespace db_compress

#endif
