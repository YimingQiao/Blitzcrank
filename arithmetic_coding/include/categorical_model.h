/*
 * The header file for categorical SquID and SquIDModel
 */

#ifndef CATEGORICAL_MODEL_H
#define CATEGORICAL_MODEL_H

#include <vector>

#include "base.h"
#include "model.h"
#include "utility.h"

namespace db_compress {
/**
 * Statistic of one specific categorical attribute, consists of a histogram and
 * delayed coding components.
 */
struct CategoricalStats {
  //! histogram
  std::vector<int> count_;
  //! histogram after normalization
  std::vector<Prob> prob_;
};

/**
 * Squid of categorical attribute is a simple one-layer decision tree.
 * CategoricalSquID has two main functions: 1) Transform a value into
 * corresponding probability intervals; 2) Given compressed bits, return a
 * decompressed attribute value.
 */
class CategoricalSquID : public SquID {
 public:
  /**
   * Initialize the squid for categorical attribute with a categorical statistic
   * table. Also, member variable choice_ needs initialization.
   *
   * @param prob_segs a statistic table for categorical table
   */
  inline void Init(std::vector<Prob> &prob_segs) {
    choice_ = -1;
    prob_segments_ = &prob_segs;
  }

  /**
   * Return the decompressed result value.
   *
   * @return a const pointer to attribute value
   */
  const AttrValue &GetResultAttr() {
    attr_.Set(choice_);
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
  void GetProbIntervals(std::vector<ProbInterval> &prob_intervals, int &prob_intervals_index,
                        const AttrValue &attr);

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
  std::vector<Prob> *prob_segments_;
  int choice_;
  AttrValue attr_;
};

/**
 * TableCategorical is utilized to determine which Categorical squid should be
 * produced.
 */
class TableCategorical : public SquIDModel {
 public:
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
   * @param[unused] err tolerance error, it is useless for lossless compression
   */
  TableCategorical(const std::vector<int> &attr_type, const std::vector<size_t> &predictor_list,
                   size_t target_var, double err);

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
   * Get model cost of current model, model cost is estimated from statistic
   * table.
   *
   * @return the model cost of model
   */
  int GetModelCost() const override { return model_cost_; }

  /**
   * In the learning stage, model needs to scan datasets to generated statistic
   * tables. This function feeds one sample attributes to model.
   *
   * @param tuple attributes value of one sample
   */
  void FeedTuple(const AttrVector &tuple) override;

  /**
   * End of Learning, update model cost, prepare for delayed coding.
   */
  void EndOfData() override;

  /**
   * Get size of model description length in bytes, i.e. the overhead of a
   * model.
   *
   * @return length of model description
   */
  int GetModelDescriptionLength() const override;

  /**
   * Write down parameters of a model, including config information
   * and statistic tables in the dynamic list.
   *
   * @param byte_writer is used to write down bytes in sequence
   */
  void WriteModel(SequenceByteWriter *byte_writer) const override;

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
  static SquIDModel *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index);

 private:
  // member variables
  std::vector<const AttrInterpreter *> predictor_interpreter_;
  size_t target_range_;
  size_t cell_size_;
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
  SquIDModel *CreateModel(const std::vector<int> &attr_type, const std::vector<size_t> &predictors, size_t index,
                          double err) override;

 private:
  // maximum size of dynamic list. Too many statistic table leads to very large
  // overhead.
  const size_t kMaxTableSize = 1000;
};

}  // namespace db_compress

#endif
