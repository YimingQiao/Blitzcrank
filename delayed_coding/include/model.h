/**
 * @file model.h
 * @brief This header file defines SquID interface and several related classes.
 */

#ifndef MODEL_H
#define MODEL_H

#include <functional>
#include <unordered_map>
#include <vector>

#include "base.h"
#include "data_io.h"
#include "utility.h"

namespace db_compress {

class Decoder;

/**
 * Decoder Class is initialized with two ProbIntervals and one SquID instance,
 * it reads in bits from input stream, determines the next branch until reaching
 * a leaf attr_record_ of the SquID, and then emits the result and two reduced
 * ProbIntervals. It preserves one probability interval for decompression [a,
 * b].
 */
class Decoder {
 public:
  Decoder() = default;

  /**
   * Initialize probability interval as [0, 1]
   */
  inline void InitProbInterval() {
    num_ = 0;
    den_ = 1;
    virtual_available_ = false;
    num_interval_ = 0;
  }

  /**
   * Return how many block has been decoded.
   *
   * @return current block index
   */
  inline int CurBlockSize() const { return num_interval_; }

  /**
   * Update current probability interval with denominator and numerator by
   * numerator = numerator * denominator + numerator, denominator = denominator
   * * denominator. Every time numerator > 2^48, two virtual bytes are
   * generated.
   *
   * @param denominator denominator of input fraction
   * @param numerator numerator of input fraction
   */
  inline void Update(unsigned denominator, unsigned numerator) {
    num_ = num_ * denominator + numerator;
    den_ = den_ * denominator;

    if ((den_ >> kDelayedCoding) > 0) {
      virtual_available_ = true;
      virtual_16bits_ = static_cast<uint16_t>(num_);
      num_ >>= 16;
      den_ >>= 16;
    }
  }

  /**
   * Read two bytes from virtual bytes buffer or disks. If virtual bytes buffer
   * is not empty or has only one bytes, fetch two bytes from virtual bytes
   * buffer, or read bytes from disks.
   *
   * @param byte_reader byte reader is used to read bytes from disk
   * @return return two bytes.
   */
  inline unsigned Read16Bits(ByteReader *byte_reader) {
    ++num_interval_;
    if (!virtual_available_) return byte_reader->Read16BitFast();

    virtual_available_ = false;
    return virtual_16bits_;
  }

 private:
  uint64_t num_{0}, den_{1};
  uint16_t virtual_16bits_;
  bool virtual_available_{false};
  uint16_t num_interval_{0};
};

/**
 * The SquIDModel class represents the local conditional probability
 * distribution. The SquIDModel object can be used to generate Decoder object
 * which can be used to infer the result attribute value based on bit string
 * (decompressing). It can also be used to create ProbInterval object which can
 * be used for compressing.
 */
class SquIDModel {
 public:
  /**
   * Create a squid model.
   *
   * @param predictors attribute may depend on other attributes, and they are
   * called predictors
   * @param target_var index of target attribute
   */
  SquIDModel(const std::vector<size_t> &predictors, size_t target_var);

  SquIDModel() = default;

  /**
   * Deconstruct a squid model.
   */
  virtual ~SquIDModel() = 0;

  /**
   * Get an estimation of model cost, which is used in model selection process.
   *
   * @return return estimated model cost
   */
  virtual int GetModelCost() const = 0;

  /**
   * Feed attributes to model.
   *
   * @param attrs one sample, a collection of attributes
   */
  virtual void FeedAttrs(const AttrVector &attrs, int count) = 0;

  /**
   * End learning or compression.
   */
  virtual void EndOfData() {}

  /**
   * Get number of bits used to describe this model.
   *
   * @return model description length in bytes
   */
  virtual int GetModelDescriptionLength() const = 0;

  /**
   * Write model.
   *
   * @param byte_writer it is used to access disk
   */
  virtual void WriteModel(SequenceByteWriter *byte_writer) = 0;

  /**
   * Get predictors.
   *
   * @return return predictors.
   */
  const std::vector<size_t> &GetPredictorList() const { return predictor_list_; }

  /**
   * Get target attribute index.
   *
   * @return return target attribute index
   */
  size_t GetTargetVar() const { return target_var_; }

 protected:
  std::vector<size_t> predictor_list_;
  size_t predictor_list_size_;
  size_t target_var_;
};

inline SquIDModel::~SquIDModel() = default;

/**
 * The ModelCreator class is used to create SquIDModel object either from
 * compressed file or from scratch, the ModelCreator classes must be registered
 * to be applied during training process.
 */
class ModelCreator {
 public:
  virtual ~ModelCreator() = 0;
  /**
   * Read model from disks. Caller takes ownership.
   *
   * @param byte_reader it is used for disk access.
   * @param schema schema contains attributes types
   * @param index target attribute index
   * @return return squid model if read model successfully; or return nullptr
   */
  virtual SquIDModel *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index) = 0;
  /**
   * Create model. Caller takes ownership, return NULL if predictors don't match
   *
   * @param attr_type attribute type
   * @param predictor attribute may depend on other attributes, and they are
   * called predictors
   * @param index target attribute index
   * @param err error tolerance
   * @return return squid model if read model successfully; or return nullptr
   */
  virtual SquIDModel *CreateModel(const std::vector<int> &attr_type,
                                  const std::vector<size_t> &predictor, size_t index,
                                  double err) = 0;
};

inline ModelCreator::~ModelCreator() = default;

/**
 * The AttrInterpreter is used to allow nonstandard attribute values to be used
 * as predictors to predict other attributes, it translates any attribute value
 * to either categorical value or numerical value.
 */
class AttrInterpreter {
 public:
  virtual ~AttrInterpreter();
  virtual bool EnumInterpretable() const { return false; }
  virtual int EnumCap() const { return 0; }
  virtual size_t EnumInterpret(const AttrValue &attr) const { return 0; }
};

inline AttrInterpreter::~AttrInterpreter() = default;

/**
 * This function registers the ModelCreator, which can be used to create models.
 * Multiple ModelCreators can be associated with the same path_type_ number.
 * This function takes the ownership of ModelCreator object.
 *
 * @param attr_type attribute type
 * @param creator model squid creator
 */
void RegisterAttrModel(int attr_type, ModelCreator *creator);

/**
 * Get attribute model creator based on given attribute type.
 *
 * @param attr_type attribute type
 * @return attribute model creator
 */
ModelCreator *GetAttrModel(int attr_type);

/**
 * This function registers the AttrInterpreter, which can be used to interpret
 * attributes so that they can be used as predictors for other attributes.
 * In our implementation, each attribute type could have only one interpreter.
 * This function takes the ownership of ModelCreator object.
 *
 * @param attr_index attribute index
 * @param interpreter attribute interpreter, each attribute has an interpreter
 */
void RegisterAttrInterpreter(int attr_index, AttrInterpreter *interpreter);

/**
 * Get attribute model creator based on given attribute index
 *
 * @param attr_type attribute index
 * @return attribute interpreter
 */
const AttrInterpreter *GetAttrInterpreter(int attr_type);

/**
 * Given an attribute, capacity means the number of possible values. This
 * function gets capacity for each predictor.
 *
 * @param pred a collection of attributes
 * @return return capacity of each attribute
 */
std::vector<size_t> GetPredictorCap(const std::vector<size_t> &pred);

}  // namespace db_compress

#endif
