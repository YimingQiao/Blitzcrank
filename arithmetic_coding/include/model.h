/*
 * This header file defines SquID interface and several related classes.
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
 * The SquID class defines the interface of branching, which can be used in
 * encoding & decoding. This interface simplifies the process of defining models
 * for new attributes
 */
class SquID {
 public:
  ProbInterval prob_interval_;
  int branch_left_{}, branch_right_{}, branch_mid_{}, prob_right_{}, prob_pit_pib_ratio_{};

  virtual ~SquID() = 0;
};

inline SquID::~SquID() = default;

/**
 * Decoder Class is initialized with two ProbIntervals and one SquID instance,
 * it reads in bits from input stream, determines the next branch until reaching
 * a leaf node of the SquID, and then emits the result and two reduced
 * ProbIntervals.
 */
class Decoder {
 public:
  Decoder();
  void InitProbInterval() {
    pit_.left_prob_ = 0;
    pit_.right_prob_ = 65536;
    pib_.exp_ = 0;
    pib_.num_ = 0;
  }
  void UpdatePIt(ProbInterval prob_interval);
  ProbInterval &GetPIt() { return pit_; }
  UnitProbInterval &GetPIb() { return pib_; }

  /**
   * Since exp of PIb is greater than 40, PIb could be considered as a point,
   * and this function give the relative position of PIb in Pit.
   *
   * @return the relative position of PIb in Pit
   */
  int GetPIbPItRatio() const {
    // pib_.exp >= 40
    return 65536 - 65536 * ((pit_.right_prob_ << (pib_.exp_ - 16)) - (pib_.num_ + 0.5)) /
                       ((pit_.right_prob_ - pit_.left_prob_) << (pib_.exp_ - 16));
  }
  void FeedByte(unsigned char byte) { pib_.GoByte(byte); }

 private:
  ProbInterval pit_;
  UnitProbInterval pib_;

  std::vector<unsigned char> bytes_;
  int bytes_index_;
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
  virtual void FeedTuple(const AttrVector &tuple) {}

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
  virtual void WriteModel(SequenceByteWriter *byte_writer) const = 0;

  /**
   * Set creator index.
   *
   * @param index creator index
   */
  void SetCreatorIndex(unsigned char index) { creator_index_ = index; }

  /**
   * Return creator index.
   *
   * @return creator index
   */
  unsigned char GetCreatorIndex() const { return creator_index_; }

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

 private:
  unsigned char creator_index_;
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

  virtual double NumericInterpretable() const { return 0.0; }
  virtual double NumericInterpret(const AttrValue *attr) const { return 0; }
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
void RegisterAttrInterpreter(int attr_type, AttrInterpreter *interpreter);

/**
 * Get attribute model creator based on given attribute index
 *
 * @param attr_type attribute index
 * @return attribute interpreter
 */
const AttrInterpreter *GetAttrInterpreter(int attr_type);

std::vector<size_t> GetPredictorCap(const std::vector<size_t> &pred);

}  // namespace db_compress

#endif
