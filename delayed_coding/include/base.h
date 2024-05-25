/**
 * @file base.h
 * @brief The base header files that defines several basic structures
 */

#ifndef BASE_H
#define BASE_H

#include <cassert>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

namespace db_compress {
// ---------------------------- Hyper Parameters ----------------------------

// Blitzcrank
#define DEBUG 0
#define kNonFullPassStopPoint 20000
#define kIntervalSize 10000

// Delayed Coding. For random access, we recommend 16; for best compression ratio, we recommend 24.
#define kDelayedCoding 24
#define kBlockSize 1

// String Model
#define kLocalDictSize 1
#define kMarkovModel 1

// Numeric Model
#define kNumBranch 512
#define kNumEstSample 5000

// ---------------------- Structural Model -------------------------------------
/**
 * AttrValue is a union of possible values type. It is defined as union instead
 * of virtual class to avoid function call.
 */
struct AttrValue {
  std::variant<int, double, std::string> value_;

  // constructor
  AttrValue() : value_(0) {}
  explicit AttrValue(int enum_val) : value_(enum_val) {}
  explicit AttrValue(double num_val) : value_(num_val) {}
  explicit AttrValue(std::string str_val) : value_(str_val) {}

  // getter
  inline int Int() const { return std::get<int>(value_); }
  inline double Double() const { return std::get<double>(value_); }
  inline std::string &String() { return std::get<std::string>(value_); }
  inline const std::string &String() const { return std::get<std::string>(value_); }
};

/**
 * AttrVector structure contains fixed number of pointers to attributes,
 * the attribute types can be determined by Schema class. Note that
 * AttrVector structures do not own the attribute value objects.
 */
struct AttrVector {
  explicit AttrVector(int cols) : attr_(cols) {}

  std::vector<AttrValue> attr_;
};

/**
 * Schema structure contains the attribute types information, which are used for
 * type casting when we need to interpret the values in each tuple.
 */
struct Schema {
  std::vector<int> attr_type_; /**< schema attributes */

  Schema() = default;
  explicit Schema(std::vector<int> attr_type_vec) : attr_type_(std::move(attr_type_vec)) {}
  size_t size() const { return attr_type_.size(); }
};

// --------------------------------- Coding ------------------------------
/**
 * Structure for probability values. We don't use float-point value, and thus
 * precision overflow is avoided.
 */
using Prob = int32_t;

/**
 * Structure used to represent any probability interval between [0, 1].
 */
struct ProbInterval {
  Prob left_prob_, right_prob_;

  ProbInterval() : left_prob_(0), right_prob_(65536) {}
  ProbInterval(const Prob &left_prob, const Prob &right_prob)
      : left_prob_(left_prob), right_prob_(right_prob) {}

  bool operator==(const ProbInterval &other) const {
    return left_prob_ == other.left_prob_ && right_prob_ == other.right_prob_;
  }
};

/**
 * Structure used to represent each segments_ of one branch. Each branch has
 * several segments_, weight is the sum of all segments_ (length).
 */
struct Branch {
  std::vector<ProbInterval> segments_; /**< each branch has several segments_ */
  unsigned total_weights_;             /**< weights of each branch*/

  Branch() = default;

  Branch(std::vector<ProbInterval> segments, unsigned total_weights)
      : segments_(std::move(segments)), total_weights_(total_weights) {}

  Branch(unsigned total_weight, ProbInterval PI) : total_weights_(total_weight) {
    segments_.push_back(PI);
  }
};

/**
 * To apply delayed coding, these params are needed.
 */
struct DelayedCodingParams {
  std::vector<Branch> branches_;
  std::vector<std::pair<int, int>> segment_left_branches_;
  std::vector<std::pair<int, int>> segment_right_branches_;
  // Numerator help is used to accelerate finding numerator when decoding.
  // numerator = 16 bits (we use 16 bits to represent a probability interval) -
  // numerator_helper_[segment index]. Please refer to InitDelayedCodingParams()
  // and TableCategorical::Decompress() for detail.
  std::vector<int> numerator_helper_;
  int num_represent_bits_;

  void Clear() {
    branches_.clear();
    segment_left_branches_.clear();
    segment_right_branches_.clear();
    numerator_helper_.clear();
    num_represent_bits_ = 0;
  }
};

// --------------------------- Enum BiMap -----------------------------------
// BiMap for Enum
struct BiMap {
  std::vector<std::string> enums;
  std::unordered_map<std::string, int> enum2idx;
};
}  // namespace db_compress
#endif
