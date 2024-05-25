/**
 * The base header files that defines several basic structures
 */

#ifndef BASE_H
#define BASE_H

#include <iostream>
#include <string>
#include <utility>
#include <variant>
#include <vector>
#include <unordered_map>
#define kNonFullPassStopPoint 20000
// Numeric Model
#define kNumBranch 512
#define kNumEstSample 5000

namespace db_compress {
/**
 * AttrValue is a union of possible values type. It is defined as union instead
 * of virtual class to avoid function call.
 */
class AttrValue {
 public:
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

  // setter
  inline void Set(int enum_val) { value_ = enum_val; }
  inline void Set(double num_val) { value_ = num_val; }
  inline void Set(std::string str_val) { value_ = str_val; }
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

/**
 * Structure for probability values. We don't use float-point value, and thus
 * precision overflow is avoided. 32-bit integer is not enough.
 */
using Prob = int64_t;

/**
 * Structure used to represent any probability interval between [0, 1].
 */
struct ProbInterval {
  Prob left_prob_, right_prob_;

  ProbInterval() : left_prob_(0), right_prob_(65536) {}
  ProbInterval(const Prob &left_prob, const Prob &right_prob)
      : left_prob_(left_prob), right_prob_(right_prob) {}
};

/**
 * Structure used to represent unit probability interval (i.e., [n/2^k,
 * (n+1)/2^k])
 */
struct UnitProbInterval {
  int64_t num_;
  char exp_;

  constexpr UnitProbInterval(int num, char exp) : num_(num), exp_(exp) {}
  Prob Left() const {
    if (exp_ <= 16) return Prob(num_ << (16 - exp_));
    std::cout << "Error: overflow in UnitProbInterval::Left()" << std::endl;
    return -1;
  }
  Prob Right() const {
    if (exp_ <= 16) return Prob((num_ + 1) << (16 - exp_));
    std::cout << "Error: overflow in UnitProbInterval::Right()" << std::endl;
    return -1;
  }
  Prob Mid() const {
    if (exp_ <= 16) return Prob(((num_ << 1) + 1) << (16 - exp_ - 1));
    std::cout << "Error: overflow in UnitProbInterval::Mid()" << std::endl;
    return -1;
  }
  ProbInterval GetProbInterval() const { return {Left(), Right()}; }

  void GoLeft() {
    num_ <<= 1;
    ++exp_;
  }
  void GoRight() {
    num_ <<= 1;
    ++exp_;
    ++num_;
  }
  /**
   * Go to next level unit probability interval, if bit equals 0, go to the left
   * part; otherwise, go to the right part. For example, given unitProbInterval
   * [0, 1/2], after Go(0), it turns out [0, 1/4]; or [1/4, 1/2] if Go(1).
   *
   * @param bit bit equals to 0 if left part selected; otherwise bit equals to 1
   */
  void Go(bool bit) {
    num_ <<= 1;
    ++exp_;
    num_ += static_cast<int64_t>(bit);
  }
  void GoByte(unsigned char byte) {
    exp_ += 8;
    num_ = (num_ << 8) + static_cast<int>(byte);
  }
  bool Back() {
    const bool bit = num_ & 1;
    num_ >>= 1;
    --exp_;
    return bit;
  }

  /**
   * Check whether PIt is included by unit probability interval. Note that base
   * of PIt is 16.
   *
   * @param PIt probability interval
   * @return return true if PIt is included; otherwise false
   */
  bool IncludedBy(const ProbInterval &PIt) const {
    return (num_ << (40 - exp_)) >= (PIt.left_prob_ << 24) &&
           ((num_ + 1) << (40 - exp_)) <= (PIt.right_prob_ << 24);
  }
};

/**
 * The following functions are used to generate Prob.
 */
constexpr Prob GetZeroProb() { return Prob(0); }
constexpr Prob GetOneProb() { return Prob(65536); }
constexpr UnitProbInterval GetWholeProbInterval() { return {0, 0}; }

// --------------------------- Enum BiMap -----------------------------------
// BiMap for Enum
struct BiMap {
  std::vector<std::string> enums;
  std::unordered_map<std::string, int> enum2idx;
};
}  // namespace db_compress
#endif
