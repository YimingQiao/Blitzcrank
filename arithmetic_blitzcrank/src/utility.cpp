#include "../include/utility.h"

#include <climits>
#include <iostream>
#include <sstream>
#include <fstream>

#include "../include/base.h"

namespace db_compress {

/*
 * The quantization follows two steps:
 * 1. Mark all categories consists of less than 1/2^base portion of total count
 * 2. Distribute the remaining probability proportionally
 */
void Quantization(std::vector<Prob> *prob, const std::vector<int> &cnt, int base) {
  std::vector<int> prob_cnt(cnt.size(), 0);

  int sum = 0;
  for (size_t i = 0; i < cnt.size(); ++i) sum += cnt[i];

  int64_t total = (1 << base);
  // We have to make sure that every prob_ has num < 2^base
  if (cnt[cnt.size() - 1] == 0) --total;
  while (1) {
    bool found = false;
    for (size_t i = 0; i < cnt.size(); ++i)
      if (cnt[i] > 0 && cnt[i] * total < sum && prob_cnt[i] == 0) {
        prob_cnt[i] = 1;
        --total;
        sum -= cnt[i];
      }
    if (!found) break;
  }

  for (size_t i = 0; i < cnt.size(); ++i)
    if (cnt[i] > 0 && prob_cnt[i] == 0) {
      int share = cnt[i] * total / sum;
      prob_cnt[i] = share;
      total -= share;
      sum -= cnt[i];
    }

  sum = 0;
  prob->resize(prob_cnt.size() - 1);
  for (size_t i = 0; i < prob_cnt.size() - 1; ++i) {
    sum += prob_cnt[i];
    if (sum) prob->at(i) = GetProb(sum, base);
  }
}

void GetPIProduct(ProbInterval &left, const ProbInterval &right,
                  std::vector<unsigned char> *emit_bytes, int &emit_byte_index) {
  const int64_t range = left.right_prob_ - left.left_prob_;
  uint64_t product_l_num = (left.left_prob_ << 16) + range * right.left_prob_;
  uint64_t product_r_num = (left.left_prob_ << 16) + range * right.right_prob_;

  if (emit_bytes != nullptr) {
    int64_t l_num = product_l_num >> 16;
    int64_t r_num = product_r_num >> 16;

    if (r_num > l_num + 1) {
      left.left_prob_ = l_num + 1 - (product_l_num == (l_num << 16));
      left.right_prob_ = r_num;
    } else if (l_num + 1 == r_num) {
      if ((r_num << 16) - product_l_num >= product_r_num - (r_num << 16)) {
        left.right_prob_ = 65536;
        left.left_prob_ = product_l_num - (l_num << 16);
      } else {
        left.left_prob_ = 0;
        left.right_prob_ = product_r_num - (r_num << 16);
        ++l_num;
      }
      EmitBytes(emit_bytes, emit_byte_index, l_num, 16);
    } else if (r_num < l_num + 1) {
      left.left_prob_ = product_l_num - (l_num << 16);
      left.right_prob_ = product_r_num - (l_num << 16);
      EmitBytes(emit_bytes, emit_byte_index, l_num, 16);
    }
  }
}

void inline EmitBytes(std::vector<unsigned char> *emit_bytes, int &emit_byte_index, uint64_t bytes,
                      int len) {
  for (int i = len - 8; i >= 0; i = i - 8) {
    (*emit_bytes)[emit_byte_index++] = static_cast<unsigned char>((bytes & 0xff << i) >> i);
  }
}

ProbInterval ReducePIProduct(const std::vector<ProbInterval> &vec,
                             std::vector<unsigned char> *emit_bytes, int &emit_byte_index,
                             int vec_size) {
  if (vec_size == 0) {
    return {GetZeroProb(), GetOneProb()};
  }
  ProbInterval ret = vec[0];
  for (size_t i = 1; i < vec_size; ++i) {
    GetPIProduct(ret, vec[i], emit_bytes, emit_byte_index);
  }
  return ret;
}

// the base of ratio is always 16, so the return Prob has base PI.exp + 16.
Prob GetPIRatioPoint(const ProbInterval &PI, const Prob &ratio) {
  return (PI.right_prob_ - PI.left_prob_) * ratio + (PI.left_prob_ << 16);
}

void ReducePI(UnitProbInterval *PI, const std::vector<unsigned char> &bytes, int bytes_index) {
  for (size_t i = 0; i < bytes_index; ++i) {
    int64_t byte = bytes[i];
    // PI->l = (PI->l - byte/256) * 256, PI->r = (PI->r - byte/256) *
    // 256
    PI->exp_ -= 8;
    PI->num_ -= byte << PI->exp_;
  }
}

void QuantizationToFloat32Bit(double *val) {
  unsigned char bytes[4];
  ConvertSinglePrecision(*val, bytes);
  *val = ConvertSinglePrecision(bytes);
}

void ConvertSinglePrecision(double val, unsigned char bytes[4]) {
  bytes[0] = bytes[1] = bytes[2] = bytes[3] = 0;
  if (val == 0) return;
  if (val < 0) {
    val = -val;
    bytes[0] |= 128;
  }
  int exponent = 127;
  while (val < 1) {
    val *= 2;
    --exponent;
  }
  while (val >= 2) {
    val /= 2;
    ++exponent;
  }
  bytes[0] |= ((static_cast<unsigned char>(exponent) >> 1) & 127);
  bytes[1] |= ((static_cast<unsigned char>(exponent) & 1) << 7);
  unsigned int fraction = static_cast<int>(floor(val * (1 << 23) + 0.5));
  bytes[1] |= ((fraction >> 16) & 0x7f);
  bytes[2] = ((fraction >> 8) & 0xff);
  bytes[3] = (fraction & 0xff);
}

double ConvertSinglePrecision(unsigned char bytes[4]) {
  int exponent = (bytes[0] & 0x7f) * 2 + ((bytes[1] >> 7) & 1) - 127;
  int val = ((bytes[1] & 0x7f) << 16) + (bytes[2] << 8) + bytes[3];
  if (exponent == -127 && val == 0) return 0;
  double ret = (val + (1 << 23)) * pow(2, exponent - 23);
  if ((bytes[0] & 0x80) > 0)
    return -ret;
  else
    return ret;
}

void BitString::StrCat(unsigned int byte) {
  int index = length_ / 32;
  int offset = length_ & 31;
  if (offset == 0 && index >= 1024) bits_.push_back(0);
  if (offset <= 24) {
    bits_[index] |= (byte << (24 - offset));
    length_ += 8;
  } else {
    StrCat(byte, 8);
  }
}

void BitString::StrCat(unsigned bits, int len) {
  int index = length_ / 32;
  int offset = length_ & 31;
  bits &= (1 << len) - 1;
  if (offset == 0 && index >= 1024) bits_.push_back(0);
  if (offset + len <= 32) {
    bits_[index] |= (bits << (32 - len - offset));
    length_ += len;
  } else {
    int extra_bits = len + offset - 32;
    bits_[index] |= (bits >> extra_bits);
    length_ += 32 - offset;
    StrCat(bits, extra_bits);
  }
}

void BitString::StrCat(const BitString &cat) {
  for (size_t i = 0; i < cat.length_ / 32; ++i) {
    StrCat(cat.bits_[i], 32);
  }
  if ((cat.length_ & 31) > 0) {
    int len = cat.length_ & 31;
    StrCat(cat.bits_[cat.length_ / 32] >> (32 - len), len);
  }
}

void GetBitStringFromProbInterval(BitString *str, const ProbInterval &prob) {
  str->Clear();
  UnitProbInterval PI = GetWholeProbInterval();
  while (PI.Left() < prob.left_prob_ || PI.Right() > prob.right_prob_) {
    int offset = str->length_ & 31;
    if (offset == 0 && str->length_ / 32 >= 1024) str->bits_.push_back(0);
    unsigned &last = str->bits_[str->length_ / 32];

    if (PI.Mid() - prob.left_prob_ > prob.right_prob_ - PI.Mid()) {
      PI.GoLeft();
    } else {
      last |= (1 << (31 - offset));
      PI.GoRight();
    }
    str->length_++;
  }
}
unsigned RoundToNearest2Exp(unsigned val) {
  unsigned m = 0;
  while (val >> 1) {
    m++;
    val >>= 1;
  }

  return m;
}

void Write(const std::vector<BiMap> &data) {
  std::ofstream f("enum.dat");

  for (const BiMap &map : data) {
    for (const std::string &s : map.enums) {
      f << s << ",";
    }
    f << "\n";
  }
}
void Read(std::vector<BiMap> &data) {
  std::ifstream f("enum.dat");
  std::string s;
  for (BiMap &map : data) {
    std::getline(f, s);
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
      map.enums.push_back(item);
      map.enum2idx[item] = map.enums.size() - 1;
    }
  }
}
}  // namespace db_compress
