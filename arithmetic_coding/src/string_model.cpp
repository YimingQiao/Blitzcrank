#include "../include/string_model.h"

#include <string>
#include <vector>

#include "../include/base.h"
#include "../include/model.h"
#include "../include/utility.h"

namespace db_compress {

inline void StringSquID::Init(const std::vector<Prob> *char_prob,
                              const std::vector<Prob> *len_prob) {
  char_prob_ = char_prob;
  len_prob_ = len_prob;
  is_end_ = false;
  word_length_ = -1, attr_.Set("");
}

int StringSquID::GetNextBranch(const AttrValue &attr) {
  const std::string &str = attr.String();
  if (word_length_ == -1)
    return (str.length() >= 63 ? 63 : str.length());
  else
    return (unsigned char)str[attr_.String().length()];
}

void StringSquID::ChooseNextBranch(int branch) {
  if (word_length_ == -1) {
    word_length_ = (branch == 63 ? -2 : branch);
    if (word_length_ == 0) is_end_ = true;
  } else {
    attr_.String().push_back((char)branch);
    if (branch == 0 || (int)attr_.String().length() == word_length_) is_end_ = true;
  }
}

ProbInterval &StringSquID::GenerateNextBranch(int branch) {
  std::vector<Prob> prob_segs_;
  if (word_length_ == -1)
    prob_segs_ = *len_prob_;
  else if (attr_.String().length() == 0)
    prob_segs_ = *char_prob_;

  Prob l = GetZeroProb(), r = GetOneProb();
  if (branch > 0) l = prob_segs_[branch - 1];
  if (branch < (int)prob_segs_.size()) r = prob_segs_[branch];
  prob_interval_.left_prob_ = l;
  prob_interval_.right_prob_ = r;

  return prob_interval_;
}

int StringSquID::GenerateNextBranchDecoder(int branch) {
  std::vector<Prob> prob_segs_;
  if (word_length_ == -1)
    prob_segs_ = *len_prob_;
  else if (attr_.String().length() == 0)
    prob_segs_ = *char_prob_;

  Prob l = GetZeroProb(), r = GetOneProb();
  if (branch < (int)prob_segs_.size()) r = prob_segs_[branch];

  return r;
}

void StringSquID::Decompress(Decoder *decoder, ByteReader *byte_reader) {
  while (HasNextBranch()) {
    branch_left_ = 0;
    branch_right_ = GetBranchSize();

    if (decoder->GetPIb().exp_ < 40) decoder->FeedByte(byte_reader->ReadByte());
    prob_pit_pib_ratio_ = decoder->GetPIbPItRatio();
    while (branch_left_ != branch_right_) {
      branch_mid_ = (branch_left_ + branch_right_) / 2;
      prob_right_ = GenerateNextBranchDecoder(branch_mid_);
      if (prob_right_ > prob_pit_pib_ratio_)
        branch_right_ = branch_mid_;
      else
        branch_left_ = branch_mid_ + 1;
    }
    decoder->UpdatePIt(GenerateNextBranch(branch_left_));
    ChooseNextBranch(branch_left_);
    UnitProbInterval &PIb_ = decoder->GetPIb();
  }
}

StringModel::StringModel(size_t target_var)
    : SquIDModel(std::vector<size_t>(), target_var), char_count_(256), length_count_(64) {}

StringSquID *StringModel::GetSquID(const AttrVector &tuple) {
  squid_.Init(&char_prob_, &length_prob_);
  return &squid_;
}

void StringModel::FeedTuple(const AttrVector &tuple) {
  const AttrValue &attr = tuple.attr_[target_var_];
  const std::string &str = attr.String();
  for (size_t i = 0; i < str.length(); i++) char_count_[(unsigned char)str[i]]++;
  if (str.length() >= 63) {
    length_count_[63]++;
    char_count_[0]++;
  } else {
    length_count_[str.length()]++;
  }
}

void StringModel::EndOfData() {
  // Calculate the probability vector of characters
  Quantization(&char_prob_, char_count_, 16);
  char_count_.clear();
  // Calculate the probability vector of string lengths
  Quantization(&length_prob_, length_count_, 8);
  length_count_.clear();
}

void StringModel::GetProbInterval(std::vector<ProbInterval> &prob_intervals,
                                  const AttrVector &tuple, int &index) {
  StringSquID *squid = static_cast<StringSquID *>(GetSquID(tuple));
  squid->GetProbIntervals(prob_intervals, index, tuple.attr_[target_var_]);
}

int StringModel::GetModelCost() const {
  // Since we only have one type of string model, this number does not matter.
  return 0;
}

int StringModel::GetModelDescriptionLength() const { return 255 * 16 + 63 * 8; }

void StringModel::WriteModel(SequenceByteWriter *byte_writer) const {
  for (int i = 0; i < 255; ++i) {
    int code = CastInt(char_prob_[i], 16);
    byte_writer->Write16Bit(code);
  }
  for (int i = 0; i < 63; ++i) {
    int code = CastInt(length_prob_[i], 8);
    byte_writer->WriteByte(code);
  }
}

SquIDModel *StringModel::ReadModel(ByteReader *byte_reader, size_t index) {
  StringModel *model = new StringModel(index);
  model->char_count_.clear();
  model->length_count_.clear();
  model->char_prob_.resize(255);
  model->length_prob_.resize(63);
  for (int i = 0; i < 255; ++i) model->char_prob_[i] = GetProb(byte_reader->Read16Bit(), 16);
  for (int i = 0; i < 63; ++i) model->length_prob_[i] = GetProb(byte_reader->ReadByte(), 8);
  return model;
}

SquIDModel *StringModelCreator::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                          size_t index) {
  return StringModel::ReadModel(byte_reader, index);
}

SquIDModel *StringModelCreator::CreateModel(const std::vector<int> &attr_type,
                                            const std::vector<size_t> &predictor, size_t index,
                                            double err) {
  if (predictor.size() > 0) return NULL;
  return new StringModel(index);
}
}  // namespace db_compress
