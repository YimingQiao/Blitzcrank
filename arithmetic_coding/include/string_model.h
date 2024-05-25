#ifndef STRING_MODEL_H
#define STRING_MODEL_H

#include <vector>

#include "base.h"
#include "model.h"

namespace db_compress {

class StringSquID : public SquID {
 public:
  void Init(const std::vector<Prob> *char_prob, const std::vector<Prob> *len_prob);
  bool HasNextBranch() const { return !is_end_; }
  int GetNextBranch(const AttrValue &attr);
  void ChooseNextBranch(int branch);
  const AttrValue &GetResultAttr() { return attr_; }
  ProbInterval &GenerateNextBranch(int branch);
  inline int GenerateNextBranchDecoder(int branch);
  int GetBranchSize() {
    if (word_length_ == -1) {
      return len_prob_->size();
    }
    return char_prob_->size();
  }
  void Decompress(Decoder *decoder, ByteReader *byte_reader);
  void GetProbIntervals(std::vector<ProbInterval> &prob_intervals, int &index,
                        const AttrValue &value) {
    while (HasNextBranch()) {
      const int branch = GetNextBranch(value);
      prob_intervals[index++] = GenerateNextBranch(branch);
      ChooseNextBranch(branch);
    }
  }

 private:
  const std::vector<Prob> *char_prob_, *len_prob_;
  bool is_end_;
  int word_length_;

  AttrValue attr_;
};

class StringModel : public SquIDModel {
 public:
  explicit StringModel(size_t target_var);
  StringSquID *GetSquID(const AttrVector &tuple);
  int GetModelCost() const override;

  void FeedTuple(const AttrVector &tuple) override;
  void EndOfData() override;
  void GetProbInterval(std::vector<ProbInterval> &prob_intervals, const AttrVector &tuple,
                       int &index);

  int GetModelDescriptionLength() const override;
  void WriteModel(SequenceByteWriter *byte_writer) const override;
  static SquIDModel *ReadModel(ByteReader *byte_reader, size_t index);

 private:
  std::vector<Prob> char_prob_, length_prob_;
  std::vector<int> char_count_, length_count_;

  StringSquID squid_;
};

class StringModelCreator : public ModelCreator {
 public:
  SquIDModel *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index);
  SquIDModel *CreateModel(const std::vector<int> &attr_type, const std::vector<size_t> &predictor, size_t index,
                          double err);
};
}  // namespace db_compress

#endif
