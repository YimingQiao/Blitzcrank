/**
 * @file categorical_tree_model.h
 * @brief Categorical model can only support not more than 65536 values. Advanced categorical model
 * is created to support more than 65536 * 65536.
 */

#ifndef DB_COMPRESS_CATEGORICAL_TREE_MODEL_H
#define DB_COMPRESS_CATEGORICAL_TREE_MODEL_H

#include "categorical_model.h"

namespace db_compress {
const int kMaxCategoricalSizeBits = 13;
class CategoricalTreeSquid {
 public:
  void Init(TableCategorical &group_table, std::vector<TableCategorical> &groups, int group_size);

  void GetProbIntervals(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                        const AttrValue &value) const;

  void Decompress(Decoder *decoder, ByteReader *byte_reader);

  const AttrValue &GetResultAttr() { return attr_; }

 private:
  AttrValue attr_;
  int group_size_bits_;
  CategoricalSquID *group_table_squid_;
  std::vector<CategoricalSquID *> squid_groups_;
};

class TableCategoricalTree {
 public:
  int target_range_;

  void Init(int target_range) {
    target_range_ = target_range;
    group_size_bits_ = kMaxCategoricalSizeBits;
    int num_group = 1 + (target_range >> group_size_bits_);
    groups_.resize(num_group);
  }

  void FeedAttrs(const AttrValue &attr_val, int count);

  void EndOfData();

  void WriteModel(SequenceByteWriter *byte_writer);

  static TableCategoricalTree *ReadModel(ByteReader *byte_reader);

  CategoricalTreeSquid *GetSquID() { return &squid_; }

 private:
  int group_size_bits_;
  TableCategorical group_table_;
  std::vector<TableCategorical> groups_;
  CategoricalTreeSquid squid_;
};
}  // namespace db_compress

#endif  // DB_COMPRESS_CATEGORICAL_TREE_MODEL_H
