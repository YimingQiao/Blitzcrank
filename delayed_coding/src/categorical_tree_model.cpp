//
// Created by Qiao Yiming on 2022/5/23.
//

#include "../include/categorical_tree_model.h"
namespace db_compress {
void db_compress::CategoricalTreeSquid::Init(TableCategorical &group_table,
                                             std::vector<TableCategorical> &groups,
                                             int group_size_bits) {
  group_table_squid_ = group_table.GetSquID();
  int num_group_ = static_cast<int>(groups.size());
  squid_groups_.resize(num_group_);
  group_size_bits_ = group_size_bits;
  for (int i = 0; i < num_group_; ++i) squid_groups_[i] = groups[i].GetSquID();
}

void db_compress::CategoricalTreeSquid::GetProbIntervals(
    std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
    const db_compress::AttrValue &value) const {
  int group_id = value.Int() >> group_size_bits_;
  int term_id = value.Int() & ((1 << group_size_bits_) - 1);
  group_table_squid_->GetProbIntervals(prob_intervals, prob_intervals_index, AttrValue(group_id));
  squid_groups_[group_id]->GetProbIntervals(prob_intervals, prob_intervals_index,
                                            AttrValue(term_id));
}

void db_compress::CategoricalTreeSquid::Decompress(db_compress::Decoder *decoder,
                                                   db_compress::ByteReader *byte_reader) {
  group_table_squid_->Decompress(decoder, byte_reader);
  int group_id = group_table_squid_->GetResultAttr().Int();
  squid_groups_[group_id]->Decompress(decoder, byte_reader);
  int term_id = squid_groups_[group_id]->GetResultAttr().Int();
  attr_.value_ = ((group_id << group_size_bits_) + term_id);
}

void db_compress::TableCategoricalTree::FeedAttrs(const db_compress::AttrValue &attr_val,
                                                  int count) {
  int value = attr_val.Int();
  if (value >= target_range_) target_range_ = value + 1;

  int group_id = value >> group_size_bits_;
  int term_id = value & ((1 << group_size_bits_) - 1);
  group_table_.FeedAttrs(AttrValue(group_id), count);
  groups_[group_id].FeedAttrs(AttrValue(term_id), count);
}

void db_compress::TableCategoricalTree::EndOfData() {
  group_table_.EndOfData();
  for (TableCategorical &squid : groups_) squid.EndOfData();

  squid_.Init(group_table_, groups_, group_size_bits_);
}

void db_compress::TableCategoricalTree::WriteModel(db_compress::SequenceByteWriter *byte_writer) {
  byte_writer->Write32Bit(target_range_);
  group_table_.WriteModel(byte_writer);
  for (TableCategorical &squid : groups_) squid.WriteModel(byte_writer);
}

db_compress::TableCategoricalTree *db_compress::TableCategoricalTree::ReadModel(
    db_compress::ByteReader *byte_reader) {
  TableCategoricalTree *model = new TableCategoricalTree();

  int target_range = byte_reader->Read32Bit();
  model->Init(target_range);
  model->group_table_ = *static_cast<TableCategorical *>(TableCategorical::ReadModel(byte_reader));
  for (int i = 0; i < model->groups_.size(); ++i)
    model->groups_[i] = *static_cast<TableCategorical *>(TableCategorical::ReadModel(byte_reader));
  model->squid_.Init(model->group_table_, model->groups_, model->group_size_bits_);

  return model;
}
}  // namespace db_compress
