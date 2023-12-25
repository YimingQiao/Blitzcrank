/**
 * @file markov_model.h
 * @brief A markov chain is proposed to model categorical attribute, mining the relationship between
 * columnar data.
 */

#ifndef SQUISH_MARKOV_MODEL_H
#define SQUISH_MARKOV_MODEL_H

#include "categorical_model.h"

namespace db_compress {
class TableMarkov : public SquIDModel {
 public:
  TableMarkov(const std::vector<size_t> &predictor_list, size_t target_var)
      : SquIDModel(predictor_list, target_var), cur_state_(0){};

  void InitModels(const std::vector<int> &attr_type, const std::vector<size_t> &predictor_list,
                  size_t target_var, int num_state);

  void LoadModels(const std::vector<TableCategorical> &models) { states_ = models; }

  void FeedAttrs(const AttrVector &vec, int count) override;

  void EndOfData() override {
    for (TableCategorical &stat : states_) stat.EndOfData();
  }

  CategoricalSquID *GetSquID(const AttrVector &tuple);

  void WriteModel(SequenceByteWriter *byte_write) override;

  static TableMarkov *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index);

  int GetModelCost() const override {
    int ret = 0;
    for (const TableCategorical &model : states_) ret += model.GetModelCost();

    return ret;
  }

  int GetModelDescriptionLength() const override {
    int ret = 0;
    for (const TableCategorical &model : states_) ret += model.GetModelDescriptionLength();

    return ret;
  }

  void SetState(int state) { cur_state_ = state; };

 private:
  std::vector<TableCategorical> states_;
  int num_state_;
  int cur_state_;
};

class TableMarkovCreator : public ModelCreator {
 public:
  SquIDModel *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index) override {
    return TableMarkov::ReadModel(byte_reader, schema, index);
  }

  SquIDModel *CreateModel(const std::vector<int> &attr_type, const std::vector<size_t> &predictor,
                          size_t index, double err) override;

 private:
  // maximum size of dynamic list. Too many statistic table leads to very large
  // overhead.
  const size_t kMaxTableSize = 1000;
};

}  // namespace db_compress

#endif  // SQUISH_MARKOV_MODEL_H
