//
// Created by Qiao Yiming on 2022/7/22.
//

#include "markov_model.h"
namespace db_compress {
void TableMarkov::InitModels(const std::vector<int> &attr_type,
                             const std::vector<size_t> &predictor_list, size_t target_var,
                             int num_state) {
  states_.resize(num_state, {attr_type, predictor_list, target_var});
  num_state_ = num_state;
}

void TableMarkov::FeedAttrs(const AttrVector &vec, int count) {
  states_[cur_state_].FeedAttrs(vec, count);
  cur_state_ = vec.attr_[target_var_].Int();
  if (cur_state_ < 0 || cur_state_ >= num_state_)
    std::cerr << "Markov Chain state is out of boundary.\n";
}

CategoricalSquID *TableMarkov::GetSquID(const AttrVector &tuple) {
  return states_[cur_state_].GetSquID(tuple);
}

void TableMarkov::WriteModel(SequenceByteWriter *byte_write) {
  byte_write->Write16Bit(num_state_);
  for (TableCategorical &model : states_) model.WriteModel(byte_write);
}

TableMarkov *TableMarkov::ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index) {
  int num_state = byte_reader->Read16Bit();
  if (num_state == 0) std::cerr << "[TableMarkov] Read Model Failed.\n";

  std::vector<TableCategorical> models;
  TableCategoricalCreator creator;
  for (int i = 0; i < num_state; ++i)
    models.push_back(
        *static_cast<TableCategorical *>(creator.ReadModel(byte_reader, schema, index)));

  TableMarkov *model = new TableMarkov(models[0].GetPredictorList(), models[0].GetTargetVar());
  model->LoadModels(models);

  return model;
}

SquIDModel *TableMarkovCreator::CreateModel(const std::vector<int> &attr_type,
                                            const std::vector<size_t> &predictor, size_t index,
                                            double err) {
  // filter illegal model, i.e. all predictors should have a non-zero capacity.
  size_t table_size = 1;
  for (int attr : predictor) {
    if (!GetAttrInterpreter(attr)->EnumInterpretable()) return nullptr;
    table_size *= GetAttrInterpreter(attr)->EnumCap();
  }
  if (table_size > kMaxTableSize) return nullptr;

  TableMarkov *ret = new TableMarkov(predictor, index);
  ret->InitModels(attr_type, predictor, index, GetAttrInterpreter(index)->EnumCap());

  return ret;
}
}  // namespace db_compress
