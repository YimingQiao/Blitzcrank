/**
 * @file model_learner.h
 * @brief The header for model learner for relational dataset.
 */
#ifndef MODEL_LEARNER_H
#define MODEL_LEARNER_H

#include <map>
#include <set>
#include <vector>

#include "base.h"
#include "model.h"

namespace db_compress {

/**
 * Compression config. Squish could skip model learning stage, and utilize given
 * attribute order and predictor model to compress data. If user wants squish to
 * learn structure, allowed error should be given.
 *
 */
struct CompressionConfig {
  std::vector<double> allowed_err_;
  bool skip_model_learning_;
};

/**
 * RelationModelLearner learns all the models simultaneously in an online
 * fashion.
 */
class RelationModelLearner {
 public:
  /**
   * Create a new relation model learner
   *
   * @param schema relational dataset schema
   * @param config compression config
   */
  RelationModelLearner(Schema schema, const CompressionConfig &config);

  /**
   * These function are used to learn the model objects.
   *
   * @param tuple unit of a relational dataset
   */
  void FeedTuple(const AttrVector &tuple);
  void EndOfData();

  /**
   * This function returns the SquIDModel object given attribute index. Caller
   * takes ownership of the SquIDModel object. This function should only be
   * called once for each attribute.
   *
   * @param attr_index attribute index in schema
   * @return return squid model for attribute which has the given index
   */
  SquIDModel *GetModel(size_t attr_index) { return selected_model_[attr_index].release(); }

  /**
   * This function gets the order of attr_type_ during the encoding/decoding
   * phase.
   *
   * @return attribute order
   */
  const std::vector<size_t> &GetOrderOfAttributes() const { return ordered_attr_list_; }

  bool RequireFullPass() const { return learner_stage_ != 0; }
  bool RequireMoreIterations() const { return learner_stage_ != 2; }

 private:
  Schema schema_;

  int learner_stage_;
  CompressionConfig config_;

  std::vector<size_t> ordered_attr_list_;
  std::set<size_t> inactive_attr_;
  std::vector<std::unique_ptr<SquIDModel>> active_model_list_;
  std::vector<std::unique_ptr<SquIDModel>> selected_model_;
  std::vector<std::vector<size_t>> model_predictor_list_;
  std::map<std::pair<std::set<size_t>, size_t>, int> stored_model_cost_;

  /**
   * Initialize active model. Active model is trained by feeding some tuples,
   * then produce a model cost. By comparing model costs, the best model
   * (predictors) is selected for each attribute.
   */
  void InitActiveModelList();

  /**
   * Some model costs could be referenced for many times, they are stored for
   * future use.
   *
   * @param model index of model cost, each model has a cost.
   */
  void StoreModelCost(const SquIDModel &model);

  /**
   * Get the model cost based on predictors and target variable. If not known,
   * return -1.
   *
   * @param predictors target attribute may depend on other attributes, called
   * predictors.
   * @param target target attribute, which is this model for.
   * @return return model cost
   */
  int GetModelCost(const std::vector<size_t> &predictors, size_t target) const {
    std::set<size_t> predictors_set(predictors.begin(), predictors.end());
    auto iterator = stored_model_cost_.find(make_pair(predictors_set, target));
    if (iterator == stored_model_cost_.end()) {
      return -1;
    }
    return iterator->second;
  }
};
}  // namespace db_compress

#endif
