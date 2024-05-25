/**
 * @file json_model_learner.h
 * @brief The header of model learner for JSON-style files
 */

#ifndef DB_COMPRESS_JSON_MODEL_LEARNER_H
#define DB_COMPRESS_JSON_MODEL_LEARNER_H

#include "json_base.h"
#include "json_model.h"

namespace db_compress {
struct CompressionConfig {
  bool skip_model_learning_;
};

struct Dependency {
  size_t target;
  std::vector<size_t> predictors;
};

struct LearnerTriplet {
  // each object has a triplet
  JSONModel *object;

  // ancestors could be dependent by object's leaf nodes.
  std::set<size_t> ancestors;

  // GOALS:
  std::vector<size_t> ordered_leaf_nodes_list;
  std::set<size_t> inactive_nodes;

  // utils:
  std::vector<Dependency> dependencies;

  LearnerTriplet(JSONModel *object, std::set<size_t> ancestors)
      : object(object), ancestors(ancestors) {}
};

/**
 * Structure learning for json.
 * i) generate all objects and their ancestors based on init model.
 * ii) loop all objects and their members, create new json models for learning.
 * iii) for each objects, update its members order and newly added member predictors in init model.
 */
class JSONModelLearner {
 public:
  /**
   * Create a new json model learner
   *
   * @param schema relational dataset schema
   * @param config compression config
   */
  JSONModelLearner(const JSONSchema &json_schema, const CompressionConfig &config,
                   JSONModel *init_model);

  /**
   * These function are used to learn sketch tree.
   *
   * @param attr_vec leaf attributes in a json sample (tree)
   * @param target_index target attribute index
   */
  void FeedNode(const rapidjson::Value &node, AttrVector &attr_record);
  void EndOfData();
  bool RequireFullPass() const { return learner_stage_ != 0; }
  bool RequireMoreIterations() const { return learner_stage_ != 2; }

 private:
  JSONSchema json_schema_;
  int learner_stage_;
  CompressionConfig config_;

  // roots of json tree
  JSONModel *sketch_root_;
  std::vector<LearnerTriplet> triplet_list_;

  std::vector<std::unique_ptr<JSONModel>> active_model_list_;
  std::vector<std::vector<size_t>> model_predictor_list_;
  std::map<std::pair<std::set<size_t>, size_t>, int> stored_model_cost_;

  void InitTriplet();

  void InitActiveModelList();

  /**
   * Create active models to train from object list. This function is called inside
   * InitActiveModelList().
   */
  void CreateActiveModel();

  /**
   * Update sketch tree with triplet orderings and predictors.This function is called inside
   * InitActiveModelList().
   */
  void UpdateSketchRoot();

  /**
   * Get the model cost based on predictors and target variable. If not known,
   * return -1.
   *
   * @param predictors target attribute may depend on other attributes, called
   * predictors.
   * @param target target attribute, which is this model for.
   * @return return model cost
   */
  int GetModelCost(const std::vector<size_t> &predictors, size_t target) const;

  /**
   * Some model costs could be referenced for many times, they are stored for
   * future use.
   *
   * @param model index of model cost, each model has a cost.
   */
  void StoreModelCost(JSONModel &model);
};
}  // namespace db_compress
#endif  // DB_COMPRESS_JSON_MODEL_LEARNER_H
