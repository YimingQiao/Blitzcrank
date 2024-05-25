#include "../include/model_learner.h"

#include <algorithm>
#include <utility>

#include "../include/model.h"

namespace db_compress {

namespace {

// New Models are appended to the end of vector
bool CreateModel(const Schema &schema, const std::vector<size_t> &predictors, size_t target_var,
                 const CompressionConfig &config, std::vector<std::unique_ptr<SquIDModel> > *vec) {
  double err = config.allowed_err_[target_var];
  int attr_type = schema.attr_type_[target_var];
  bool success = false;
  ModelCreator *creator = GetAttrModel(attr_type);
  std::unique_ptr<SquIDModel> model(
      creator->CreateModel(schema.attr_type_, predictors, target_var, err));
  if (model != nullptr) {
    vec->push_back(std::move(model));
    success = true;
  }

  return success;
}

}  // anonymous namespace
void RelationModelLearner::StoreModelCost(const SquIDModel &model) {
  std::set<size_t> predictors(model.GetPredictorList().begin(), model.GetPredictorList().end());
  size_t target = model.GetTargetVar();
  int previous_cost = GetModelCost(model.GetPredictorList(), model.GetTargetVar());
  if (previous_cost == -1 || previous_cost > model.GetModelCost())
    stored_model_cost_[make_pair(predictors, target)] = std::max(model.GetModelCost(), 0);
}
RelationModelLearner::RelationModelLearner(Schema schema, const CompressionConfig &config)
    : schema_(std::move(schema)),
      selected_model_(schema_.attr_type_.size()),
      model_predictor_list_(schema_.attr_type_.size()),
      config_(config),
      learner_stage_(0) {
  if (config_.skip_model_learning_) {
    ordered_attr_list_.resize(schema_.attr_type_.size());
    model_predictor_list_.resize(schema.attr_type_.size());
    for (int i = 0; i < schema_.attr_type_.size(); i++) ordered_attr_list_[i] = i;
    learner_stage_ = 1;
    inactive_attr_.clear();
  }
  InitActiveModelList();
}
void RelationModelLearner::FeedTuple(const AttrVector &tuple) {
  for (auto &active_model : active_model_list_) {
    active_model->FeedAttrs(tuple, 1);
  }
}
void RelationModelLearner::EndOfData() {
  switch (learner_stage_) {
    case 0:
      // At the end of data, we inform each of the active models, let them
      // compute their model cost, and then store them into the
      // stored_model_cost_ variable.
      for (auto &active_model : active_model_list_) {
        active_model->EndOfData();
      }
      for (auto &active_model : active_model_list_) {
        StoreModelCost(*active_model);
      }

      // Now if there is no longer any active model, we add the best model
      // to ordered_attr_list_ and then start a new iteration. Note that
      // in order to save memory space, we only store the target variable
      // and predictor variables, the actual model will be learned again
      // during the second stage of the algorithm.
      if (active_model_list_.empty()) {
        int next_attr = -1;
        for (size_t i = 0; i < schema_.attr_type_.size(); ++i) {
          if (inactive_attr_.count(i) == 0) {
            if (next_attr == -1) {
              next_attr = i;
            } else if (GetModelCost(model_predictor_list_[i], i) <
                       GetModelCost(model_predictor_list_[next_attr], next_attr))
              next_attr = i;
          }
        }
        // If there is no more active attribute, we are done.
        if (next_attr != -1) {
          ordered_attr_list_.push_back(next_attr);
          inactive_attr_.insert(next_attr);
        }

        // Now if we reach the point where models for every attribute
        // has been selected, we mark the end of this stage and start
        // next stage. Otherwise, we simply start another iteration.
        if (ordered_attr_list_.size() == schema_.attr_type_.size()) {
          learner_stage_ = 1;
          inactive_attr_.clear();
        }
      }
      break;
    case 1:
      for (auto &active_model : active_model_list_) {
        active_model->EndOfData();
        int target_var = active_model->GetTargetVar();
        inactive_attr_.insert(target_var);
        if (selected_model_[target_var] == nullptr ||
            selected_model_[target_var]->GetModelCost() > active_model->GetModelCost()) {
          selected_model_[target_var] = std::move(active_model);
        }
      }
      if (inactive_attr_.size() == schema_.attr_type_.size()) {
        learner_stage_ = 2;
      }
  }
  // If we still haven't reached end stage, init active models
  if (learner_stage_ != 2) InitActiveModelList();
}
void RelationModelLearner::InitActiveModelList() {
  active_model_list_.clear();

  if (learner_stage_ == 0) {
    // In the first stage, we initially create an empty model for every
    // inactive attribute. Then we expand each of these models.
    for (size_t i = 0; i < schema_.attr_type_.size(); ++i) {
      if (inactive_attr_.count(i) == 0) {
        if (GetModelCost(std::vector<size_t>(), i) == -1) {
          CreateModel(schema_, std::vector<size_t>(), i, config_, &active_model_list_);
        } else {
          // We empty the current predictor list, and search from
          // scratch
          model_predictor_list_[i].clear();
          // We choose the models based on a greedy criterion, we
          // choose the predictor attribute that can reduce the cost
          // in largest amount. During this process, all models with
          // unknown cost (a.k.a. "active" models) are added to a
          // list, and then choose the "inactive" model with lowest
          // cost to expand.
          while (true) {
            std::vector<size_t> predictor_list(model_predictor_list_[i]);
            std::set<size_t> predictor_set(predictor_list.begin(), predictor_list.end());
            int previous_cost = GetModelCost(predictor_list, i);
            // Add a new slot in predictor list
            predictor_list.push_back(0);
            bool model_expanded = false;
            for (size_t attr : ordered_attr_list_) {
              if (predictor_set.count(attr) == 0) {
                predictor_list[predictor_set.size()] = attr;
                if (GetModelCost(predictor_list, i) == -1) {
                  // Multiple models may be associated for any
                  // predictor and target
                  CreateModel(schema_, predictor_list, i, config_, &active_model_list_);
                } else if (GetModelCost(predictor_list, i) < previous_cost) {
                  model_predictor_list_[i] = predictor_list;
                  previous_cost = GetModelCost(predictor_list, i);
                  model_expanded = true;
                }
              }
            }
            // If current model can not be expanded, break the loop
            if (!model_expanded) {
              break;
            }
          }
        }
      }
    }
  } else {
    // In the second stage, we simply relearn the model selected from the
    // first stage, no model expansion is needed. However, we need to assure
    // that the models that are currently learning have predictors all lies
    // within the range of target vars of learned models.
    for (size_t i = 0; i < schema_.attr_type_.size(); ++i) {
      bool learnable = true;
      for (size_t attr : model_predictor_list_[i]) {
        if (inactive_attr_.count(attr) == 0) {
          learnable = false;
        }
      }
      if (!learnable) continue;

      CreateModel(schema_, model_predictor_list_[i], i, config_, &active_model_list_);
    }
  }
}
}  // namespace db_compress