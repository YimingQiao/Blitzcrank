//
// Created by Qiao Yiming on 2022/3/9.
//

#include "../include/json_model_learner.h"

namespace db_compress {
namespace {
template <typename T>
std::set<T> GetUnion(const std::set<T> &a, const std::set<T> &b) {
  std::set<T> result = a;
  result.insert(b.begin(), b.end());
  return result;
}
}  // namespace
JSONModelLearner::JSONModelLearner(const JSONSchema &json_schema, const CompressionConfig &config,
                                   JSONModel *init_model)
    : json_schema_(json_schema),
      config_(config),
      sketch_root_(init_model),
      model_predictor_list_(json_schema_.path_type_.size()) {
  // if (config_.skip_model_learning_) learner_stage_ = 1;
  learner_stage_ = 1;
  InitTriplet();
  InitActiveModelList();
}
int JSONModelLearner::GetModelCost(const std::vector<size_t> &predictors, size_t target) const {
  std::set<size_t> predictors_set(predictors.begin(), predictors.end());
  auto iterator = stored_model_cost_.find(make_pair(predictors_set, target));
  if (iterator == stored_model_cost_.end()) {
    return -1;
  }
  return iterator->second;
}

void JSONModelLearner::StoreModelCost(JSONModel &model) {
  std::vector<JSONModel *> node_list;
  node_list.push_back(&model);

  while (!node_list.empty()) {
    JSONModel *node = node_list.back();
    node_list.pop_back();

    // only consider members in the order
    for (size_t idx : node->members_order_) {
      auto &member = node->object_members_[idx];
      // store leaf node cost
      if (member->types_.count(NodeType::kNumberType) ||
          member->types_.count(NodeType::kStringType) ||
          member->types_.count(NodeType::kDoubleType)) {
        std::set<size_t> predictors(member->predictors_.begin(), member->predictors_.end());
        size_t target = member->node_id_;
        int previous_cost = GetModelCost(member->predictors_, target);
        if (previous_cost == -1 || previous_cost > member->GetModelCost())
          stored_model_cost_[make_pair(predictors, target)] = std::max(member->GetModelCost(), 0);
      }

      // push non-leaf nodes
      if (member->types_.count(NodeType::kObjectType) ||
          member->types_.count(NodeType::kArrayType)) {
        node_list.push_back(member.get());
      }
    }
  }
}

void JSONModelLearner::InitTriplet() {
  std::vector<JSONModel *> node_list;
  std::map<JSONModel *, std::set<size_t>> ancestors_map;

  node_list.push_back(sketch_root_);
  ancestors_map[sketch_root_] = std::set<size_t>();
  triplet_list_.emplace_back(sketch_root_, std::set<size_t>());

  while (!node_list.empty()) {
    JSONModel *node = node_list.back();
    node_list.pop_back();
    std::set<size_t> ancestors = ancestors_map[node];
    std::vector<size_t> non_leaf_nodes_indices;
    // find all leaf nodes, add them to ancestors;
    for (size_t i = 0; i < node->object_members_.size(); ++i) {
      auto &member = node->object_members_[i];
      if (member->types_.count(NodeType::kObjectType) || member->types_.count(NodeType::kArrayType))
        non_leaf_nodes_indices.push_back(i);
      else
        ancestors.insert(member->node_id_);
    }
    // find all objects, all them to object list
    for (size_t idx : non_leaf_nodes_indices) {
      auto &member = node->object_members_[idx];
      node_list.push_back(member.get());
      ancestors_map[member.get()] = ancestors;
      // node cannot be dependent on array
      if (member->types_.count(NodeType::kObjectType))
        triplet_list_.emplace_back(member.get(), ancestors);
    }

    // all leaf nodes are excluded.
    node->members_order_ = non_leaf_nodes_indices;
  }
}

void JSONModelLearner::InitActiveModelList() {
  active_model_list_.clear();

  if (learner_stage_ == 0) {
    for (auto &triplet : triplet_list_) {
      for (auto &child : triplet.object->object_members_) {
        if (triplet.inactive_nodes.count(child->node_id_)) continue;

        if (child->types_.count(NodeType::kStringType) ||
            child->types_.count(NodeType::kNumberType) ||
            child->types_.count(NodeType::kDoubleType)) {
          size_t idx = child->node_id_;
          if (GetModelCost(std::vector<size_t>(), idx) == -1) {
            triplet.dependencies.push_back({idx, std::vector<size_t>()});
          } else {
            model_predictor_list_[idx].clear();
            while (true) {
              std::vector<size_t> predictor_list(model_predictor_list_[idx]);
              std::set<size_t> predictor_set(predictor_list.begin(), predictor_list.end());
              int previous_cost = GetModelCost(predictor_list, idx);

              predictor_list.push_back(0);
              bool model_expanded = false;
              for (size_t node : GetUnion(triplet.ancestors, triplet.inactive_nodes)) {
                if (predictor_set.count(node) == 0) {
                  predictor_list.back() = node;
                  if (GetModelCost(predictor_list, idx) == -1) {
                    triplet.dependencies.push_back({idx, predictor_list});
                  } else if (GetModelCost(predictor_list, idx) < previous_cost) {
                    model_expanded = true;
                    model_predictor_list_[idx] = predictor_list;
                    previous_cost = GetModelCost(predictor_list, idx);
                  }
                }
              }

              if (!model_expanded) break;
            }
          }
        }
      }
    }
    UpdateSketchRoot();
    CreateActiveModel();
  } else {
    for (auto &triplet : triplet_list_) {
      for (auto &child : triplet.object->object_members_) {
        if (child->types_.count(NodeType::kStringType) ||
            child->types_.count(NodeType::kNumberType) ||
            child->types_.count(NodeType::kDoubleType)) {
          bool learnable = true;
          size_t idx = child->node_id_;
          for (size_t node : model_predictor_list_[idx]) {
            if (triplet.inactive_nodes.count(node) == 0) {
              learnable = false;
              break;
            }
          }
          if (!learnable) {
            std::cerr << "Model " << idx << " is not learnable" << std::endl;
            continue;
          }
        }
      }
    }
    UpdateSketchRoot();
  }
}

void JSONModelLearner::UpdateSketchRoot() {
  // set predictors for inactive nodes.
  for (size_t k = 0; k < triplet_list_.size(); ++k) {
    auto &triplet = triplet_list_[k];
    JSONModel *object = triplet.object;
    for (size_t i = 0; i < triplet.ordered_leaf_nodes_list.size(); ++i) {
      size_t node_id = triplet.ordered_leaf_nodes_list[i];
      size_t node_index = object->GetMemberIndex(node_id);
      bool success = object->object_members_[node_index]->SetPredictors(
          model_predictor_list_[node_id], json_schema_);
      if (!success) std::cerr << "Failed to set predictors for inactive node.\n " << std::endl;
    }
    // set triplet ordering
    std::vector<size_t> triplet_ordering(triplet.ordered_leaf_nodes_list.size());
    for (size_t i = 0; i < triplet_ordering.size(); ++i)
      triplet_ordering[i] = object->GetMemberIndex(triplet.ordered_leaf_nodes_list[i]);
    object->SetObjectOrdering(triplet_ordering);
  }
}

void JSONModelLearner::CreateActiveModel() {
  size_t num_models = 0;
  // count how many active models we need
  for (auto &triplet : triplet_list_)
    num_models = std::max(num_models, triplet.dependencies.size());

  while (num_models-- > 0) {
    std::vector<char> success(triplet_list_.size());

    for (size_t k = 0; k < triplet_list_.size(); ++k) {
      auto &triplet = triplet_list_[k];
      // In this triplet, no leaf node models are needed to estimate.
      if (triplet.dependencies.empty()) {
        std::vector<size_t> ordering(triplet.ordered_leaf_nodes_list.size());
        for (size_t i = 0; i < ordering.size(); ++i)
          ordering[i] = triplet.object->GetMemberIndex(triplet.ordered_leaf_nodes_list[i]);
        triplet.object->SetObjectOrdering(ordering);
        continue;
      }

      JSONModel *object = triplet.object;
      Dependency dependency = triplet.dependencies.back();
      triplet.dependencies.pop_back();

      // get triplet ordering
      size_t idx = object->GetMemberIndex(dependency.target);
      std::vector<size_t> ordering(triplet.ordered_leaf_nodes_list.size() + 1, idx);
      for (size_t i = 0; i < triplet.ordered_leaf_nodes_list.size(); ++i) {
        size_t node_id = triplet.ordered_leaf_nodes_list[i];
        ordering[i] = object->GetMemberIndex(node_id);
      }

      // set triplet ordering
      object->SetObjectOrdering(ordering);

      // set triplet predictors
      success[k] = object->object_members_[idx]->SetPredictors(dependency.predictors, json_schema_);
    }

    // remove model when all triplets of it are failed.
    bool all_failure = true;
    for (bool bit : success)
      if (bit) {
        all_failure = false;
        break;
      }

    if (!all_failure) active_model_list_.push_back(std::make_unique<JSONModel>(*sketch_root_));
  }
}

void JSONModelLearner::FeedNode(const rapidjson::Value &node, AttrVector &attr_record) {
  if (learner_stage_ == 0)
    for (int i = 0; i < active_model_list_.size(); ++i) {
      JSONModel *model = static_cast<JSONModel *>(active_model_list_[i].get());
      LearnNode(model, node, attr_record);
    }
  else
    LearnNode(sketch_root_, node, attr_record);
}

void JSONModelLearner::EndOfData() {
  switch (learner_stage_) {
    case 0:
      for (auto &active_model : active_model_list_) active_model->EndOfData();
      for (auto &active_model : active_model_list_) StoreModelCost(*active_model);

      if (active_model_list_.empty()) {
        std::vector<char> finished(triplet_list_.size());

        for (size_t k = 0; k < triplet_list_.size(); ++k) {
          auto &triplet = triplet_list_[k];
          int next_node = -1;
          int num_leaf_nodes = 0;

          for (size_t i = 0; i < triplet.object->object_members_.size(); ++i) {
            auto &object = triplet.object->object_members_[i];
            // only consider the leaf nodes
            if (object->types_.count(NodeType::kStringType) ||
                object->types_.count(NodeType::kNumberType) ||
                object->types_.count(NodeType::kDoubleType)) {
              num_leaf_nodes += 1;
              size_t node_id = object->node_id_;
              if (triplet.inactive_nodes.count(node_id) == 0) {
                if (next_node == -1)
                  next_node = node_id;
                else if (GetModelCost(model_predictor_list_[node_id], node_id) <
                         GetModelCost(model_predictor_list_[next_node], next_node))
                  next_node = node_id;
              }
            }
          }

          if (next_node != -1) {
            triplet.ordered_leaf_nodes_list.push_back(next_node);
            triplet.inactive_nodes.insert(next_node);
          }

          finished[k] = triplet.ordered_leaf_nodes_list.size() == num_leaf_nodes;
        }

        // check if all triplets are finished.
        bool all_finished = true;
        for (bool bit : finished)
          if (!bit) {
            all_finished = false;
            break;
          }
        if (all_finished) {
          learner_stage_ = 1;
          for (auto &triplet : triplet_list_) triplet.inactive_nodes.clear();
        }
      }

      break;
    case 1:
      sketch_root_->EndOfData();
      learner_stage_ = 2;
      break;
  }

  // If we still haven't reached end stage, init active models
  if (learner_stage_ != 2) InitActiveModelList();
}
}  // namespace db_compress