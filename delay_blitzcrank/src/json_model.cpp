//
// Created by Qiao Yiming on 2022/2/16.
//

#include "../include/json_model.h"

#include <iostream>
#include <utility>
#include <vector>

#include "../include/blitzcrank_exception.h"
#include "../include/categorical_model.h"
#include "../include/json_base.h"
#include "../include/numerical_model.h"
#include "../include/string_model.h"

namespace db_compress {
std::vector<JSONModel *> sketch_stack;
std::vector<const rapidjson::Value *> node_stack;

void LearnNode(JSONModel *sketch, const rapidjson::Value &sample, AttrVector &attr_record_) {
  sketch_stack.push_back(sketch);
  node_stack.push_back(&sample);

  while (!node_stack.empty()) {
    const rapidjson::Value &node = *node_stack.back();
    JSONModel &sketch_node = *sketch_stack.back();
    node_stack.pop_back();
    sketch_stack.pop_back();

    NodeType node_type = GetNodeType(node);
    sketch_node.FeedNodeType(node_type);

    switch (node_type) {
      case kNullType:
      case kFalseType:
      case kTrueType:
        break;
      case kObjectType: {
        for (int idx : sketch_node.members_order_) {
          std::unique_ptr<JSONModel> &member_node = sketch_node.object_members_[idx];
          std::string &name = member_node->name_;
          if (!node.HasMember(name.c_str())) {
            member_node->FeedNodeExist(0);
            continue;
          }
          member_node->FeedNodeExist(1);
          sketch_stack.push_back(member_node.get());
          node_stack.push_back(&node.GetObject()[name.c_str()]);
        }
        break;
      }
      case kArrayType: {
        int array_size = static_cast<int>(node.GetArray().Size());
        sketch_node.FeedArraySize(array_size);
        for (int i = 0; i < array_size; ++i) {
          sketch_stack.push_back(sketch_node.GetArrayNode());
          node_stack.push_back(&node[i]);
        }
        break;
      }
      case kStringType: {
        attr_record_.attr_[sketch_node.node_id_].value_ = (node.GetString());
        sketch_node.string_model_->FeedAttrs(attr_record_, 1);
        break;
      }
      case kNumberType: {
        attr_record_.attr_[sketch_node.node_id_].value_ = (node.GetInt());
        sketch_node.number_model_->FeedAttrs(attr_record_, 1);
        break;
      }
      case kDoubleType: {
        attr_record_.attr_[sketch_node.node_id_].value_ = (node.GetDouble());
        sketch_node.number_model_->FeedAttrs(attr_record_, 1);
        break;
      }
      case kTimeSeriesType: {
        // Feed array size
        int array_size = static_cast<int>(node.GetArray().Size());
        sketch_node.FeedArraySize(array_size);

        // Feed time series
        std::vector<double> &time_series =
            sketch_node.GetArrayNode()->ts_model_->time_series_buffer_;
        if (time_series.size() < array_size) time_series.resize(array_size);
        for (int i = 0; i < array_size; ++i) time_series[i] = node[i].GetDouble();

        sketch_node.GetArrayNode()->ts_model_->FeedTimeSeries(attr_record_, array_size);
        break;
      }
      default: {
        std::cerr << "JSONModel::LearnNode - Unknown Type.\n";
      }
    }
  }
}

void GetProbInterval(JSONModel *sketch, const rapidjson::Value &sample, AttrVector &attr_record_,
                     std::vector<Branch *> &prob_intervals, int &prob_intervals_index) {
  sketch_stack.push_back(sketch);
  node_stack.push_back(&sample);

  while (!node_stack.empty()) {
    const rapidjson::Value &node = *node_stack.back();
    JSONModel &sketch_node = *sketch_stack.back();
    node_stack.pop_back();
    sketch_stack.pop_back();

    NodeType node_type = GetNodeType(node);
    sketch_node.GetProbIntervalsNodeType(node_type, prob_intervals, prob_intervals_index);

    switch (node_type) {
      case kNullType:
        break;
      case kFalseType:
        break;
      case kTrueType:
        break;
      case kObjectType: {
        for (int idx : sketch_node.members_order_) {
          std::unique_ptr<JSONModel> &member_node = sketch_node.object_members_[idx];
          auto *name = member_node->name_.c_str();
          if (!node.HasMember(name)) {
            member_node->GetProbIntervalsExist(0, prob_intervals, prob_intervals_index);
            continue;
          }
          member_node->GetProbIntervalsExist(1, prob_intervals, prob_intervals_index);

          sketch_stack.push_back(member_node.get());
          node_stack.push_back(&node.GetObject()[name]);
        }
        break;
      }
      case kArrayType: {
        int array_size = static_cast<int>(node.GetArray().Size());
        sketch_node.GetProbIntervalsArraySize(array_size, prob_intervals, prob_intervals_index);
        for (int i = 0; i < array_size; ++i) {
          sketch_stack.push_back(sketch_node.GetArrayNode());
          node_stack.push_back(&node[i]);
        }
        break;
      }
      case kStringType: {
        attr_record_.attr_[sketch_node.node_id_].value_ = (node.GetString());
        StringSquID *squid = sketch_node.string_model_->GetSquID(attr_record_);
        squid->GetProbIntervals(prob_intervals, prob_intervals_index,
                                attr_record_.attr_[sketch_node.node_id_]);
        break;
      }
      case kNumberType: {
        attr_record_.attr_[sketch_node.node_id_].value_ = (node.GetInt());
        NumericalSquID *squid = sketch_node.number_model_->GetSquID(attr_record_);
        squid->GetProbIntervals(prob_intervals, prob_intervals_index,
                                attr_record_.attr_[sketch_node.node_id_]);
        break;
      }
      case kDoubleType: {
        attr_record_.attr_[sketch_node.node_id_].value_ = (node.GetDouble());
        NumericalSquID *squid = sketch_node.number_model_->GetSquID(attr_record_);
        squid->GetProbIntervals(prob_intervals, prob_intervals_index,
                                attr_record_.attr_[sketch_node.node_id_]);
        break;
      }
      case kTimeSeriesType: {
        // Compress length
        int array_size = static_cast<int>(node.GetArray().Size());
        sketch_node.GetProbIntervalsArraySize(array_size, prob_intervals, prob_intervals_index);

        // Compress time series
        std::vector<double> &time_series =
            sketch_node.GetArrayNode()->ts_model_->time_series_buffer_;
        if (time_series.size() < array_size) time_series.resize(array_size);
        for (int i = 0; i < array_size; ++i) time_series[i] = node[i].GetDouble();

        TimeSeriesSquID *squid = sketch_node.GetArrayNode()->ts_model_->GetSquID(attr_record_);
        squid->GetProbIntervals(prob_intervals, prob_intervals_index, time_series, array_size);
        break;
      }
      default: {
        std::cerr << "JSONModel::LearnNode - Unknown Type.\n";
      }
    }
  }
}

JSONModel::JSONModel(JSONModel &json_node) {
  name_ = json_node.name_;
  node_id_ = json_node.node_id_;
  types_ = json_node.types_;
  predictors_ = json_node.predictors_;
  members_order_ = json_node.members_order_;

  for (std::unique_ptr<JSONModel> &child : json_node.object_members_)
    object_members_.push_back(std::make_unique<JSONModel>(*child));

  if (json_node.array_member_) {
    array_member_ = std::make_unique<JSONModel>(*json_node.array_member_);
    // array_size_squid_ = std::make_unique<TableNumerical>(true, 1);
    array_size_squid_ = std::make_unique<TableCategorical>();
  }
  if (json_node.string_model_)
    string_model_ = std::make_unique<StringModel>(*json_node.string_model_);
  if (json_node.number_model_)
    number_model_ = std::make_unique<TableNumerical>(*json_node.number_model_);
  if (json_node.ts_model_) {
    ts_model_ = std::make_unique<TableTimeSeries>(*json_node.ts_model_);
  }
}

JSONModel *JSONModel::CreateJSONTree(const JSONSchema &json_schema) {
  if (json_schema.path_order_.size() > 65535)
    throw JsonLeafNodeIndexException(
        "JSONModel::CreateJSONTree: Leaf node index overflow. 16 bits are used to encode leaf "
        "indices, so there are almost 65536 leaf nodes in a json file.\n");

  auto *root = new JSONModel();
  JSONModel *node;
  for (const auto &attr : json_schema.path_order_) {
    std::vector<std::string> attr_path = attr.first;
    NodeType attr_type = Num2NodeType(json_schema.path_type_[attr.second]);
    node = root;

    for (int i = 0; i < attr_path.size(); ++i) {
      std::string name = attr_path[i];
      if (name.empty()) {
        if (!node->array_member_) {
          // node->array_size_squid_ = std::make_unique<TableNumerical>(true, 1);
          node->array_size_squid_ = std::make_unique<TableCategorical>();
          node->array_member_ = std::make_unique<JSONModel>("");
          node->types_.insert(NodeType::kArrayType);
        }

        node = node->GetArrayNode();
      } else {
        if (!node->GetMember(name)) {
          node->object_members_.push_back(std::make_unique<JSONModel>(name));
          node->members_order_.push_back(node->members_order_.size());
          node->types_.insert(NodeType::kObjectType);
        }

        node = node->GetMember(name);
      }

      if (i == attr_path.size() - 1) {
        // TODO(yiqiao): Support multi-type for one node.
        node->node_id_ = attr.second;
        node->types_.insert(attr_type);
        switch (attr_type) {
          case NodeType::kNumberType:
            // Here, we assume that all nodes are independent.
            node->number_model_ = std::make_unique<TableNumerical>(
                json_schema.path_type_, std::vector<size_t>(), attr.second, 1, true);
            break;

          case NodeType::kDoubleType:
            if (name.empty()) {
              node->ts_model_ =
                  std::make_unique<TableTimeSeries>(json_schema.path_type_, std::vector<size_t>(),
                                                    attr.second, kTimeSeriesPrecision, false);
            }
            node->number_model_ = std::make_unique<TableNumerical>(
                json_schema.path_type_, std::vector<size_t>(), attr.second, 0.0025, false);
            break;

          case NodeType::kStringType:
            node->string_model_ = std::make_unique<StringModel>(attr.second);
            break;
          default:
            break;
        }
      }
    }
  }

  return root;
}

void JSONModel::WriteModel(SequenceByteWriter *byte_writer) {
  byte_writer->Write16Bit(node_id_);
  byte_writer->Write16Bit(members_order_.size());
  for (int idx : members_order_) byte_writer->Write16Bit(idx);

  exist_squid_->WriteModel(byte_writer);
  type_squid_->WriteModel(byte_writer);
  if (array_member_) {
    byte_writer->WriteLess(1, 1);
    array_size_squid_->WriteModel(byte_writer);
  } else
    byte_writer->WriteLess(0, 1);

  if (string_model_ != nullptr) {
    byte_writer->WriteLess(1, 1);
    string_model_->WriteModel(byte_writer);
  } else
    byte_writer->WriteLess(0, 1);

  if (number_model_ != nullptr) {
    byte_writer->WriteLess(1, 1);
    byte_writer->WriteLess(number_model_->target_int_, 1);
    number_model_->WriteModel(byte_writer);
  } else
    byte_writer->WriteLess(0, 1);

  if (ts_model_ != nullptr) {
    byte_writer->WriteLess(1, 1);
    ts_model_->WriteModel(byte_writer);
  } else {
    byte_writer->WriteLess(0, 1);
  }

  if (array_member_) array_member_->WriteModel(byte_writer);

  for (auto &member_node : object_members_) member_node->WriteModel(byte_writer);
}

JSONModel *JSONModel::ReadModel(const JSONSchema &json_schema, ByteReader *byte_writer) {
  JSONModel *root = ReadJSONNode(byte_writer, json_schema);
  JSONModel *node = nullptr;
  for (const auto &attr : json_schema.path_order_) {
    std::vector<std::string> attr_path = attr.first;
    node = root;

    for (const std::string &name : attr_path) {
      if (name.empty()) {
        if (!node->array_member_) {
          node->array_member_.reset(ReadJSONNode(byte_writer, json_schema));
          node->array_member_->name_ = name;
          node->types_.insert(NodeType::kArrayType);
        }

        node = node->GetArrayNode();
      } else {
        if (!node->GetMember(name)) {
          std::unique_ptr<JSONModel> member_node(ReadJSONNode(byte_writer, json_schema));
          member_node->name_ = name;
          node->object_members_.push_back(std::move(member_node));
          node->types_.insert(NodeType::kObjectType);
        }

        node = node->GetMember(name);
      }
    }
  }

  return root;
}

JSONModel *JSONModel::ReadJSONNode(ByteReader *byte_reader, const JSONSchema &json_schema) {
  auto *node = new JSONModel();
  node->node_id_ = static_cast<int>(byte_reader->Read16Bit());
  node->members_order_.resize(byte_reader->Read16Bit());
  if (!node->members_order_.empty()) {
    node->types_.insert(NodeType::kObjectType);
    for (int i = 0; i < node->members_order_.size(); ++i)
      node->members_order_[i] = byte_reader->Read16Bit();
  }

  Schema schema(json_schema.path_type_);
  node->exist_squid_.reset(static_cast<TableCategorical *>(
      GetAttrModel(0)->ReadModel(byte_reader, schema, node->node_id_)));
  node->type_squid_.reset(static_cast<TableCategorical *>(
      GetAttrModel(0)->ReadModel(byte_reader, schema, node->node_id_)));

  if (byte_reader->ReadBit()) {
    node->types_.insert(NodeType::kArrayType);
    node->array_size_squid_.reset(static_cast<TableCategorical *>(
        GetAttrModel(0)->ReadModel(byte_reader, Schema(), node->node_id_)));
    //    node->array_size_squid_.reset(static_cast<TableNumerical *>(
    //        GetAttrModel(6)->ReadModel(byte_reader, Schema(), node->node_id_)));
  }

  if (byte_reader->ReadBit()) {
    node->types_.insert(NodeType::kStringType);
    node->string_model_.reset(static_cast<StringModel *>(
        GetAttrModel(5)->ReadModel(byte_reader, schema, node->node_id_)));
  }

  if (byte_reader->ReadBit()) {
    bool target_int = byte_reader->ReadBit();
    if (target_int) {
      node->types_.insert(NodeType::kNumberType);
      node->number_model_.reset(static_cast<TableNumerical *>(
          (GetAttrModel(6)->ReadModel(byte_reader, schema, node->node_id_))));
    } else {
      node->types_.insert(NodeType::kDoubleType);
      node->number_model_.reset(static_cast<TableNumerical *>(
          (GetAttrModel(8)->ReadModel(byte_reader, schema, node->node_id_))));
    }
  }

  if (byte_reader->ReadBit()) {
    node->types_.insert(NodeType::kTimeSeriesType);
    node->ts_model_.reset(static_cast<TableTimeSeries *>(
        GetAttrModel(7)->ReadModel(byte_reader, schema, node->node_id_)));
  }

  return node;
}

void JSONModel::FeedNodeExist(int is_existed) const {
  exist_squid_->FeedAttrs(AttrValue(is_existed), 1);
}
void JSONModel::FeedNodeType(int node_type) const {
  type_squid_->FeedAttrs(AttrValue(node_type), 1);
}
void JSONModel::FeedArraySize(int array_size) const {
  array_size_squid_->FeedAttrs(AttrValue(array_size), 1);
}

void JSONModel::GetProbIntervalsExist(int is_existed, std::vector<Branch *> &prob_intervals,
                                      int &prob_intervals_index) const {
  if (exist_squid_->GetSimpleSquidValue() != 65535) return;

  CategoricalSquID *squid = exist_squid_->GetSquID();
  squid->GetProbIntervals(prob_intervals, prob_intervals_index, AttrValue(is_existed));
}

void JSONModel::GetProbIntervalsNodeType(int node_type, std::vector<Branch *> &prob_intervals,
                                         int &prob_intervals_index) const {
  if (type_squid_->GetSimpleSquidValue() != 65535) return;

  CategoricalSquID *squid = type_squid_->GetSquID();
  squid->GetProbIntervals(prob_intervals, prob_intervals_index, AttrValue(node_type));
}

void JSONModel::GetProbIntervalsArraySize(int array_size, std::vector<Branch *> &prob_intervals,
                                          int &prob_intervals_index) const {
  //  NumericalSquID *squid = array_size_squid_->GetSquID();
  //  squid->GetProbIntervals(prob_intervals, prob_intervals_index, AttrValue(array_size));
  CategoricalSquID *squid = array_size_squid_->GetSquID();
  squid->GetProbIntervals(prob_intervals, prob_intervals_index, AttrValue(array_size));
}

int JSONModel::DecompressNodeExist(Decoder *decoder, ByteReader *byte_reader) {
  if (exist_squid_->GetSimpleSquidValue() != 65535) return exist_squid_->GetSimpleSquidValue();

  CategoricalSquID *squid = exist_squid_->GetSquID();
  squid->Decompress(decoder, byte_reader);
  return squid->GetResultAttr().Int();
}

int JSONModel::DecompressNodeType(Decoder *decoder, ByteReader *byte_reader) {
  if (type_squid_->GetSimpleSquidValue() != 65535) return type_squid_->GetSimpleSquidValue();

  CategoricalSquID *squid = type_squid_->GetSquID();
  squid->Decompress(decoder, byte_reader);
  return squid->GetResultAttr().Int();
}
int JSONModel::DecompressArraySize(Decoder *decoder, ByteReader *byte_reader) {
  // NumericalSquID *squid = array_size_squid_->GetSquID();
  CategoricalSquID *squid = array_size_squid_->GetSquID();
  squid->Decompress(decoder, byte_reader);
  // return squid->GetResultAttr(true).Int();

  return squid->GetResultAttr().Int();
}

bool JSONModel::SetPredictors(std::vector<size_t> predictors, const JSONSchema &json_schema) {
  bool success = true;

  if (types_.count(NodeType::kStringType) == 0 && types_.count(NodeType::kNumberType) == 0 &&
      types_.count(NodeType::kDoubleType) == 0) {
    std::cerr << "JSONModel::SetPredictors: No string or number type node found. It is not a leaf "
                 "node.\n";
  }

  // set numerical model
  if (number_model_ != nullptr) {
    ModelCreator *creator = GetAttrModel(json_schema.path_type_[node_id_]);
    std::unique_ptr<TableNumerical> model(static_cast<TableNumerical *>(
        creator->CreateModel(json_schema.path_type_, predictors, node_id_, 0.5)));

    if (model == nullptr)
      success = false;
    else
      number_model_ = std::move(model);
  }

  // set string model
  if (string_model_ != nullptr) {
    ModelCreator *creator = GetAttrModel(json_schema.path_type_[node_id_]);
    std::unique_ptr<StringModel> model(static_cast<StringModel *>(
        creator->CreateModel(json_schema.path_type_, predictors, node_id_, 0.5)));

    if (model == nullptr)
      success = false;
    else
      string_model_ = std::move(model);
  }

  // set time series model
  if (ts_model_ != nullptr) {
    ModelCreator *creator = GetAttrModel(json_schema.path_type_[node_id_]);
    std::unique_ptr<TableTimeSeries> model(static_cast<TableTimeSeries *>(
        creator->CreateModel(json_schema.path_type_, predictors, node_id_, 0.0000001)));

    if (model == nullptr)
      success = false;
    else
      ts_model_ = std::move(model);
  }

  if (success) predictors_ = predictors;
  return success;
}

void JSONModel::SetObjectOrdering(const std::vector<size_t> &members_order) {
  // validate check
  for (size_t idx : members_order) {
    if (idx >= object_members_.size()) {
      std::cerr << "JSONModel::SetObjectOrdering: member_order is not valid.\n";
      return;
    }
  }
  members_order_ = members_order;

  // add left leaf nodes into the ordering.
  for (size_t i = 0; i < object_members_.size(); ++i) {
    auto &member = object_members_[i];
    if (member->types_.count(NodeType::kNumberType) ||
        member->types_.count(NodeType::kStringType) ||
        member->types_.count(NodeType::kDoubleType)) {
      bool existed = false;
      for (size_t idx : members_order_) {
        if (i == idx) {
          existed = true;
          break;
        }
      }
      if (!existed) members_order_.push_back(i);
    }
  }

  // add non-leaf nodes into the ordering, note that node could be different types, so we need to
  // check the repetition of node id.
  for (size_t i = 0; i < object_members_.size(); ++i) {
    auto &member = object_members_[i];
    if (member->types_.count(NodeType::kObjectType) || member->types_.count(NodeType::kArrayType)) {
      bool existed = false;
      for (size_t idx : members_order_) {
        if (i == idx) {
          existed = true;
          break;
        }
      }
      if (!existed) members_order_.push_back(i);
    }
  }
}

JSONModel *JSONModel::GetMember(const std::string &node_name) {
  for (std::unique_ptr<JSONModel> &member_node : object_members_) {
    if (member_node->name_ == node_name) {
      return member_node.get();
    }
  }
  return nullptr;
}

size_t JSONModel::GetMemberIndex(size_t id) {
  for (size_t i = 0; i < object_members_.size(); ++i)
    if (object_members_[i]->node_id_ == id) return i;

  return -1;
}

void JSONModel::EndOfData() {
  exist_squid_->EndOfData();
  type_squid_->EndOfData();
  for (auto &member_node : object_members_) member_node->EndOfData();

  if (array_member_) {
    array_size_squid_->EndOfData();
    array_member_->EndOfData();
  }
  if (number_model_ != nullptr) number_model_->EndOfData();
  if (string_model_ != nullptr) string_model_->EndOfData();
  if (ts_model_ != nullptr) ts_model_->EndOfData();
}

int JSONModel::GetModelCost() const {
  int ret = exist_squid_->GetModelCost() + type_squid_->GetModelCost();
  if (string_model_) ret += string_model_->GetModelCost();
  if (number_model_) ret += number_model_->GetModelCost();
  if (ts_model_) ret += ts_model_->GetModelCost();
  if (array_size_squid_) ret += array_size_squid_->GetModelCost();

  return ret;
}
}  // namespace db_compress
