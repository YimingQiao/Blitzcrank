/**
 * @file json_model.h
 * @brief The header of JSON Model, which is an abstract of a node in a json tree.
 */

#ifndef DB_COMPRESS_JSON_MODEL_H
#define DB_COMPRESS_JSON_MODEL_H
#include <iostream>
#include <utility>
#include <vector>

#include <document.h>
#include <prettywriter.h>
#include <stringbuffer.h>

#include "categorical_model.h"
#include "json_base.h"
#include "numerical_model.h"
#include "simple_prob_interval_pool.h"
#include "string_model.h"
#include "timeseries_model.h"

namespace db_compress {
/**
 * For each possible path (non-leaf path and leaf path), it has a JSONNode.
 * When come across a node (in decompression): i) since each path may have
 * various types, we determine its existence and type by calling
 * type_squid_.Decompress(); ii) if node is null, true, false, not existed or
 * terminal nodes, we get the result and end it; iii) or (it is object, array),
 * recursion continues.
 */
class JSONModel {
 public:
  // Path of JSONNode.
  std::string name_;
  // range of leaf node id: 0 - 65534 (16 bits). For non-leaf node, it is 65535.
  int node_id_ = 65535;
  // type of node, a node could have different types.
  std::set<NodeType> types_;
  std::vector<size_t> predictors_;

  // Object: parse its child nodes within a loop.
  std::vector<std::unique_ptr<JSONModel>> object_members_;
  std::vector<size_t> members_order_;

  // Array: each array only has one JSONNode, i.e. members of array are
  // described by the same JSONNode.
  std::unique_ptr<JSONModel> array_member_;

  // String/number/time series: node is a terminal node (leaf node). Terminal node has a
  // specific squid model, i.e. String model, categorical model, numerical
  // model or time series model. Here, categorical model is equal to numerical model with global
  // dictionary.
  std::unique_ptr<StringModel> string_model_;
  std::unique_ptr<TableNumerical> number_model_;
  std::unique_ptr<TableTimeSeries> ts_model_;

  JSONModel() = default;

  /**
   * Create a json node.
   *
   * @param name name of node
   */
  explicit JSONModel(std::string name) : name_(std::move(name)){};

  /**
   * The created json model has the same structure as the older, but the squid they have are
   * different.
   *
   * @param json_node JSONModel to be copied
   */
  JSONModel(JSONModel &json_node);

  /**
   * Get member from object.
   *
   * @param node_name member name
   * @return return object member node if it exists; or return nullptr
   */
  JSONModel *GetMember(const std::string &node_name);

  /**
   * Pass a leaf node index, return a leaf node.
   *
   * @param id index of leaf node
   * @return return object member node if it exists; or return nullptr
   */
  size_t GetMemberIndex(size_t id);

  /**
   * Get array member from node.
   *
   * @return return array member node if it exists; or return nullptr
   */
  JSONModel *GetArrayNode() const { return array_member_.get(); }

  bool SetPredictors(std::vector<size_t> predictors, const JSONSchema &json_schema);

  void SetObjectOrdering(const std::vector<size_t> &members_order);

  void EndOfData();

  int GetModelCost() const;

  void WriteModel(SequenceByteWriter *byte_writer);

  static JSONModel *ReadModel(const JSONSchema &json_schema, ByteReader *byte_writer);

  /**
   * Create a json tree based on json schema
   *
   * @param json_schema json schema contains leaf node path and types
   * @return root of json tree
   */
  static JSONModel *CreateJSONTree(const JSONSchema &json_schema);

  // Learning
  void FeedNodeExist(int is_existed) const;
  void FeedNodeType(int node_type) const;
  void FeedArraySize(int array_size) const;
  // Compression
  void GetProbIntervalsExist(int is_existed, std::vector<Branch *> &prob_intervals,
                             int &prob_intervals_index) const;
  void GetProbIntervalsNodeType(int node_type, std::vector<Branch *> &prob_intervals,
                                int &prob_intervals_index) const;
  void GetProbIntervalsArraySize(int array_size, std::vector<Branch *> &prob_intervals,
                                 int &prob_intervals_index) const;

  // Decompression
  int DecompressNodeExist(Decoder *decoder, ByteReader *byte_reader);
  int DecompressNodeType(Decoder *decoder, ByteReader *byte_reader);
  int DecompressArraySize(Decoder *decoder, ByteReader *byte_reader);

 private:
  // Node existence.
  std::unique_ptr<TableCategorical> exist_squid_ = std::make_unique<TableCategorical>();

  // Node type. Type values: null, false, true, object, array, string, number
  // (0 ~ 7). If value is in {null, true, false}, learn/compress/decompress.csv
  // and return.
  std::unique_ptr<TableCategorical> type_squid_ = std::make_unique<TableCategorical>();

  // Number of elements in an array
  //  std::unique_ptr<TableNumerical> array_size_squid_;
  std::unique_ptr<TableCategorical> array_size_squid_;

  // stat
  uint64_t bits_{0};

  static JSONModel *ReadJSONNode(ByteReader *byte_reader, const JSONSchema &json_schema);
};

void LearnNode(JSONModel *sketch, const rapidjson::Value &sample, AttrVector &attr_record_);

void GetProbInterval(JSONModel *sketch, const rapidjson::Value &sample, AttrVector &attr_record_,
                     std::vector<Branch *> &prob_intervals, int &prob_intervals_index);

}  // namespace db_compress

#endif  // DB_COMPRESS_JSON_MODEL_H
