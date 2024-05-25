/**
 * @file json_base.h
 * @brief Header of index, which is used for random access
 */

#ifndef JSON_BASE_H
#define JSON_BASE_H

#include <iostream>
#include <map>
#include <set>
#include <utility>
#include <vector>

#include <document.h>
#include <error/en.h>
#include <filereadstream.h>
#include <prettywriter.h>
#include <stringbuffer.h>

#include "categorical_model.h"
#include "numerical_model.h"
#include "string_model.h"

namespace db_compress {
enum NodeType {
  kNullType,   /*!< null */
  kFalseType,  /*!< false */
  kTrueType,   /*!< true */
  kObjectType, /*!< object */
  kArrayType,  /*!< array */
  kStringType, /*!< string */
  kNumberType, /*!< number */
  kDoubleType,
  kTimeSeriesType
};
NodeType Num2NodeType(int node_type);

NodeType GetNodeType(const rapidjson::Value &node);

void PrintObject(const rapidjson::Value &object);

struct NodeNameComparator {
  bool operator()(const rapidjson::Value::Member &lhs, const rapidjson::Value::Member &rhs) const {
    return (strcmp(lhs.name.GetString(), rhs.name.GetString()) < 0);
  }
};

struct VectorStringComparator {
  bool operator()(const std::vector<std::string> &lhs, const std::vector<std::string> &rhs) const {
    std::string left_string;
    std::string right_string;
    for (const auto &str : lhs) {
      left_string.append(str);
    }
    for (const auto &str : rhs) {
      right_string.append(str);
    }
    return left_string < right_string;
  }
};

/**
 * Schema for json. It is either user defined or auto generated.
 */
class JSONSchema {
 public:
  std::map<std::vector<std::string>, int> path_order_;
  std::vector<int> path_type_;

  explicit JSONSchema(const char *config_file_name) : config_file_name_(config_file_name) {
    LoadJSONSchema();
  }

  JSONSchema(const char *config_file_name, std::map<std::vector<std::string>, int> attr_order,
             std::vector<int> &attr_type)
      : config_file_name_(config_file_name),
        path_type_(attr_type),
        path_order_(std::move(attr_order)) {}

  void WriteJSONSchema();

  std::vector<std::vector<std::string>> GetAttrPath() {
    std::vector<std::vector<std::string>> attr_path(path_type_.size());
    for (auto &pair : path_order_) {
      attr_path[pair.second] = pair.first;
    }
    return attr_path;
  }

 private:
  const char *config_file_name_;

  void LoadJSONSchema();

  static std::vector<std::string> ExtractLine(const std::string &delimiter, std::string line);
};

/**
 * If user does not provide a config file, we will use the generated one.
 */
class JSONSchemaGenerator {
 public:
  explicit JSONSchemaGenerator(char *config_file) : config_file_name_(config_file) {}

  db_compress::JSONSchema GenerateSchema(char *file_name);

 private:
  char *config_file_name_;

  std::vector<int> attr_type;
  std::vector<std::string> cur_path;
  std::vector<std::vector<std::string>> path_names;
  std::map<std::vector<std::string>, int> path_order;

  void RecordAttr(int type);

  void ParseObject(rapidjson::Value &object);

  void ParseArray(rapidjson::Value &array);
};
}  // namespace db_compress
#endif  // JSON_BASE_H
