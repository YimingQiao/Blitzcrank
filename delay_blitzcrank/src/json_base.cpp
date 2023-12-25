#include "../include/json_base.h"

#include <fstream>
#include <memory>
namespace db_compress {

void PrintObject(const rapidjson::Value &object) {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  object.Accept(writer);

  // Output {"project":"rapidjson","stars":11}
  std::cout << buffer.GetString() << std::endl;
}

NodeType Num2NodeType(int node_type) {
  switch (node_type) {
    case 0:
      return kNullType;
    case 1:
      return kFalseType;
    case 2:
      return kTrueType;
    case 3:
      return kObjectType;
    case 4:
      return kArrayType;
    case 5:
      return kStringType;
    case 6:
      return kNumberType;
    case 7:
      return kDoubleType;
    case 8:
      return kTimeSeriesType;
    default:
      std::cerr << "Unknown node type: " << node_type << std::endl;
      return kNullType;
  }
}

NodeType GetNodeType(const rapidjson::Value &node) {
  NodeType node_type = Num2NodeType(node.GetType());
  if (node_type == kNumberType && node.IsDouble()) node_type = kDoubleType;

  if (node_type == kArrayType) {
    // Time Series Check
    int array_size = static_cast<int>(node.GetArray().Size());

    bool is_time_series = (array_size > 0);
    for (int i = 0; i < array_size; ++i) {
      if (!node[i].IsDouble()) {
        is_time_series = false;
        break;
      }
    }
    if (is_time_series) return kTimeSeriesType;
  }

  return node_type;
}

std::vector<std::string> JSONSchema::ExtractLine(const std::string &delimiter, std::string line) {
  std::vector<std::string> ret;
  size_t pos;
  std::string token;
  while ((pos = line.find(delimiter)) != std::string::npos) {
    token = line.substr(0, pos);
    ret.push_back(token);
    line.erase(0, pos + 2);
  }
  ret.push_back(line);

  return ret;
}
void JSONSchema::WriteJSONSchema() {
  std::ofstream config;
  config.open(config_file_name_);

  config << "Attribute path: \n";
  // sort paths based on original json
  std::vector<std::vector<std::string>> sorted_path(path_order_.size());
  for (const auto &pair : path_order_) {
    sorted_path[pair.second] = pair.first;
  }
  for (const auto &path : sorted_path) {
    for (int i = 0; i < path.size(); ++i) {
      std::string name = path[i];
      config << name << ((i == path.size() - 1) ? "\n" : ", ");
    }
  }

  config << "\n"
         << "Attribute Type: \n";
  for (int i = 0; i < path_type_.size(); ++i) {
    config << path_type_[i] << ((i == path_type_.size() - 1) ? "\n" : ", ");
  }

  config.close();
}

void JSONSchema::LoadJSONSchema() {
  std::ifstream config;
  config.open(config_file_name_);

  // load attribute path
  std::string string;
  std::getline(config, string);
  if (string.back() == '\r')
    string.pop_back();
  int count = 0;
  while (true) {
    std::getline(config, string);
    if (string.back() == '\r')
      string.pop_back();

    std::vector<std::string> path = ExtractLine(", ", string);
    if (path[0].empty()) {
      break;
    }
    path_order_[path] = count++;
  }

  // load attribute type
  std::getline(config, string);
  if (string.back() == '\r')
    string.pop_back();
  std::getline(config, string);
  if (string.back() == '\r')
    string.pop_back();
  if (!string.empty()) {
    std::vector<std::string> attr_type_string = ExtractLine(", ", string);
    for (const std::string &num : attr_type_string) {
      path_type_.push_back(std::stoi(num));
    }
  }
}

db_compress::JSONSchema JSONSchemaGenerator::GenerateSchema(char *file_name) {
  FILE *file_ptr = fopen(file_name, "r");  // non-Windows use "r"
  if (file_ptr == nullptr) std::cerr << "Load failure.\n";
  char read_buffer[65536];
  rapidjson::FileReadStream stream(file_ptr, read_buffer, sizeof(read_buffer));

  rapidjson::Document json;
  while (!json.ParseStream<rapidjson::kParseStopWhenDoneFlag>(stream).HasParseError()) {
    cur_path.resize(0);
    rapidjson::Value &object = json.GetObject();
    ParseObject(object);
  }
  if (json.GetParseError() != rapidjson::kParseErrorDocumentEmpty) {
    fprintf(stderr, "\nError(offset %u): %s\n", static_cast<unsigned>(json.GetErrorOffset()),
            rapidjson::GetParseError_En(json.GetParseError()));
    db_compress::PrintObject(json);
  }
  fclose(file_ptr);

  // sort path alphabetically
  std::sort(path_names.begin(), path_names.end(), db_compress::VectorStringComparator());
  std::vector<int> path_type_sorted;
  for (auto &path : path_names) {
    int index = path_order[path];
    path_order[path] = static_cast<int>(path_type_sorted.size());
    path_type_sorted.push_back(attr_type[index]);
  }

  return {config_file_name_, path_order, path_type_sorted};
}

void JSONSchemaGenerator::RecordAttr(int type) {
  if (path_order.count(cur_path) == 1) return;

  std::vector<std::string> path(cur_path);
  path_order.insert(std::pair<std::vector<std::string>, int>(path, path_order.size()));
  attr_type.push_back(type);
  path_names.push_back(path);
}

void JSONSchemaGenerator::ParseObject(rapidjson::Value &object) {
  std::sort(object.MemberBegin(), object.MemberEnd(), db_compress::NodeNameComparator());
  for (auto &itr : object.GetObject()) {
    int type = Num2NodeType(itr.value.GetType());

    cur_path.emplace_back(itr.name.GetString());
    switch (type) {
      case kNullType:
        RecordAttr(0);
        break;
      case kFalseType:
        RecordAttr(1);
        break;
      case kTrueType:
        RecordAttr(2);
        break;
      case kObjectType:
        ParseObject(itr.value.GetObject());
        break;
      case kArrayType:
        ParseArray(itr.value.GetArray());
        break;
      case kStringType:
        RecordAttr(5);
        break;
      case kNumberType:
        if (itr.value.IsInt())
          RecordAttr(6);
        else
          RecordAttr(7);

        break;
      default:
        std::cerr << "ParseObject: Unrecognized Type!\n";
    }
    cur_path.pop_back();
  }
}

void JSONSchemaGenerator::ParseArray(rapidjson::Value &array) {
  if (!array.IsArray()) std::cerr << "It is not an array.\n";

  cur_path.emplace_back("");
  // Assumption: elements of an array have the same type.
  for (int i = 0; i < array.GetArray().Size(); ++i) {
    rapidjson::Value &itr = array[i];
    int type = Num2NodeType(itr.GetType());

    switch (type) {
      case kNullType:
        RecordAttr(0);
        break;
      case kFalseType:
        RecordAttr(1);
        break;
      case kTrueType:
        RecordAttr(2);
        break;
      case kObjectType:
        ParseObject(itr.GetObject());
        break;
      case kArrayType:
        ParseArray(itr.GetArray());
        break;
      case kStringType:
        RecordAttr(5);
        break;
      case kNumberType:
        // time series
        if (itr.IsInt())
          RecordAttr(6);
        else
          RecordAttr(7);
        break;
      default:
        std::cerr << "ParseArray: Unrecognized Type!\n";
    }
  }
  cur_path.pop_back();
}
}  // namespace db_compress
