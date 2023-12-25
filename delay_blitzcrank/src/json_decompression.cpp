//
// Created by Qiao Yiming on 2022/2/16.
//

#include "../include/json_decompression.h"
#include <utility>
#include <vector>

#include "../include/base.h"
#include "../include/json_model.h"
#include "../include/numerical_model.h"
#include "../include/string_model.h"
namespace db_compress {
namespace {
struct MemberInfo {
  bool exist;
  std::string *name;
  rapidjson::Value value;
};
}  // namespace

JSONDecompressor::JSONDecompressor(rapidjson::Document::AllocatorType &allocator,
                                   const char *compressed_file_name, JSONSchema json_schema,
                                   int block_size)
    : json_schema_(std::move(json_schema)),
      allocator_(allocator),
      num_converted_nodes_(0),
      attr_record_(static_cast<int>(json_schema_.path_order_.size())),
      real_json_root_(rapidjson::kObjectType),
      string_buffer_pool_(json_schema_.path_order_.size() << 1),
      kBlockSizeThreshold(block_size),
      byte_reader_(compressed_file_name) {}

void JSONDecompressor::Init() {
  // Number of tuples
  num_total_nodes_ = byte_reader_.Read32Bit();

  // Read Models
  sketch_root_.reset(JSONModel::ReadModel(json_schema_, &byte_reader_));
}

bool JSONDecompressor::HasNext() const { return num_converted_nodes_ < num_total_nodes_; }

rapidjson::Value &JSONDecompressor::ReadNextNode() {
  if (decoder_.CurBlockSize() > kBlockSizeThreshold / 10) decoder_.InitProbInterval();

  real_json_root_.RemoveAllMembers();
  DecompressNode(sketch_root_.get(), real_json_root_);

  num_converted_nodes_++;
  string_buffer_pool_.Clear();
  if (num_converted_nodes_ % 100000 == 0) {
    std::cout << "Decompressed Nodes: " << num_converted_nodes_ << "\n";
  }

  return real_json_root_;
}

void JSONDecompressor::DecompressNode(JSONModel *sketch_root, rapidjson::Value &sample) {
  sketch_stack_.push_back(sketch_root);
  node_stack_.push_back(&sample);

  while (!node_stack_.empty()) {
    rapidjson::Value &real_node = *node_stack_.back();
    JSONModel &sketch_node = *sketch_stack_.back();
    node_stack_.pop_back();
    sketch_stack_.pop_back();

    NodeType node_type = Num2NodeType(sketch_node.DecompressNodeType(&decoder_, &byte_reader_));
    switch (node_type) {
      case kNullType:
        real_node.SetNull();
        break;
      case kFalseType:
        real_node.SetBool(false);
        break;
      case kTrueType:
        real_node.SetBool(true);
        break;
      case kObjectType: {
        // node is object
        real_node.SetObject();
        for (int idx : sketch_node.members_order_) {
          auto &member_node = sketch_node.object_members_[idx];
          if (member_node->DecompressNodeExist(&decoder_, &byte_reader_)) {
            auto *name = member_node->name_.c_str();
            real_node.AddMember(rapidjson::StringRef(name), rapidjson::Value(), allocator_);
            sketch_stack_.push_back(member_node.get());
            node_stack_.push_back(&real_node[name]);
          }
        }

        break;
      }
      case kArrayType: {
        // node is array
        real_node.SetArray();
        int array_length = sketch_node.DecompressArraySize(&decoder_, &byte_reader_);
        real_node.Reserve(array_length, allocator_);
        for (int i = 0; i < array_length; ++i) {
          real_node.PushBack(rapidjson::Value(), allocator_);

          sketch_stack_.push_back(sketch_node.array_member_.get());
          node_stack_.push_back(&real_node[i]);
        }
        break;
      }
      case kStringType: {
        // node is string
        int attr_index = sketch_node.node_id_;
        StringSquID *squid = sketch_node.string_model_->GetSquID(attr_record_);
        squid->Decompress(&decoder_, &byte_reader_);
        attr_record_.attr_[attr_index] = squid->GetResultAttr();
        real_node = rapidjson::StringRef(
            string_buffer_pool_.BufferString(attr_record_.attr_[attr_index].String()));
        break;
      }
      case kNumberType: {
        // node is number
        int attr_index = sketch_node.node_id_;
        NumericalSquID *squid = sketch_node.number_model_->GetSquID(attr_record_);
        squid->Decompress(&decoder_, &byte_reader_);
        attr_record_.attr_[attr_index] = squid->GetResultAttr(true);
        real_node.SetInt(attr_record_.attr_[attr_index].Int());
        break;
      }
      case kDoubleType: {
        // node is number
        int attr_index = sketch_node.node_id_;
        NumericalSquID *squid = sketch_node.number_model_->GetSquID(attr_record_);
        squid->Decompress(&decoder_, &byte_reader_);
        attr_record_.attr_[attr_index] = squid->GetResultAttr(true);

        real_node.SetDouble(attr_record_.attr_[attr_index].Double());
        break;
      }
      case kTimeSeriesType: {
        // node is time series
        real_node.SetArray();
        int array_length = sketch_node.DecompressArraySize(&decoder_, &byte_reader_);
        real_node.Reserve(array_length, allocator_);

        std::vector<double> &time_series =
            sketch_node.GetArrayNode()->ts_model_->time_series_buffer_;
        if (time_series.size() < array_length) time_series.resize(array_length);

        TimeSeriesSquID *squid = sketch_node.GetArrayNode()->ts_model_->GetSquID(attr_record_);
        squid->Decompress(&decoder_, &byte_reader_, time_series, array_length);
        for (int i = 0; i < array_length; ++i)
          real_node.PushBack(rapidjson::Value().SetDouble(time_series[i]), allocator_);
        break;
      }
      default:
        std::cerr << "JSONDecompressor::DecompressNode - Unknown Type.\n";
        break;
    }
  }
}

const char *StringBufferPool::BufferString(const std::string &str) {
  if (string_buffer_.size() <= string_buffer_index_)
    string_buffer_.resize(string_buffer_.size() << 1);

  string_buffer_[string_buffer_index_] = std::make_unique<std::string>(str);
  return string_buffer_[string_buffer_index_++]->c_str();
}
}  // namespace db_compress
