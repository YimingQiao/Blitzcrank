#include "../include/decompression.h"

#include <utility>

#include "base.h"
#include "timeseries_model.h"

namespace db_compress {
namespace {
SquIDModel *GetModelFromDescription(ByteReader *byte_reader, const Schema &schema, size_t index) {
  return GetAttrModel(schema.attr_type_[index])->ReadModel(byte_reader, schema, index);
}
}  // anonymous namespace

RelationDecompressor::RelationDecompressor(const char *compressed_file_name, Schema schema,
                                           int block_size)
    : byte_reader_(compressed_file_name),
      kBlockSizeThreshold(block_size),
      schema_(std::move(schema)),
      index_reader_(),
      num_converted_tuples_(0) {}

void RelationDecompressor::Init() {
  // Number of tuples
  num_total_tuples_ = byte_reader_.Read32Bit();
  // Ordering of attributes
  for (size_t i = 0; i < schema_.attr_type_.size(); ++i) {
    attr_order_.push_back(byte_reader_.Read16Bit());
  }
  // Load models
  model_.resize(schema_.attr_type_.size());
  for (size_t i = 0; i < schema_.attr_type_.size(); ++i) {
    std::unique_ptr<SquIDModel> model(GetModelFromDescription(&byte_reader_, schema_, i));
    model_[i] = std::move(model);
  }

  // Default: decompress the whole data set
  num_todo_tuples_ = num_total_tuples_;

  // init index
  index_reader_.Init();
  data_pos_ = byte_reader_.Tellg();
}
void RelationDecompressor::LocateTuple(uint32_t tuple_idx) {
  assert(tuple_idx < num_total_tuples_);

  num_todo_tuples_ = index_reader_.LocateBlock(num_bytes_, tuple_idx) + 1;
  byte_reader_.SetPos(data_pos_ + (num_bytes_ << 3));

  tuple_idx_ = tuple_idx;
  num_converted_tuples_ = 0;
  decoder_.InitProbInterval();
}

void RelationDecompressor::ReadNextTuple(AttrVector *tuple) {
  if (decoder_.CurBlockSize() > kBlockSizeThreshold) decoder_.InitProbInterval();

  for (int attr_index : attr_order_) {
    switch (schema_.attr_type_[attr_index]) {
      case 0: {
        auto *model = static_cast<TableCategorical *>(model_[attr_index].get());
        CategoricalSquID *squid = model->GetSquID(*tuple);
        squid->Decompress(&decoder_, &byte_reader_);
        tuple->attr_[attr_index] = squid->GetResultAttr();
        break;
      }
      case 1: {
        auto *model = static_cast<TableNumerical *>(model_[attr_index].get());
        NumericalSquID *squid = model->GetSquID(*tuple);
        squid->Decompress(&decoder_, &byte_reader_);
        tuple->attr_[attr_index] = squid->GetResultAttr(true);
        break;
      }
      case 2: {
        auto *model = static_cast<TableNumerical *>(model_[attr_index].get());
        NumericalSquID *squid = model->GetSquID(*tuple);
        squid->Decompress(&decoder_, &byte_reader_);
        tuple->attr_[attr_index] = squid->GetResultAttr(true);
        break;
      }
      case 3: {
        auto *model = static_cast<StringModel *>(model_[attr_index].get());
        model->squid_.Decompress(&decoder_, &byte_reader_);
        tuple->attr_[attr_index] = model->squid_.GetResultAttr();
        break;
      }
      case 5: {
        auto *model = static_cast<TableMarkov *>(model_[attr_index].get());
        CategoricalSquID *squid = model->GetSquID(*tuple);
        squid->Decompress(&decoder_, &byte_reader_);
        tuple->attr_[attr_index] = squid->GetResultAttr();
        model->SetState(tuple->attr_[attr_index].Int());
        break;
      }
    }
  }
  num_converted_tuples_++;
  //  if (num_converted_tuples_ % 500000 == 0) {
  //    std::cout << "Decompressed Tuples: " << num_converted_tuples_ << "\n";
  //  }
}

void RelationDecompressor::ReadTargetTuple(size_t tuple_idx, AttrVector *tuple) {
  assert(tuple_idx < num_total_tuples_);
  num_bytes_ = index_reader_.LocateTuple(tuple_idx);
  byte_reader_.SetPos(data_pos_ + (num_bytes_ << 3));
  decoder_.InitProbInterval();

  for (int attr_index : attr_order_) {
    switch (schema_.attr_type_[attr_index]) {
      case 0: {
        auto *model = static_cast<TableCategorical *>(model_[attr_index].get());
        CategoricalSquID *squid = model->GetSquID(*tuple);
        squid->Decompress(&decoder_, &byte_reader_);
        // tuple->attr_[attr_index] = squid->GetResultAttr();
        break;
      }
      case 1: {
        auto *model = static_cast<TableNumerical *>(model_[attr_index].get());
        NumericalSquID *squid = model->GetSquID(*tuple);
        squid->Decompress(&decoder_, &byte_reader_);
        // tuple->attr_[attr_index] = squid->GetResultAttr(true);
        break;
      }
      case 2: {
        auto *model = static_cast<TableNumerical *>(model_[attr_index].get());
        NumericalSquID *squid = model->GetSquID(*tuple);
        squid->Decompress(&decoder_, &byte_reader_);
        // tuple->attr_[attr_index] = squid->GetResultAttr(true);
        break;
      }
      case 3: {
        auto *model = static_cast<StringModel *>(model_[attr_index].get());
        model->squid_.Decompress(&decoder_, &byte_reader_);
        // tuple->attr_[attr_index] = model->squid_.GetResultAttr();
        break;
      }
      case 5: {
        auto *model = static_cast<TableMarkov *>(model_[attr_index].get());
        CategoricalSquID *squid = model->GetSquID(*tuple);
        squid->Decompress(&decoder_, &byte_reader_);
        // tuple->attr_[attr_index] = squid->GetResultAttr();
        model->SetState(tuple->attr_[attr_index].Int());
        break;
      }
    }
  }
}
}  // namespace db_compress
