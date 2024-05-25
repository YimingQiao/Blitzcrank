#include "../include/decompression.h"

#include <utility>
#include <vector>

#include "../include/base.h"
#include "../include/categorical_model.h"
#include "../include/numerical_model.h"
#include "../include/string_model.h"

namespace db_compress {

namespace {

SquIDModel *GetModelFromDescription(ByteReader *byte_reader,
                                    const Schema &schema, size_t index) {
  return GetAttrModel(schema.attr_type_[index])
      ->ReadModel(byte_reader, schema, index);
}

} // anonymous namespace

RelationDecompressor::RelationDecompressor(const char *compressedFileName,
                                           Schema schema, int block_size)
    : schema_(std::move(schema)), byte_reader_(compressedFileName),
      index_reader_(compressedFileName), tuple_idx_(0), num_wanted_tuples_(1),
      num_converted_tuples_(0), bit_buffer_(64), bit_buffer_index_(0) {}

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
    std::unique_ptr<SquIDModel> model(
        GetModelFromDescription(&byte_reader_, schema_, i));
    model_[i] = std::move(model);
  }

  // Default: decompress the whole data set
  num_unwanted_tuples_ = 0;
  num_wanted_tuples_ = num_total_tuples_;
  num_todo_tuples_ = num_total_tuples_;

  // init index
  index_reader_.Init();
  data_pos_ = byte_reader_.Tellg();
}

void RelationDecompressor::LocateTuple(int tuple_idx) {
  //    if (tuple_idx >= num_total_tuples_) {
  //      std::cerr << "Tuple Index out of range. \n";
  //    } else {
  //      std::cout << "Decompress tuple " << tuple_idx << ".\n";
  //    }

  num_unwanted_tuples_ =
      index_reader_.LocateBlock(num_bytes_, num_bits_, tuple_idx);
  byte_reader_.Seekg(num_bytes_ + data_pos_, num_bits_, std::ios_base::beg);

  tuple_idx_ = tuple_idx;
  num_wanted_tuples_ = 1;
  num_todo_tuples_ = num_unwanted_tuples_ + num_wanted_tuples_;
  num_converted_tuples_ = 0;
  decoder_.InitProbInterval();
}

void RelationDecompressor::ReadNextTuple(AttrVector *tuple) {
  InitPIb(decoder_.GetPIb());

  for (int attr_index : attr_order_) {
    switch (schema_.attr_type_[attr_index]) {
    case 0: {
      auto model = static_cast<TableCategorical *>(model_[attr_index].get());
      CategoricalSquID *squid = model->GetSquID(*tuple);
      squid->Decompress(&decoder_, &byte_reader_);
      // tuple->attr_[attr_index] = squid->GetResultAttr();
      break;
    }
    case 1: {
      auto model = static_cast<TableNumerical *>(model_[attr_index].get());
      NumericalSquID *squid = model->GetSquID(*tuple);
      squid->Decompress(&decoder_, &byte_reader_);
      tuple->attr_[attr_index] = squid->GetResultAttr();
      break;
    }
    case 2: {
      auto model = static_cast<TableNumerical *>(model_[attr_index].get());
      NumericalSquID *squid = model->GetSquID(*tuple);
      squid->Decompress(&decoder_, &byte_reader_);
      tuple->attr_[attr_index] = squid->GetResultAttr();
      break;
    }
    case 3: {
      auto model = static_cast<StringModel *>(model_[attr_index].get());
      StringSquID *squid = model->GetSquID(*tuple);
      squid->Decompress(&decoder_, &byte_reader_);
      tuple->attr_[attr_index] = squid->GetResultAttr();
      break;
    }
    }
  }
  EmitAdditionBits(decoder_.GetPIb(), decoder_.GetPIt());

  ++num_converted_tuples_;
  //  if (num_converted_tuples_ % 100000 == 0) {
  //    std::cout << "Decompressed Tuple: " << num_converted_tuples_ << "\n";
  //  }
}
void RelationDecompressor::InitPIb(UnitProbInterval &PIb) {
//  decoder_.InitProbInterval();
//  for (int i = bit_buffer_index_ - 1; i >= 0; --i)
//    PIb.Go(bit_buffer_[i]);

//  bit_buffer_index_ = 0;

  while (PIb.exp_ < 32) {
    PIb.GoByte(byte_reader_.ReadByte());
  }
  while (PIb.exp_ < 40) {
    PIb.Go(byte_reader_.ReadBit());
  }
}
void RelationDecompressor::EmitAdditionBits(UnitProbInterval &PIb,
                                            const ProbInterval &PIt) {
//  while (PIb.IncludedBy(PIt))
//    bit_buffer_[bit_buffer_index_++] = PIb.Back();
//
//  // the last bit should be taken, since PIb needs it to be included by PIt
//  --bit_buffer_index_;
}

} // namespace db_compress
