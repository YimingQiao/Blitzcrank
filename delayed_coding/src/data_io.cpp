#include "../include/data_io.h"

#include <iostream>
#include <string>
#include <vector>

#include "../include/base.h"
#include "../include/blitzcrank_exception.h"

namespace db_compress {

SequenceByteWriter::SequenceByteWriter(const std::string &file_name)
    : file_(file_name, std::ios::binary), buffer_{0}, bits_counter_(0) {
  if (!file_) throw IOException("Cannot open file " + file_name + " for writing.\n");
}
// Write all the remaining, we can use bits_counter_ to determine the
// size.
SequenceByteWriter::~SequenceByteWriter() {
  file_.write(buffer_, (bits_counter_ >> 3) + static_cast<int>((bits_counter_ & 7) > 0));
}
void SequenceByteWriter::WriteLess(unsigned char byte, size_t len) {
  // stats
  num_bits_ += len;

  const int byte_index = bits_counter_ >> 3;
  const int bits_index = bits_counter_ & 7;

  if (len <= 8 - bits_index) {
    buffer_[byte_index] += byte << (8 - len - bits_index);
  } else {
    buffer_[byte_index] += byte >> (len + bits_index - 8);
    buffer_[byte_index + 1] += byte << (8 - (len + bits_index - 8));
  }
  bits_counter_ += len;

  if (bits_counter_ >= (sizeof(buffer_) - 1) * 8) {
    const int num_full_block = bits_counter_ >> 3;
    file_.write(buffer_, num_full_block);
    // the last block may be not full
    const char last_byte = buffer_[num_full_block];
    bits_counter_ &= 7;
    std::fill_n(buffer_, sizeof(buffer_), 0);
    buffer_[0] = last_byte;
  }
}

void SequenceByteWriter::WriteByte(unsigned char byte) { WriteLess(byte, 8); }

void SequenceByteWriter::Write16Bit(unsigned int val) {
  val &= 65535;
  WriteByte(val >> 8);
  WriteByte(val & 255);
}

void SequenceByteWriter::Write32Bit(unsigned char bytes[4]) {
  WriteByte(bytes[0]);
  WriteByte(bytes[1]);
  WriteByte(bytes[2]);
  WriteByte(bytes[3]);
}
void SequenceByteWriter::Write32Bit(uint32_t data) {
  Write16Bit(data >> 16);
  Write16Bit(data & 0xffff);
}
void SequenceByteWriter::WriteUint64(const uint64_t data) {
  unsigned high = data >> 32;
  unsigned low = data & 0xffffffff;
  Write32Bit(high);
  Write32Bit(low);
}

// ByteReader::ByteReader(const std::string &file_name)
//     : fin_(file_name, std::ios::binary), buffer_(0), buffer_len_(0) {
//   if (!fin_) throw IOException("Cannot open file " + file_name + ".\n");
// }
// ByteReader::~ByteReader() { fin_.close(); }
// unsigned char ByteReader::ReadByte() {
//   if (buffer_len_ < 8) {
//     buffer_ = ((buffer_ << 8) | fin_.get());
//     buffer_len_ += 8;
//   }
//   buffer_len_ -= 8;
//   unsigned char ret = (buffer_ >> buffer_len_) & 0xff;
//   buffer_ ^= ret << buffer_len_;
//   return ret;
// }
// bool ByteReader::ReadBit() {
//   if (buffer_len_ == 0) {
//     buffer_ = ((buffer_ << 8) | fin_.get());
//     buffer_len_ += 8;
//   }
//   --buffer_len_;
//   bool ret = (buffer_ >> buffer_len_) & 1;
//   buffer_ ^= ret << buffer_len_;
//   return ret;
// }
// unsigned int ByteReader::Read16Bit() {
//   while (buffer_len_ < 16) {
//     buffer_ = ((buffer_ << 8) | fin_.get());
//     buffer_len_ += 8;
//   }
//   buffer_len_ -= 16;
//   unsigned int ret = (buffer_ >> buffer_len_) & 0xffff;
//   buffer_ ^= ret << buffer_len_;
//   return ret;
// }
// int ByteReader::Read32Bit() { return ((Read16Bit()) << 16) + Read16Bit(); }
//
// void ByteReader::Read32Bit(unsigned char *bytes) {
//   for (int i = 0; i < 4; ++i) {
//     if (buffer_len_ < 8) {
//       buffer_ = ((buffer_ << 8) | fin_.get());
//       buffer_len_ += 8;
//     }
//     buffer_len_ -= 8;
//     unsigned char ret = (buffer_ >> buffer_len_) & 0xff;
//     buffer_ ^= ret << buffer_len_;
//     bytes[i] = ret;
//   }
// }
//
// uint64_t ByteReader::ReadUint64() {
//   uint64_t data = Read32Bit();
//   unsigned low = Read32Bit();
//   data = (data << 32) + low;
//
//   return data;
// }

}  // namespace db_compress
