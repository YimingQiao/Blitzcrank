/**
 * @file data_io.h
 * @brief Header file for data IO.
 */

#ifndef DATA_IO_H
#define DATA_IO_H

#include <fstream>
#include <iostream>
#include <vector>

#include "base.h"

namespace db_compress {

/**
 * SequenceByteWriter is a utility class that can be used to write bit strings
 * in sequence.
 */
class SequenceByteWriter {
 public:
  /**
   * Create a SequenceByteWriter.
   *
   * @param file_name compressed file address, user specified
   */
  explicit SequenceByteWriter(const std::string &file_name);

  /**
   * Deconstruct a SequenceByteWriter.
   */
  ~SequenceByteWriter();

  /**
   * Write the least significant (len) bits
   *
   * @param byte a byte to be written partly
   * @param len length of bits to be written from left to right
   */
  void WriteLess(unsigned char byte, size_t len);

  /**
   * Write a byte at once.
   *
   * @param byte a byte to be written
   */
  void WriteByte(unsigned char byte);

  /**
   * Write 16 bits at once.
   *
   * @param val unsigned int is 32 bits, write the lower 16 bits
   */
  void Write16Bit(unsigned int val);

  /**
   * Write 32 bits at once.
   *
   * @param byte byte is an array of 4 char
   */
  void Write32Bit(unsigned char byte[4]);

  /**
   * Write 32 bits at once.
   *
   * @param data it is 64 bits, write the lower 32 bits
   */
  void Write32Bit(unsigned int data);

  /**
   * Write a uint_64
   */
  void WriteUint64(uint64_t data);

  uint64_t GetNumBits() { return num_bits_; }

  void ClearNumBits() { num_bits_ = 0; }

 private:
  std::ofstream file_;
  // 64 KB buffer
  char buffer_[65537];
  // 0 - 8191 * 8 (8192 * 8)
  uint64_t bits_counter_;

  // stats
  uint64_t num_bits_{0};
};

/**
 * ByteReader is a utility class that can be used to read bit strings.
 * It allows us to read in single bit at each time.
 */
// class ByteReader {
//  public:
//   /**
//    * Create a ByteReader.
//    *
//    * @param file_name compressed file address
//    */
//   explicit ByteReader(const std::string &file_name);
//
//   /**
//    * Deconstruct a ByteReader.
//    */
//   ~ByteReader();
//
//   /**
//    * Read a bit.
//    *
//    * @return return one bit.
//    */
//   bool ReadBit();
//
//   /**
//    * Read a byte.
//    *
//    * @return return a unsigned char of 8 bits
//    */
//   unsigned char ReadByte();
//
//   /**
//    * Read 16 bits.
//    *
//    * @return return a unsigned int of 32 bits, the result is in the lower 16
//    * bits.
//    */
//   unsigned int Read16Bit();
//
//   /**
//    * Read a int.
//    *
//    * @return return a int variable
//    */
//   int Read32Bit();
//
//   /**
//    * Read 32 bits.
//    *
//    * @param bytes return an unsigned char array of length 4
//    */
//   void Read32Bit(unsigned char *bytes);
//
//   /**
//    * Read an uint_64 number
//    */
//   uint64_t ReadUint64();
//
//   /**
//    * Sets the position of the next character to be extracted from the input
//    * stream. The next position is 8 * num_bytes_ + num_bits_ bits relative to
//    * current position.
//    *
//    * @param num_bytes distance between current position and next character to
//    * read in bytes.
//    * @param num_bits how many bits left to reach the desired position
//    * @param way this parameter is the same as input way in function 'seekg'
//    */
//   void Seekg(int64_t num_bytes, unsigned char num_bits, std::ios_base::seekdir way) {
//     if (buffer_len_ != 0) {
//       std::cerr
//           << "Seekg: buffer is not empty, please make sure buffer is empty before calling
//           Seekg\n";
//       return;
//     }
//
//     fin_.seekg(num_bytes, way);
//     for (int i = 0; i < num_bits; ++i) ReadBit();
//   }
//
//   int Tellg() { return fin_.tellg(); }
//
//  private:
//   std::ifstream fin_;
//   unsigned int buffer_, buffer_len_;
// };

/**
 * ByteReader firstly loads all data into memory, then output data from memory.
 */
class ByteReader {
 public:
  std::vector<unsigned char> stream_;

  /**
   * Create a in-memory byte reader
   *
   * @param file_name compressed file address
   */
  explicit ByteReader(const std::string &file_name) : fin_(file_name, std::ios::binary) {
    if (!fin_.is_open()) std::cout << "Cannot open file " << file_name << "\n";
    while (!fin_.eof()) stream_.push_back(fin_.get());
    // pop the last byte '255'.
    stream_.pop_back();
    fin_.close();
  }

  /**
   * Read a bit.
   *
   * @return return one bit.
   */
  bool ReadBit() {
    uint32_t byte_idx = position_ >> 3;
    uint8_t bit_idx = position_ & 7;
    bool ret = (stream_[byte_idx] >> (7 - bit_idx)) & 1;

    position_++;
    return ret;
  }

  /**
   * Read a byte.
   *
   * @return return a unsigned char of 8 bits
   */
  unsigned char ReadByte() {
    uint32_t byte_idx = position_ >> 3;
    uint8_t bit_idx = position_ & 7;
    uint8_t ret;
    if (bit_idx == 0)
      ret = stream_[byte_idx];
    else
      ret = (stream_[byte_idx] << bit_idx) | (stream_[byte_idx + 1] >> (8 - bit_idx));

    position_ += 8;
    return ret;
  }

  /**
   * Read 16 bits.
   *
   * @return return a unsigned int of 32 bits, the result is in the lower 16
   * bits.
   */
  inline unsigned int Read16Bit() {
    uint32_t byte_idx = position_ >> 3;
    uint8_t bit_idx = position_ & 7;
    uint16_t ret = (stream_[byte_idx] << (bit_idx + 8)) | (stream_[byte_idx + 1] << bit_idx);
    if (bit_idx != 0) ret |= stream_[byte_idx + 2] >> (8 - bit_idx);

    position_ += 16;
    return ret;
  }

  /**
   * If there is no bit read requirement, we can be faster.
   */
  inline uint32_t Read16BitFast() {
    uint32_t byte_idx = position_ >> 3;
    uint16_t ret = (stream_[byte_idx] << 8) | stream_[byte_idx + 1];
    position_ += 16;
    return ret;
  }

  /**
   * Read 32 bit.
   *
   * @return return a int variable
   */
  int Read32Bit() { return (Read16Bit() << 16) + Read16Bit(); }

  uint32_t ReadUint32() { return (Read16Bit() << 16) + Read16Bit(); }

  /**
   * Read 32 bits.
   *
   * @param bytes return an unsigned char array of length 4
   */
  void Read32Bit(unsigned char *bytes) {
    for (int i = 0; i < 4; ++i) bytes[i] = ReadByte();
  }

  /**
   * Read an uint_64 number
   */
  uint64_t ReadUint64() {
    uint64_t high = ReadUint32();
    uint32_t low = ReadUint32();

    return (high << 32) + low;
  }

  /**
   * Sets the position of the next character to be extracted from the input
   * stream. The next position is 8 * num_bytes_ + num_bits_ bits relative to
   * current position.
   *
   * @param num_bytes distance between current position and next character to
   * read in bytes.
   * @param num_bits how many bits left to reach the desired position
   * @param way this parameter is the same as input way in function 'seekg'
   */
  inline void Seekg(int64_t num_bytes, unsigned char num_bits, std::ios_base::seekdir way) {
    if (way == std::ios_base::beg)
      position_ = (num_bytes * 8) + num_bits;
    else if (way == std::ios_base::end)
      position_ = ((stream_.size()) << 3) + (num_bytes * 8) + num_bits;
    else if (way == std::ios_base::cur)
      position_ += (num_bytes * 8) + num_bits;
  }

  inline void SetPos(uint64_t pos) { position_ = pos; }

  inline uint64_t Tellg() { return position_; }

 private:
  std::ifstream fin_;
  uint64_t position_{0};
};

}  // namespace db_compress

#endif
