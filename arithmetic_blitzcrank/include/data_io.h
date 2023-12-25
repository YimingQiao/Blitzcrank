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
   * @param num_of_tuple it is 64 bits, write the lower 32 bits
   */
  void WriteUnsigned(unsigned int num_of_tuple);

 private:
  std::ofstream file_;
  // 1 KB buffer
  char buffer_[1024];
  // 0 - 8191 (8192)
  int bits_counter_;
};

/**
 * ByteReader is a utility class that can be used to read bit strings.
 * It allows us to read in single bit at each time.
 */
class ByteReader {
 public:
  /**
   * Create a ByteReader.
   *
   * @param file_name compressed file address
   */
  explicit ByteReader(const std::string &file_name);

  /**
   * Deconstruct a ByteReader.
   */
  ~ByteReader();

  /**
   * Read a bit.
   *
   * @return return one bit.
   */
  bool ReadBit();

  /**
   * Read a byte.
   *
   * @return return a unsigned char of 8 bits
   */
  unsigned char ReadByte();

  /**
   * Read 16 bits.
   *
   * @return return a unsigned int of 32 bits, the result is in the lower 16
   * bits.
   */
  unsigned int Read16Bit();

  /**
   * Read a int.
   *
   * @return return a int variable
   */
  int Read32Bit();

  /**
   * Read 32 bits.
   *
   * @param bytes return an unsigned char array of length 4
   */
  void Read32Bit(unsigned char *bytes);

  /**
   * Sets the position of the next character to be extracted from the input
   * stream. The next position is 8 * num_bytes + num_bits bits relative to
   * current position.
   *
   * @param num_bytes distance between current position and next character to
   * read in bytes.
   * @param num_bits how many bits left to reach the desired position
   * @param way this parameter is the same as input "way" in function 'seekg'
   */
  void Seekg(int64_t num_bytes, unsigned char num_bits, std::ios_base::seekdir way) {
    if (buffer_len_ != 0) {
      std::cout
          << "Seekg: buffer is not empty, please make sure buffer is empty before calling Seekg\n";
      return;
    }
    fin_.seekg(num_bytes, way);
    for (int i = 0; i < num_bits; ++i) {
      ReadBit();
    }
  }

  int Tellg() { return fin_.tellg(); }

 private:
  std::ifstream fin_;
  unsigned int buffer_, buffer_len_;
};

}  // namespace db_compress

#endif
