/**
 * @file index.h
 * @brief header file for tuple Index
 */

#ifndef INDEX_H
#define INDEX_H

#include <memory>
#include <unistd.h>

#include "blitzcrank_exception.h"
#include "data_io.h"

namespace db_compress {
/**
 * Indexer creator of squish, the core component of random access. It is used to
 * create a indexer file when compression. Length of compressed bits for each
 * tuple is recorded in temp.index file, called index table. After compression,
 * we concatenate temp.index with compressed binary file.
 */
class IndexCreator {
 public:
  /**
   * Create a index creator.
   */
  IndexCreator()
      : file_writer_(std::make_unique<SequenceByteWriter>(index_file_)),
        num_block_(0),
        last_block_size_(0) {}
  /**
   * Write how many bits used for compressing last block. Block is a set of many
   * probability intervals. Assumption: length and tuple number of every block
   * are not longer than 65536.
   *
   * @param length length of bits for a block
   * @param num_tuple compressed tuple number
   */
  void WriteBlockInfo(uint32_t length, uint32_t num_tuple) {
    file_writer_->Write16Bit(length);
    file_writer_->Write16Bit(num_tuple - last_block_size_);
    if (block_size_ == -1) block_size_ = num_tuple - last_block_size_;
    last_block_size_ = num_tuple;
    num_block_++;
  }

  /**
   * End of index creator, concatenate temp.index and compressed file into one
   * final file.
   *
   * @param output_file recovered dataset address
   */
  void End() {
    file_writer_->Write32Bit(num_block_);
    file_writer_ = nullptr;
  }

 private:
  // std::string index_file_ = std::to_string(getpid()) +  "_temp.index";
  std::string index_file_ = "_temp.index";
  std::unique_ptr<SequenceByteWriter> file_writer_;
  uint32_t num_block_;
  uint32_t last_block_size_;
  int block_size_ = -1;
};

/**
 * Indexer reader of squish, it is used to locate the start point of
 * decompression according to user requirement. Count how many bits should be
 * ignored, then move to the start point of decompression accordingly.
 */
class IndexReader {
 public:
  explicit IndexReader() : file_reader_(index_file_), num_block_(0){};

  /**
   * Init Indexer.
   */
  void Init() {
    file_reader_.Seekg(-4, 0, std::ios_base::end);
    num_block_ = file_reader_.Read32Bit();

    block_bits_.resize(num_block_ + 1, 0);
    block_tuples_.resize(num_block_ + 1, 0);

    file_reader_.Seekg(-4 * num_block_ - 4, 0, std::ios_base::end);
    for (int i = 0; i < num_block_; ++i) {
      block_bits_[i + 1] = block_bits_[i] + file_reader_.Read16Bit();
      block_tuples_[i + 1] = block_tuples_[i] + file_reader_.Read16Bit();
    }

    std::cout << "Block Size: " << block_tuples_[1] - block_tuples_[0] << " tuple" << std::endl;
  }

  /**
   * Calculate the position of first bit to read, i.e. the first bit of block. Tuple index is
   * user-specified, it means where to start decompression. This function is used to support random
   * access.
   *
   * @param[out] n_byte how many bytes need to ignore (without decompression).
   * @param[out] m_bit how many bits left to ignore
   * @param tuple_idx index of first block to be decompressed
   * @return return how many tuples we do not need, but have to decompress.csv.
   */
  inline uint32_t LocateBlock(uint32_t &n_byte, size_t tuple_idx) {
    block_idx_ = 0;
    r_ = num_block_;
    while (block_idx_ != r_) {
      mid_ = block_idx_ + (r_ - block_idx_) / 2;
      if (block_tuples_[mid_] <= tuple_idx && block_tuples_[mid_ + 1] > tuple_idx) {
        block_idx_ = mid_;
        r_ = mid_;
      } else if (tuple_idx < block_tuples_[mid_])
        r_ = mid_;
      else
        block_idx_ = mid_ + 1;
    }

    n_byte = block_bits_[block_idx_] << 1;
    return tuple_idx - block_tuples_[block_idx_];
  }
  /**
   * Get the place of tuple, ONE BLOCK ONE TUPLE, here.
   *
   * @param tuple_idx
   * @return
   */
  inline uint32_t LocateTuple(size_t tuple_idx) { return block_bits_[tuple_idx] << 1; }

 private:
  // std::string index_file_ = std::to_string(getpid()) +  "_temp.index";
  std::string index_file_ = "_temp.index";

  ByteReader file_reader_;
  int num_block_;
  std::vector<uint32_t> block_bits_;
  std::vector<unsigned> block_tuples_;

  // binary search
  uint32_t block_idx_;
  uint32_t r_;
  uint32_t mid_;
};

}  // namespace db_compress

#endif  // !INDEX_H
