/*
 * The header file for tuple Index
 */

#ifndef INDEX_H
#define INDEX_H

#include <memory>

#include "data_io.h"
#include "squish_exception.h"

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
        num_tuple_in_last_block_(0),
        num_block_(0) {}

  /**
   * Write how many bits used for compressing last block. Block is a set of many
   * probability intervals. Assumption: length and tuple number of every block
   * are not longer than 65536.
   *
   * @param length length of bits for a block
   * @param num_tuple compressed tuple number
   */
  void WriteBlockInfo(unsigned length, unsigned num_tuple) {
    file_writer_->Write16Bit(length);
    file_writer_->Write16Bit(num_tuple - num_tuple_in_last_block_);
    num_tuple_in_last_block_ = num_tuple;
    num_block_++;
  }

  /**
   * End of index creator, concatenate temp.index and compressed file into one
   * final file.
   *
   * @param output_file recovered dataset address
   */
  void End(const std::string &output_file) {
    file_writer_->WriteUnsigned(num_block_);
    file_writer_ = nullptr;
    std::ofstream compressed(output_file, std::ios_base::binary | std::ios_base::app);
    if (!compressed) throw IOException("Cannot open compressed file.\n");

    std::ifstream index(index_file_, std::ios_base::binary);
    if (!index) throw IOException("Cannot open index file.\n");
    compressed.seekp(0, std::ios_base::end);
    compressed << index.rdbuf();

    // Close index ifstream before delete it
    index.close();
    compressed.close();
    remove(index_file_.c_str());
  }

 private:
  const std::string index_file_ = "temp.index";
  std::unique_ptr<SequenceByteWriter> file_writer_;
  int num_block_;
  unsigned num_tuple_in_last_block_;
};

/**
 * Indexer reader of squish, it is used to locate the start point of
 * decompression according to user requirement. Count how many bits should be
 * ignored, then move to the start point of decompression accordingly.
 */
class IndexReader {
 public:
  explicit IndexReader(const char *outputFile) : file_reader_(outputFile), num_block_(0){};

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
  int LocateBlock(uint32_t &n_byte, unsigned char &m_bit, int tuple_idx) {
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

    n_byte = block_bits_[block_idx_] >> 3;
    m_bit = block_bits_[block_idx_] & 7;
    return tuple_idx - block_tuples_[block_idx_];
  }

 private:
  ByteReader file_reader_;
  int num_block_;

  std::vector<unsigned> block_bits_;
  std::vector<unsigned> block_tuples_;

  // binary search
  int block_idx_;
  int r_;
  int mid_;
};

}  // namespace db_compress

#endif  // !INDEX_H
