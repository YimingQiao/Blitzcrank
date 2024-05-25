/**
 * @file string_squid.h
 * @brief Header for SquID
 */

#ifndef DB_COMPRESS_STRING_SQUID_H
#define DB_COMPRESS_STRING_SQUID_H

#include <deque>

#include "base.h"
#include "categorical_model.h"
#include "model.h"
#include "numerical_model.h"
#include "string_tools.h"

namespace db_compress {
/**
 * Distribution of characters depends on the former two characters.
 *
 */
class MarkovCharDist {
 public:
  explicit MarkovCharDist(int history_length);

  void FeedWord(const std::string &word);

  void EndOfData();

  void GetMarkovProbInterval(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                             const std::string &word);

  void MarkovDecompress(Decoder *decoder, ByteReader *byte_reader, std::string &word,
                        int word_length);

  /**
   * Write markov distribution to disks.
   * @param byte_writer sequence writer
   */
  void WriteMarkov(SequenceByteWriter *byte_writer) const;

  /**
   * Read markov distribution from disks, and init the delayed coding params.
   * @param byte_reader reader
   */
  void ReadMarkov(ByteReader *byte_reader);

 private:
  int history_length_;
  int num_markov_table_;
  uint8_t former_{0}, latter_{0};

  CategoricalSquID squid_;
  std::vector<CategoricalStats> markov_table_stats_;

  /**
   * Calculate the index of distribution based on queue, whose size is at least two.
   * @return index of a distribution in markov table
   */
  int GetTableIndex();

  void Reset() {
    former_ = 0;
    latter_ = 0;
  }

  void UpdateHistory(unsigned char c) {
    former_ = latter_;
    latter_ = tolower(c);
  }
};

/**
 * @brief Statistic information for a string
 * Assumption: each string has an unique delimiter
 */
struct StringStats {
  // 1. Sentence features: number of words (How many words in the given sentence), delimiter type ("
  // " or "/").
  CategoricalSquID *num_terms_squid_;
  CategoricalSquID *delimiter_type_squid_;

  // 2. Word features: word length (How long is the given word), encoding method (how to encode this
  // word, utilize global dictionary, local dictionary, etc).
  NumericalSquID *word_length_squid_;
  CategoricalSquID *encoding_method_squid_;

  // base string character param.
  MarkovCharDist *markov_dist_;

  // 3. Dictionary
  GlobalDictionary *global_dict_;

  // 4. Delta Encoding
  CategoricalSquID *delta_encoding_;
  CategoricalSquID *dict_idx_;
};

/**
 * Structure of String Model: 1) base module; 2) global dictionary; 3) local dictionary.
 *
 * 1. Base module (learnable): Original string model. First, it determines how many characters this
 * string has; then for each character, a categorical attribute is called. According to the ASCII,
 * the capacity of the categorical attributes is 64.
 *
 * 2. Global dictionary (learnable): Each string attribute has a global dictionary, it stores
 * frequent words.
 *
 * 3. Local dictionary (learn when compress/decompress): each string attribute has a local
 * dictionary, which stores the latest string value. When compress/decompress next string value, the
 * string in local dictionary can be referred.
 */
class StringSquID {
 public:
  StringSplitter splitter_;

  StringSquID(int local_dict_size) : attr_(""), local_dict_(local_dict_size) {}

  const AttrValue &GetResultAttr() { return attr_; }

  void Init(StringStats stat);

  void GetProbIntervals(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                        const AttrValue &value);

  void Decompress(Decoder *decoder, ByteReader *byte_reader);

  void NormalCompress(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                      const std::string &word);

  std::string &NormalDecompress(Decoder *decoder, ByteReader *byte_reader);

 private:
  AttrValue attr_;
  std::string word_buffer_;
  StringStats stats_;
  std::deque<std::string> local_dict_;

  // monitor
  StringMonitor monitor_;

  std::string CheckLocalDict(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                             const std::string &string);
};
}  // namespace db_compress

#endif  // DB_COMPRESS_STRING_SQUID_H
