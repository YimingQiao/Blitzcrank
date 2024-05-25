/**
 * @file string_tools.h
 * @brief This is header for some string squid tools: StringSplitter, LocalDictionary, and
 * GlobalDictionary.
 */

#ifndef DB_COMPRESS_STRING_TOOLS_H
#define DB_COMPRESS_STRING_TOOLS_H

#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "blitzcrank_exception.h"
#include "categorical_tree_model.h"
#include "simple_prob_interval_pool.h"

namespace db_compress {
/**
 * It is used to split a given string.
 */
class StringSplitter {
 public:
  // metadata
  const std::vector<char> delimiters_rank1_ = {'/', ' ', '#'};
  const std::vector<char> delimiters_rank2_ = {'/', ' ', '#', '-', '_', '.'};
  std::vector<char> id2delimiters_;
  std::map<char, int> delimiter2id_rank1_;
  std::map<char, int> delimiter2id_;
  int empty_;

  int num_words_;
  int num_phrase_;
  std::vector<int> words_;
  std::vector<int> delimiters_;

  StringSplitter();
  /**
   * Sparse a string.
   *
   * @param str string to be parsed
   * @return number of words in str
   */
  void ParseString(const std::string &str);

  /**
   * Get next word. NextDelimiter() must be called for each call of NextWord().
   *
   * @return next word
   */
  int NextWord();

  /**
   * Get next delimiter.
   *
   * @return next delimiter
   */
  int NextDelimiter();

 private:
  int word_index_, delimiter_index_;

  int Delimiter2Id(char delimiter) { return delimiter2id_[delimiter]; }
};

class StringSquID;

/**
 * It is a global dictionary.
 */
class GlobalDictionary {
 public:
  explicit GlobalDictionary(int BlockSize) : kBlockSize_(BlockSize) {}

  void PushWord(const std::string &word, int count) { word_counts_[word] += count; }

  void PushPhrase(const std::string &phrase, int delimiter_idx, int count) {
    phrase_counts_[phrase] += count;
    if (phrases_delimiter_idx_.count(phrase) == 0) phrases_delimiter_idx_[phrase] = delimiter_idx;
  }

  bool IsWordInDictionary(const std::string &word) const {
    return term_to_id_.find(word) != term_to_id_.end();
  }

  void EndOfData(TableCategorical &encoding_methods, StringSplitter &splitter);

  std::string &Decompress(Decoder *decoder, ByteReader *byte_reader, bool &is_phrase);

  void GetProbIntervals(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                        const std::string &word);

  void WriteDictionary(SequenceByteWriter *byte_writer, StringSquID *string_squid);

  void LoadDictionary(ByteReader *byte_reader, StringSquID *string_squid);

  void PrintGlobalDict() {
    std::cout << "Dict Size: " << id_to_term_.size() << "\n";
    for (int i = 0; i < line_; ++i)
      std::cout << id_to_term_[i] << ": " << phrase_counts_[id_to_term_[i]] << "\t";

    std::cout << "\n";

    for (int i = line_; i < id_to_term_.size(); ++i)
      std::cout << id_to_term_[i] << ": " << word_counts_[id_to_term_[i]] << "\t";

    std::cout << "\n";
  }

 private:
  int kBlockSize_;
  TableCategoricalTree squid_;

  /**
   * Any phrase has an id before line, while every word has an id after line.
   */
  int line_;
  std::vector<std::string> id_to_term_;
  std::unordered_map<std::string, int> term_to_id_;
  std::unordered_map<std::string, int> word_counts_;
  std::unordered_map<std::string, int> phrase_counts_;
  std::unordered_map<std::string, int> phrases_delimiter_idx_;

  void AnalyzeWordCount();

  void CountPhrases(const StringSplitter &splitter, int &num_total_freq, int &num_dict_freq,
                    int &num_dict_phrase, int &num_dict_word);

  bool IsFrequentWord(std::pair<const std::string, int> &word_count) {
    return (word_count.second > 3 && word_count.first.size() > 3) || (word_count.second > 10);
  }

  bool IsFrequentPhrase(std::pair<const std::string, int> &phrase_count) {
    return ((phrase_count.second > 10) && phrase_count.first.size() >= 3);
  }
};

template <class T>
class LoopBuffer {
 public:
  explicit LoopBuffer(size_t max_size) : size_(max_size), buffer(max_size), buffer_index(0) {}

  void PushValue(T value) {
    buffer[buffer_index] = value;
    buffer_index = (buffer_index + 1) % size_;
  }

  std::string &GetValue(size_t idx) { return buffer[idx]; }

  std::string &GetTop() { return buffer[buffer_index]; }

  size_t GetIndex(const std::string &word) const {
    for (int i = 0; i < size_; ++i)
      if (buffer[i] == word) return i;

    // Such word is not existed.
    return -1;
  }

 private:
  size_t size_;
  std::vector<T> buffer;
  size_t buffer_index;
};

class Monitor {
 public:
  void Init(std::string name, std::vector<std::string> indicators) {
    name_ = name;
    info_name_ = indicators;
  }

  ~Monitor() { Print(); }

  void UpdateIndex(int index) { num_prob_interval_ = index; }

  void AddInfo(const std::vector<Branch *> &prob_intervals, const int &prob_intervals_index,
               int which) {
    for (int i = num_prob_interval_; i < prob_intervals_index; ++i) {
      info_[which] += MeasureProbInterval(prob_intervals[i]->total_weights_);
      bits += MeasureProbInterval(prob_intervals[i]->total_weights_);
    }
  }

 private:
  std::string name_;
  int num_prob_interval_{0};
  uint64_t bits{0};
  std::vector<uint64_t> info_;
  std::vector<std::string> info_name_;

  int MeasureProbInterval(int weights) { return 16 - log2(weights); }

  void Print() {
    std::cout << name_ << "\n";
    for (int i = 0; i < info_.size(); ++i)
      std::cout << info_name_[i] << ": " << (info_[i] >> 13) << " KB"
                << "\t";

    std::cout << "Total: " << (bits >> 13) << " KB\t";
    std::cout << "\n\n";
  }
};

/**
 * This class is used to record the compression statistics information of string.
 */
class StringMonitor {
 public:
  std::string attribute_;

  StringMonitor() {
    // info_.resize(info_name_.size());
  }

  ~StringMonitor() {
    // Print();
  }

  void UpdateIndex(int index) {
    // num_prob_interval_ = index;
  }

  void AddInfo(const std::vector<Branch *> &prob_intervals, const int &prob_intervals_index,
               int which) {
    //    for (int i = num_prob_interval_; i < prob_intervals_index; ++i) {
    //      info_[which] += MeasureProbInterval(prob_intervals[i]->total_weights_);
    //      info_[info_.size() - 1] += MeasureProbInterval(prob_intervals[i]->total_weights_);
    //    }
  }

  void Print() {
    for (int i = 0; i < info_.size(); ++i) {
      std::cout << info_[i] / (8 * 1024) << ",";
    }
    std::cout << "\n";
  }

 private:
  int num_prob_interval_;
  // num_bits_base; num_bits_global; num_bits_local; num_bits_delimiter; total;
  std::vector<double> info_;
  std::vector<std::string> info_name_ = {"Size base",
                                         "Size of global dict",
                                         "Size of local dict",
                                         "Size of Delimiter",
                                         "Size of encoding method",
                                         "Size of word num",
                                         "Size Delta",
                                         "Total"};

  double MeasureProbInterval(int weights) { return 16 - log2(weights); }
};
}  // namespace db_compress

#endif  // DB_COMPRESS_STRING_TOOLS_H
