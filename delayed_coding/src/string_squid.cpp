#include "string_squid.h"

namespace db_compress {
// MarkovCharDist
MarkovCharDist::MarkovCharDist(int history_length) : history_length_(history_length) {
  // determine the size of markov table
  int num_char = 26;
  num_markov_table_ = 1;

  // Markov table
  if (history_length > 0) {
    for (int i = 0; i < history_length; ++i) num_markov_table_ *= num_char;

    // add the simple character distribution to markov table, suppose it is the first term.
    num_markov_table_ += 1;
  }

  // init each term in markov table
  markov_table_stats_.resize(num_markov_table_);
  for (int i = 0; i < num_markov_table_; ++i) {
    CategoricalStats &stats = markov_table_stats_[i];
    stats.count_.resize(256);
    // one more branch to enter next level
    stats.weight_.resize(257);
  }
}

void MarkovCharDist::FeedWord(const std::string &word) {
  Reset();

  for (uint8_t sym : word) {
    CategoricalStats &vec = markov_table_stats_[GetTableIndex()];
    vec.count_.at(sym) += 1;
    UpdateHistory(sym);
  }
}

void MarkovCharDist::EndOfData() {
  // 1. End of each statistic
  for (int i = 0; i < num_markov_table_; ++i) {
    CategoricalStats &stats = markov_table_stats_[i];
    // Extract counts vector
    std::vector<int> counts;
    counts.swap(stats.count_);

    int sum_count = 0;
    int index_max_weight = 0;
    unsigned left_weight = (1 << 16);
    bool zero_weight_exist = false;
    for (int count : counts) sum_count += count;

    if (sum_count == 0)
      zero_weight_exist = true;
    else {
      for (int j = 0; j < counts.size(); ++j) {
        stats.weight_[j] = static_cast<uint64_t>(counts[j]) * (1 << 16) / sum_count;
        left_weight -= stats.weight_[j];

        // check zero weight existence
        if (!zero_weight_exist && stats.weight_[j] == 0) zero_weight_exist = true;

        // find branch with the largest weight
        if (stats.weight_[index_max_weight] < stats.weight_[j]) index_max_weight = j;
      }
    }

    // allocate left weight
    if (zero_weight_exist) {
      // if there is no left weight, we borrow ''one'' from the branch which has at least one
      // unit weight.
      if (left_weight == 0) {
        left_weight = 1;
        stats.weight_[index_max_weight] -= 1;
      }
      // allocate weight for the extra branch
      stats.weight_[stats.weight_.size() - 1] = left_weight;
      stats.rare_branch_handler_.Init(stats.weight_);
    } else
      stats.weight_[index_max_weight] += left_weight;

    InitDelayedCodingParams(stats.weight_, stats.coding_params_);
  }

  // 2. clear history
  former_ = 0;
  latter_ = 0;
}

void MarkovCharDist::WriteMarkov(SequenceByteWriter *byte_writer) const {
  byte_writer->Write16Bit(num_markov_table_);
  for (int i = 0; i < num_markov_table_; ++i) {
    std::vector<unsigned> weights = markov_table_stats_[i].weight_;
    for (size_t j = 0; j < weights.size(); ++j) {
      // 65536 cannot be represented by 16 bits. Once there is a 65536 weight branch, then all left
      // weights are zero. So, we use 65535 to represent 65536.
      if (weights[j] == 65536)
        byte_writer->Write16Bit(65535);
      else
        byte_writer->Write16Bit(weights[j]);
    }
  }
}

void MarkovCharDist::ReadMarkov(ByteReader *byte_reader) {
  // Read Model Parameters
  num_markov_table_ = static_cast<int>(byte_reader->Read16Bit());
  for (int i = 0; i < num_markov_table_; ++i) {
    CategoricalStats &stats = markov_table_stats_[i];

    // Read weights
    size_t only_value = -1;
    unsigned sum_weights = 0;
    for (size_t j = 0; j < stats.weight_.size(); ++j) {
      stats.weight_[j] = byte_reader->Read16Bit();
      sum_weights += stats.weight_[j];

      // label branch of weight 65535, there could be only one 65535 weight branch.
      if (stats.weight_[j] == 65535) only_value = j;
    }
    // If sum of weights is not 65536, it means ''only value'' exists.
    if (sum_weights != 65536) {
      stats.weight_[only_value] = 65536;
      stats.only_value_ = only_value;
    }

    // init rare branch handler
    if (stats.weight_[stats.weight_.size() - 1] != 0)
      stats.rare_branch_handler_.Init(stats.weight_);

    // Preparation for delayed coding
    InitDelayedCodingParams(stats.weight_, stats.coding_params_);
  }
}

int MarkovCharDist::GetTableIndex() {
  // Try 1: lowercase and uppercase
  //  if (isalpha(former_) && isalpha(latter_)) {
  //    int ret = 0;
  //    if (isupper(former_))
  //      ret += former_ - 'A';
  //    else
  //      ret += former_ - 'a' + 26;
  //
  //    if (isupper(latter_))
  //      ret = (52 * ret) + latter_ - 'A';
  //    else
  //      ret = (52 * ret) + latter_ - 'a' + 26;
  //
  //    return ret + 1;
  //  }

  // Try 2: lowercase
  if (history_length_ == 2) {
    if (former_ >= 'a' && former_ <= 'z' && latter_ >= 'a' && latter_ <= 'z')
      return (former_ - 'a') * 26 + (latter_ - 'a') + 1;
  } else if (history_length_ == 1)
    if (latter_ >= 'a' && latter_ <= 'z') return latter_ - 'a' + 1;

  // Try 3: lowercase alphabet and number
  //  if ((isalpha(former_) || isdigit(former_)) && (isalpha(latter_) || isdigit(latter_))) {
  //    int ret = 0;
  //    if (isalpha(former_))
  //      ret += former_ - 'a' + 10;
  //    else
  //      ret += former_ - '0';
  //
  //    if (isalpha(latter_))
  //      ret = (36 * ret) + latter_ - 'a' + 10;
  //    else
  //      ret = (36 * ret) + latter_ - '0';
  //
  //    return ret + 1;
  //  }
  //  if (isalpha(latter_) || isdigit(latter_)) {
  //    if (isalpha(latter_))
  //      return latter_ - 'a' + 1;
  //    else
  //      return latter_ - '0' + 1;
  //  }

  // return the default distribution
  return 0;
}

void MarkovCharDist::GetMarkovProbInterval(std::vector<Branch *> &prob_intervals,
                                           int &prob_intervals_index, const std::string &word) {
  Reset();

  for (uint8_t sym : word) {
    CategoricalStats &stats = markov_table_stats_[GetTableIndex()];
    squid_.Init(stats);
    squid_.GetProbIntervals(prob_intervals, prob_intervals_index, AttrValue(sym));
    UpdateHistory(sym);
  }
}

void MarkovCharDist::MarkovDecompress(Decoder *decoder, ByteReader *byte_reader, std::string &word,
                                      int word_length) {
  Reset();

  for (int i = 0; i < word_length; ++i) {
    CategoricalStats &stats = markov_table_stats_[GetTableIndex()];
    squid_.Init(stats);
    squid_.Decompress(decoder, byte_reader);
    word[i] = static_cast<char>(squid_.GetResultAttr().Int());
    UpdateHistory(word[i]);
  }
}

// StringSquID
void StringSquID::Init(StringStats stat) { stats_ = stat; }

void StringSquID::GetProbIntervals(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                                   const AttrValue &value) {
  int start;
  int method;
  int end;
  int delimiter;

#if kLocalDictSize > 0
  // 0. Local Dictionary
  const std::string &string = value.String();
  const std::string &sentence = CheckLocalDict(prob_intervals, prob_intervals_index, string);
#else
  const std::string &sentence = value.String();
#endif

  // 1. Parse sentence
  splitter_.ParseString(sentence);
  int num_words = splitter_.num_words_;

  // 2. Calculate how many phrases in this sentence, and write the number.
  std::vector<char> is_phrase(num_words, false);
  for (int i = 0; i < splitter_.num_phrase_ - 1; i += 2) {
    start = (i == 0) ? 0 : splitter_.words_[i - 1] + 1;
    end = splitter_.words_[i + 1];
    if (end - start >= 3) {
      const std::string &phrase = sentence.substr(start, end - start);
      if (stats_.global_dict_->IsWordInDictionary(phrase)) {
        is_phrase[i] = true;
        is_phrase[i + 1] = true;
      }
    }
  }

  monitor_.UpdateIndex(prob_intervals_index);
  stats_.num_terms_squid_->GetProbIntervals(prob_intervals, prob_intervals_index,
                                            AttrValue(num_words));
  monitor_.AddInfo(prob_intervals, prob_intervals_index, 5);

  // 3. Start Compress Each Term.
  start = 0;
  if (monitor_.attribute_.empty()) monitor_.attribute_ = value.String();
  for (int word_idx = 0; word_idx < num_words; ++word_idx) {
    if (is_phrase[word_idx]) word_idx++;
    end = splitter_.words_[word_idx];
    const std::string &term = sentence.substr(start, end - start);
    method = (stats_.global_dict_->IsWordInDictionary(term)) ? 2 : 0;

    monitor_.UpdateIndex(prob_intervals_index);
    stats_.encoding_method_squid_->GetProbIntervals(prob_intervals, prob_intervals_index,
                                                    AttrValue(method));
    monitor_.AddInfo(prob_intervals, prob_intervals_index, 4);

    // 3-2. Write Term
    switch (method) {
      case 0:
        // naive method
        monitor_.UpdateIndex(prob_intervals_index);
        NormalCompress(prob_intervals, prob_intervals_index, term);
        monitor_.AddInfo(prob_intervals, prob_intervals_index, 0);
        break;
      case 2:
        // global dictionary
        monitor_.UpdateIndex(prob_intervals_index);
        stats_.global_dict_->GetProbIntervals(prob_intervals, prob_intervals_index, term);
        monitor_.AddInfo(prob_intervals, prob_intervals_index, 1);
        break;
      default:
        std::cerr << "StringSquID::GetProbIntervals:Error: unknown encoding method" << std::endl;
    }

    if (word_idx == num_words - 1) break;

    // 3-2. Write delimiter
    delimiter = splitter_.delimiters_[word_idx];
    monitor_.UpdateIndex(prob_intervals_index);
    stats_.delimiter_type_squid_->GetProbIntervals(prob_intervals, prob_intervals_index,
                                                   AttrValue(delimiter));
    monitor_.AddInfo(prob_intervals, prob_intervals_index, 3);
    start = end + static_cast<int>(delimiter != splitter_.empty_);
  }
}

std::string StringSquID::CheckLocalDict(std::vector<Branch *> &prob_intervals,
                                        int &prob_intervals_index, const std::string &string) {
  int delta_idx = 0;  // local dictionary
  int dict_idx = 0;
  int char_idx;
  for (int i = local_dict_.size() - 1; i >= 0; --i) {
    char_idx = 0;
    const std::string &buffer = local_dict_[i];
    while (char_idx < std::min(buffer.size(), string.size())) {
      if (buffer[char_idx] != string[char_idx] || char_idx > 128) break;
      char_idx++;
    }

    if (delta_idx < char_idx) {
      delta_idx = char_idx;
      dict_idx = i;
    }
    if (delta_idx > 128) break;
  }
  if (delta_idx < 5) delta_idx = 0;

  monitor_.UpdateIndex(prob_intervals_index);
  stats_.delta_encoding_->GetProbIntervals(prob_intervals, prob_intervals_index,
                                           AttrValue(delta_idx));
  if (delta_idx != 0) {
    stats_.dict_idx_->GetProbIntervals(prob_intervals, prob_intervals_index, AttrValue(dict_idx));
  }
  monitor_.AddInfo(prob_intervals, prob_intervals_index, 6);

  local_dict_.pop_front();
  local_dict_.push_back(string);

  return string.substr(delta_idx, string.size());
}

void StringSquID::NormalCompress(std::vector<Branch *> &prob_intervals, int &prob_intervals_index,
                                 const std::string &word) {
  // get word length
  int branch = static_cast<int>(word.length());

  // write down word length
  stats_.word_length_squid_->GetProbIntervals(prob_intervals, prob_intervals_index,
                                              AttrValue(branch));
  // write down each character in given word
  stats_.markov_dist_->GetMarkovProbInterval(prob_intervals, prob_intervals_index, word);
}

void StringSquID::Decompress(Decoder *decoder, ByteReader *byte_reader) {
#if kLocalDictSize > 0
  // read delta
  stats_.delta_encoding_->Decompress(decoder, byte_reader);
  int delta = stats_.delta_encoding_->GetResultAttr().Int();
  if (delta != 0) {
    stats_.dict_idx_->Decompress(decoder, byte_reader);
    int dict_idx = stats_.dict_idx_->GetResultAttr().Int();
    attr_.value_ = (local_dict_[dict_idx].substr(0, delta));
  } else {
    attr_.value_ = ("");
  }
#else
  attr_.value_ = ("");
#endif

  // read number of words
  stats_.num_terms_squid_->Decompress(decoder, byte_reader);
  int num_words = stats_.num_terms_squid_->GetResultAttr().Int();
  bool is_phrase;

  // read each word
  for (int word_idx = 0; word_idx < num_words; ++word_idx) {
    stats_.encoding_method_squid_->Decompress(decoder, byte_reader);
    int method = stats_.encoding_method_squid_->GetResultAttr().Int();
    switch (method) {
      case 0:
        // naive method
        attr_.String() += NormalDecompress(decoder, byte_reader);
        break;
      case 2:
        // global dictionary
        attr_.String() += stats_.global_dict_->Decompress(decoder, byte_reader, is_phrase);
        // if we come across a phrase, two words are solved.
        word_idx += static_cast<int>(is_phrase);
        break;
      default:
        std::cerr << "StringSquID::MarkovDecompress:Error: unknown encoding method" << std::endl;
    }

    if (word_idx == num_words - 1) break;
    stats_.delimiter_type_squid_->Decompress(decoder, byte_reader);
    int delimiter_id = stats_.delimiter_type_squid_->GetResultAttr().Int();
    if (delimiter_id != splitter_.empty_)
      attr_.String().push_back(splitter_.id2delimiters_[delimiter_id]);
  }

#if kLocalDictSize > 0
  local_dict_.pop_front();
  local_dict_.push_back(attr_.String());
#endif
}

std::string &StringSquID::NormalDecompress(Decoder *decoder, ByteReader *byte_reader) {
  // read word length
  stats_.word_length_squid_->Decompress(decoder, byte_reader);
  int word_length = stats_.word_length_squid_->GetResultAttr(true).Int();
  if (word_length < 0) {
    std::cout << "StringSquID::NormalDecompress:Error: word length is negative" << std::endl;
    exit(1);
  }
  word_buffer_.resize(word_length);

  // read each character in given word
  stats_.markov_dist_->MarkovDecompress(decoder, byte_reader, word_buffer_, word_length);

  return word_buffer_;
}
}  // namespace db_compress
