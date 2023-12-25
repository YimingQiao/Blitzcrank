//
// Created by Qiao Yiming on 2022/5/7.
//

#include "../include/string_tools.h"
#include "../include/string_squid.h"

namespace db_compress {

StringSplitter::StringSplitter() : word_index_(0), delimiter_index_(0) {
  for (int i = 0; i < delimiters_rank1_.size(); ++i) delimiter2id_rank1_[delimiters_rank1_[i]] = i;
  for (char c : delimiters_rank2_) id2delimiters_.push_back(c);
  for (int i = 0; i < delimiters_rank2_.size(); ++i) delimiter2id_[delimiters_rank2_[i]] = i;

  empty_ = id2delimiters_.size();
}

void StringSplitter::ParseString(const std::string &str) {
  num_words_ = 0;
  num_phrase_ = 0;
  word_index_ = 0;
  delimiter_index_ = 0;

  if (str.empty()) return;

  if (str.size() >= words_.size()) {
    words_.resize((str.size() | 1) << 1);
    delimiters_.resize((str.size() | 1) << 1);
  }

  // Step 1 - 1: separate string by '/', etc.
  int start = 0;
  for (int k = 0; k < str.size(); ++k) {
    // leave out non-delimiter character
    if (delimiter2id_rank1_.count(str[k]) == 0) continue;

    // while (k + 1 < str.size() && str[k] == str[k + 1]) k++;
    while (k + 1 < str.size() && delimiter2id_rank1_.count(str[k + 1])) k++;
    words_[num_words_] = k;
    delimiters_[num_words_++] = Delimiter2Id(str[k]);
    start = k + 1;
  }

  num_phrase_ = num_words_;
  // Step 2: separate the last part in the string with fine-grained delimiters.
  for (int i = start; i < str.size(); ++i) {
    // separate digit and alphabet
    if (i > 0) {
      if ((isalpha(str[i - 1]) && isdigit(str[i])) || (islower(str[i - 1]) && isupper(str[i]))) {
        words_[num_words_] = i;
        // mark 'No SEP'
        delimiters_[num_words_++] = static_cast<int>(id2delimiters_.size());
        continue;
      }
    }

    // e.g. http://www should be divided into http:/ and www, and they are seperated by /.
    if (delimiter2id_.count(str[i]) == 1) {
      while (i + 1 < str.size() && delimiter2id_.count(str[i + 1])) i++;
      words_[num_words_] = i;
      delimiters_[num_words_++] = Delimiter2Id(str[i]);
    }
  }
  words_[num_words_++] = static_cast<int>(str.size());

  if (num_words_ >= words_.size())
    throw BufferOverflowException("StringSplitter::ParseString: Vector Overflow.\n");
}

int StringSplitter::NextWord() {
  if (word_index_ >= num_words_) throw BufferOverflowException("No word left.\n");
  return words_[word_index_++];
}

int StringSplitter::NextDelimiter() {
  if (delimiter_index_ >= num_words_ - 1) throw BufferOverflowException("No Delimiter left.\n");
  return delimiters_[delimiter_index_++];
}

void GlobalDictionary::GetProbIntervals(std::vector<Branch *> &prob_intervals,
                                        int &prob_intervals_index, const std::string &word) {
  if (term_to_id_.find(word) == term_to_id_.end())
    std::cerr << "word " << word << " not in dictionary" << std::endl;

  int word_id = term_to_id_[word];
  squid_.GetSquID()->GetProbIntervals(prob_intervals, prob_intervals_index, AttrValue(word_id));
}

std::string &GlobalDictionary::Decompress(Decoder *decoder, ByteReader *byte_reader,
                                          bool &is_phrase) {
  CategoricalTreeSquid *squid = squid_.GetSquID();
  squid->Decompress(decoder, byte_reader);
  int id = squid->GetResultAttr().Int();
  is_phrase = (id < line_);
  return id_to_term_[id];
}

void GlobalDictionary::EndOfData(TableCategorical &encoding_methods, StringSplitter &splitter) {
  // word_counts analysis
  // AnalyzeWordCount();

  // 1-1. Count words.
  int num_dict_word = 0;
  int num_total_freq = 0;
  int num_dict_freq = 0;
  for (auto &word_count : word_counts_) {
    num_total_freq += word_count.second;

    if (IsFrequentWord(word_count)) {
      num_dict_freq += word_count.second;
      num_dict_word++;
    }
  }

  // 1-2. Count phrase.
  int num_dict_phrase;
  CountPhrases(splitter, num_total_freq, num_dict_freq, num_dict_phrase, num_dict_word);

  squid_.Init(num_dict_word + num_dict_phrase);
  id_to_term_.resize(num_dict_word + num_dict_phrase);
  int idx = 0;

  // 2-1. Feed Phrase
  for (auto &phrase_count : phrase_counts_) {
    if (IsFrequentPhrase(phrase_count)) {
      id_to_term_[idx] = phrase_count.first;
      term_to_id_[phrase_count.first] = idx;
      squid_.FeedAttrs(AttrValue(idx), phrase_count.second);
      idx++;
    }
  }

  // Note that line is also an id for phrase.
  line_ = idx;

  // 2-2. Feed Word
  for (auto &word_count : word_counts_) {
    if (IsFrequentWord(word_count)) {
      if (term_to_id_.count(word_count.first) == 0) {
        id_to_term_[idx] = word_count.first;
        term_to_id_[word_count.first] = idx;
        squid_.FeedAttrs(AttrValue(idx), word_count.second);
        idx++;
      }
    }
  }
  id_to_term_.resize(idx);

  // handle encoding methods
  encoding_methods.FeedAttrs(AttrValue(2), num_dict_freq);
  encoding_methods.FeedAttrs(AttrValue(0), num_total_freq - num_dict_freq);

  squid_.EndOfData();

  // PrintGlobalDict();
  word_counts_.clear();
}

void GlobalDictionary::CountPhrases(const StringSplitter &splitter, int &num_total_freq,
                                    int &num_dict_freq, int &num_dict_phrase, int &num_dict_word) {
  num_dict_phrase = 0;
  int delimiter_idx;
  for (auto &phrase_count : phrase_counts_) {
    if (IsFrequentPhrase(phrase_count)) {
      const std::string &phrase = phrase_count.first;
      delimiter_idx = phrases_delimiter_idx_[phrase];
      const std::string &former = phrase.substr(0, delimiter_idx);
      const std::string &latter = phrase.substr(delimiter_idx + 1, phrase.size());
      if (word_counts_[former] == 0 || word_counts_[latter] == 0) {
        std::cerr << "GlobalDictionary::EndOfData: Word in a phrase cannot be zero weight.\n";
        std::cerr << phrase << "\n";
      }
      word_counts_[former] -= phrase_count.second;
      word_counts_[latter] -= phrase_count.second;
      num_total_freq -= phrase_count.second;
      num_dict_freq -= phrase_count.second;
      num_dict_phrase++;
    }
  }
}

void GlobalDictionary::WriteDictionary(SequenceByteWriter *byte_writer, StringSquID *string_squid) {
  byte_writer->Write32Bit(line_);
  byte_writer->Write32Bit(id_to_term_.size());
  squid_.WriteModel(byte_writer);

  unsigned bits = 0;
  int prob_intervals_index = 0;
  std::vector<Branch *> prob_intervals(kBlockSize_ << 1);
  std::vector<bool> is_virtual(kBlockSize_ << 1);
  BitString bit_string(kBlockSize_ << 1);

  for (const auto &word : id_to_term_) {
    string_squid->NormalCompress(prob_intervals, prob_intervals_index, word);

    if (prob_intervals_index > kBlockSize_) {
      for (int i = 0; i < prob_intervals_index; ++i) {
        bits += 16 - log2(prob_intervals[i]->total_weights_);
      }
      DelayedCoding(prob_intervals, prob_intervals_index, &bit_string, is_virtual);
      bit_string.Finish(byte_writer);
      prob_intervals_index = 0;
    }
  }

  if (prob_intervals_index != 0) {
    for (int i = 0; i < prob_intervals_index; ++i) {
      bits += 16 - log2(prob_intervals[i]->total_weights_);
    }
    DelayedCoding(prob_intervals, prob_intervals_index, &bit_string, is_virtual);
    bit_string.Finish(byte_writer);
  }

  std::cout << "Global Dict - Size: " << (bits >> 3) << " byte - #word: " << squid_.target_range_
            << "\n";
}

void GlobalDictionary::LoadDictionary(ByteReader *byte_reader, StringSquID *string_squid) {
  line_ = byte_reader->Read32Bit();
  int size = byte_reader->Read32Bit();
  squid_ = *static_cast<TableCategoricalTree *>(TableCategoricalTree::ReadModel(byte_reader));
  id_to_term_.resize(size);

  Decoder decoder;
  for (int i = 0; i < size; i++) {
    if (decoder.CurBlockSize() > kBlockSize_) decoder.InitProbInterval();
    id_to_term_[i] = std::move(string_squid->NormalDecompress(&decoder, byte_reader));
  }
}

void GlobalDictionary::AnalyzeWordCount() {
  //  char name[40];
  //  tmpnam(name);
  //  std::cout << name << "\n";
  //  std::ofstream of(name);
  //  for (const auto &sample : word_counts_) {
  //    of << sample.first << "," << sample.second << "\n";
  //  }
  //  of.close();
}
}  // namespace db_compress
