#include "../include/string_model.h"

#include <vector>

namespace db_compress {
StringModel::StringModel(size_t target_var)
    : SquIDModel(std::vector<size_t>(), target_var),
      global_dictionary_(8192),
      markov_char_dist_(kMarkovModel),
      word_length_(true, 1),
      local_dict_(kLocalDictSize),
      squid_(kLocalDictSize) {}

StringSquID *StringModel::GetSquID(const AttrVector &tuple) {
  squid_.Init(GenerateStringStats());
  return &squid_;
}

void StringModel::FeedAttrs(const AttrVector &attrs, int count) {
#if kLocalDictSize > 0
  // get sentence
  const std::string &string = attrs.attr_[target_var_].String();
  const std::string &sentence = CheckLocalDict(count, string);
#else
  const std::string &sentence = attrs.attr_[target_var_].String();
#endif

  // parse sentence
  squid_.splitter_.ParseString(sentence);
  int num_words = squid_.splitter_.num_words_;

  // learn number of words in a string
  num_words_squid_.FeedAttrs(AttrValue(num_words), count);

  int start = 0;
  int end;
  int delimiter;
  for (int i = 0; i < num_words; ++i) {
    // 1-1. Learn word
    end = squid_.splitter_.NextWord();
    const std::string &word = sentence.substr(start, end - start);

    word_length_.FeedAttrs(AttrValue(static_cast<int>(word.length())), 1);
    markov_char_dist_.FeedWord(word);

    // 1-2. global dictionary learning
    global_dictionary_.PushWord(word, count);

    if (i == num_words - 1) break;

    // 2. Learn delimiter
    delimiter = squid_.splitter_.NextDelimiter();
    delimiter_type_.FeedAttrs(AttrValue(delimiter), count);
    start = end + static_cast<int>(delimiter != squid_.splitter_.empty_);
  }

  int delimiter_idx;
  for (int i = 0; i < squid_.splitter_.num_phrase_ - 1; i += 2) {
    start = (i == 0) ? 0 : squid_.splitter_.words_[i - 1] + 1;
    end = squid_.splitter_.words_[i + 1];
    delimiter_idx = squid_.splitter_.words_[i] - start;
    const std::string &phrase = sentence.substr(start, end - start);
    global_dictionary_.PushPhrase(phrase, delimiter_idx, count);
  }
}

std::string StringModel::CheckLocalDict(int count, const std::string &string) {
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
  }
  if (delta_idx < 5) delta_idx = 0;

  delta_encoding_.FeedAttrs(AttrValue(delta_idx), count);
  if (delta_idx != 0) {
    dict_idx_.FeedAttrs(AttrValue(dict_idx), count);
  }

  local_dict_.pop_front();
  local_dict_.push_back(string);

  return string.substr(delta_idx, string.length());
}

void StringModel::EndOfData() {
  markov_char_dist_.EndOfData();

  // init global dictionary
  global_dictionary_.EndOfData(encoding_methods_, squid_.splitter_);

#if kLocalDictSize > 0
  // init local dictionary
  delta_encoding_.EndOfData();
  dict_idx_.EndOfData();
#endif

  // end of squid learning
  delimiter_type_.EndOfData();
  encoding_methods_.EndOfData();
  num_words_squid_.EndOfData();
  word_length_.EndOfData();

  squid_.Init(GenerateStringStats());
}

int StringModel::GetModelCost() const {
  // Since we only have one type of string model, this number does not matter.
  return 0;
}

int StringModel::GetModelDescriptionLength() const { return 255 * 16 + 63 * 8; }

void StringModel::WriteModel(SequenceByteWriter *byte_writer) {
  markov_char_dist_.WriteMarkov(byte_writer);

  delimiter_type_.WriteModel(byte_writer);
  encoding_methods_.WriteModel(byte_writer);
  num_words_squid_.WriteModel(byte_writer);
  word_length_.WriteModel(byte_writer);

#if kLocalDictSize > 0
  delta_encoding_.WriteModel(byte_writer);
  dict_idx_.WriteModel(byte_writer);
#endif

  global_dictionary_.WriteDictionary(byte_writer, GetSquID(AttrVector(0)));
}

StringModel *StringModel::ReadModel(ByteReader *byte_reader, size_t index) {
  StringModel *model = new StringModel(index);

  model->markov_char_dist_.ReadMarkov(byte_reader);

  model->delimiter_type_ =
      *static_cast<TableCategorical *>(TableCategorical::ReadModel(byte_reader));
  model->encoding_methods_ =
      *static_cast<TableCategorical *>(TableCategorical::ReadModel(byte_reader));
  model->num_words_squid_ =
      *static_cast<TableCategorical *>(TableCategorical::ReadModel(byte_reader));

  TableNumericalIntCreator creator;
  model->word_length_ = *static_cast<TableNumerical *>(creator.ReadModel(byte_reader, Schema(), 0));

#if kLocalDictSize > 0
  model->delta_encoding_ =
      *static_cast<TableCategorical *>(TableCategorical::ReadModel(byte_reader));
  model->dict_idx_ = *static_cast<TableCategorical *>(TableCategorical::ReadModel(byte_reader));
#endif

  model->squid_.Init(model->GenerateStringStats());
  model->global_dictionary_.LoadDictionary(byte_reader, model->GetSquID(AttrVector(0)));
  model->squid_.Init(model->GenerateStringStats());

  return model;
}

StringStats StringModel::GenerateStringStats() {
  StringStats stats;
  stats.num_terms_squid_ = num_words_squid_.GetSquID();
  stats.delimiter_type_squid_ = delimiter_type_.GetSquID();
  stats.word_length_squid_ = word_length_.GetSquID();
  stats.encoding_method_squid_ = encoding_methods_.GetSquID();

#if kLocalDictSize > 0
  stats.delta_encoding_ = delta_encoding_.GetSquID();
  stats.dict_idx_ = dict_idx_.GetSquID();
#endif

  stats.markov_dist_ = &markov_char_dist_;
  stats.global_dict_ = &global_dictionary_;
  return stats;
}

SquIDModel *StringModelCreator::ReadModel(ByteReader *byte_reader, const Schema &schema,
                                          size_t index) {
  return StringModel::ReadModel(byte_reader, index);
}

SquIDModel *StringModelCreator::CreateModel(const std::vector<int> &attr_type,
                                            const std::vector<size_t> &predictor, size_t index,
                                            double err) {
  if (!predictor.empty()) return nullptr;

  return new StringModel(index);
}
}  // namespace db_compress
