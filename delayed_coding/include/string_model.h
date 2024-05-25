/**
 * @file string_model.h
 * @brief The header file for categorical SquID and SquIDModel
 */

#ifndef STRING_MODEL_H
#define STRING_MODEL_H

#include <deque>
#include <map>
#include <queue>
#include <vector>

#include "base.h"
#include "categorical_model.h"
#include "model.h"
#include "numerical_model.h"
#include "string_squid.h"
#include "string_tools.h"

namespace db_compress {
/**
 * Squid Model for string attributes.
 */
class StringModel : public SquIDModel {
 public:
  StringSquID squid_;

  explicit StringModel(size_t target_var);

  StringSquID *GetSquID(const AttrVector &tuple);

  int GetModelCost() const override;

  void FeedAttrs(const AttrVector &attrs, int count) override;

  void EndOfData() override;

  void WriteModel(SequenceByteWriter *byte_writer) override;

  static StringModel *ReadModel(ByteReader *byte_reader, size_t index);

 private:
  // How many words in the given sentence.
  TableCategorical num_words_squid_;

  // Where is the word from? global dictionary, local dictionary, or neither.
  TableCategorical encoding_methods_;

  // Is this string a sentence or url?
  TableCategorical delimiter_type_;

  // How many characters in the word.
  TableNumerical word_length_;

  // Global and local dictionary. Local dictionary is used when the word is not in the global, and
  // it is not necessary to learn and store local dictionary.
  GlobalDictionary global_dictionary_;

  // local dictionary
  std::deque<std::string> local_dict_;
  TableCategorical dict_idx_;
  TableCategorical delta_encoding_;

  MarkovCharDist markov_char_dist_;

  StringStats GenerateStringStats();

  int GetModelDescriptionLength() const override;

  std::string CheckLocalDict(int count, const std::string &string);
};

class StringModelCreator : public ModelCreator {
 public:
  SquIDModel *ReadModel(ByteReader *byte_reader, const Schema &schema, size_t index) override;
  SquIDModel *CreateModel(const std::vector<int> &attr_type, const std::vector<size_t> &predictor,
                          size_t index, double err) override;
};

}  // namespace db_compress

#endif
