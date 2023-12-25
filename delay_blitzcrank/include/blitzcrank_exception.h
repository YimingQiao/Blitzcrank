/**
 * @file squish_exception.h
 * @brief The header for exception class of squish.
 */

#ifndef DB_COMPRESS_SQUISH_EXCEPTION_H
#define DB_COMPRESS_SQUISH_EXCEPTION_H
#include <exception>
#include <iostream>

namespace db_compress {
class BufferOverflowException : public std::exception {
 public:
  explicit BufferOverflowException(std::string msg) : msg_(msg) {}

  const char *what() const noexcept override { return msg_.c_str(); }

 private:
  std::string msg_;
};

class IOException : public std::exception {
 public:
  explicit IOException(std::string msg) : msg_(msg) {}

  const char *what() const noexcept override { return msg_.c_str(); }

 private:
  std::string msg_;
};

class JsonLeafNodeIndexException : public std::exception {
 public:
  explicit JsonLeafNodeIndexException(std::string msg) : msg_(msg) {}

  const char *what() const noexcept override { return msg_.c_str(); }

 private:
  std::string msg_;
};
}  // namespace db_compress

#endif  // DB_COMPRESS_SQUISH_EXCEPTION_H
