//
// Created by Qiao Yiming on 2022/3/3.
//

#ifndef DB_COMPRESS_SQUISH_EXCEPTION_H
#define DB_COMPRESS_SQUISH_EXCEPTION_H
#include <exception>
#include <iostream>

namespace db_compress {
class BufferOverflowException : public std::exception {
 public:
  BufferOverflowException(std::string msg) : msg_(msg) {}

  const char *what() const noexcept override { return msg_.c_str(); }

 private:
  std::string msg_;
};

class IOException : public std::exception {
 public:
  IOException(std::string msg) : msg_(msg) {}

  const char *what() const noexcept override { return msg_.c_str(); }

 private:
  std::string msg_;
};
}  // namespace db_compress

#endif  // DB_COMPRESS_SQUISH_EXCEPTION_H
