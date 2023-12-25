#include "data_io.h"

#include <chrono>
#include <cstring>
#include <iostream>
#include <random>

class Index {
public:
  std::vector<uint64_t> index_;

  explicit Index(std::vector<uint8_t> &stream_) {
    index_.resize(1, 0);
    uint64_t num_byte = 0;
    for (char c : stream_) {
      num_byte++;
      if (c == '\n')
        index_.push_back(num_byte);
    }
  }

  uint64_t LocateTuple(uint32_t tuple_idx) { return index_[tuple_idx]; }
};

int main(int argc, char **argv) {
  std::cout << "Hello, random access." << std::endl;

  if (argc != 2) {
    std::cout << "Usage: ./plain_ra [file_name]" << std::endl;
    return 0;
  }

  char file_name[100];
  strcpy(file_name, argv[1]);

  // Index Construction.
  db_compress::ByteReader reader(file_name);
  Index index(reader.stream_);
  uint32_t num_tuples = index.index_.size();

  // Seed Generator
  // std::random_device random_device;
  std::mt19937 mt19937(0);
  std::uniform_int_distribution<uint32_t> dist(0, num_tuples - 1);
  size_t size = 30000;
  std::vector<uint32_t> tuple_indices(size);
  for (size_t i = 0; i < tuple_indices.size(); i++)
    tuple_indices[i] = static_cast<int>(dist(mt19937));

//    int size = 5;
//      std::vector<uint32_t> tuple_indices(size);
//      for (int i = 0; i < size; i++)
//        tuple_indices[i] = 2 * i;

  // Random Access
  char c;
  uint32_t sym_idx = 0;
  std::vector<char> tuple(100000);
  auto start = std::chrono::system_clock::now();
  for (size_t i = 0; i < tuple_indices.size(); i++) {
    uint64_t offset = index.LocateTuple(tuple_indices[i]);
    reader.SetPos(0 + (offset << 3));

    while ((c = (char)reader.ReadByte()) != '\n')
      tuple[sym_idx++] = c;

//    for (size_t i = 0; i < sym_idx; ++i)
//      std::cout << tuple[i];
//    std::cout << "\n";

    sym_idx = 0;
  }
  auto end = std::chrono::system_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start);
  std::cout << "Time:  "
            << static_cast<double>(duration.count()) *
                   std::chrono::microseconds::period::num /
                   std::chrono::microseconds::period::den / (int)size * 1e6
            << " us\n";
}
