/**
 * @file simple_prob_interval_pool
 * @brief This header is for class SimpleProbIntervalPool, which is for simple branch prob interval
 * reuse.
 */
#ifndef DB_COMPRESS_SIMPLE_PROB_INTERVAL_POOL_H
#define DB_COMPRESS_SIMPLE_PROB_INTERVAL_POOL_H
#include <memory>
#include "base.h"

namespace db_compress {
db_compress::Branch *GetSimpleBranch(int total_weights, unsigned int branch);
/**
 * For numerical model, we may meet some branch which is not in delayed coding params, e.g.
 * exponential branches or branches in the last layer of histogram part. We use this buffer to store
 * them for reuse. Note that these branches only have one segment.
 */
class SimpleProbIntervalFamily {
 public:
  explicit SimpleProbIntervalFamily(int weight) : weight_(weight) {
    special_branch_buffer_.resize(65536 / weight);
    for (int i = 0; i < special_branch_buffer_.size(); ++i)
      special_branch_buffer_[i] = std::make_unique<db_compress::Branch>(
          weight_, db_compress::ProbInterval(i * weight_, (i + 1) * weight_));
  }

  db_compress::Branch *GetBranch(int branch) {
    assert(branch < special_branch_buffer_.size());
    return special_branch_buffer_[branch].get();
  }

 private:
  int weight_;
  std::vector<std::unique_ptr<db_compress::Branch>> special_branch_buffer_;
};
/**
 * A pool_ has several families
 */
class SimpleProbIntervalPool {
 public:
  void RegisterWeight(int total_weight) {
    index_[total_weight] = static_cast<int>(pool_.size());
    pool_.emplace_back(total_weight);
  }

  db_compress::Branch *GetSimpleBranch(int total_weights, unsigned int branch) {
    if (index_.find(total_weights) == index_.end()) RegisterWeight(total_weights);
    int pool_idx = index_[total_weights];
    SimpleProbIntervalFamily &buffer = pool_[pool_idx];
    return buffer.GetBranch(branch);
  }

 private:
  std::map<int, int> index_;
  std::vector<SimpleProbIntervalFamily> pool_;
};
}  // namespace db_compress
#endif  // DB_COMPRESS_SIMPLE_PROB_INTERVAL_POOL_H
