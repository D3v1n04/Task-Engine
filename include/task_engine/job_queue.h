#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>

#include "task_engine/job.h"
#include "task_engine/types.h"

namespace task_engine {

class JobQueue {
public:
  JobQueue() = default;
  JobQueue(const JobQueue&) = delete;
  JobQueue& operator=(const JobQueue&) = delete;

  bool push(const Job& job);
  bool push(Job&& job);

  bool pop(Job& out);

  void close();

  bool is_closed() const;
  std::size_t size() const;

private:
  mutable std::mutex mtx_;
  std::condition_variable cv_;

  // FIFO within each priority bucket
  std::deque<Job> high_;
  std::deque<Job> normal_;
  std::deque<Job> low_;

  bool closed_{false};
};

} // namespace task_engine
