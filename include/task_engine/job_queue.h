#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>

#include "task_engine/job.h"

namespace task_engine {

class JobQueue {
public:
  JobQueue() = default;
  JobQueue(const JobQueue&) = delete;
  JobQueue& operator=(const JobQueue&) = delete;

  // Push a job into the queue
  // Returns false if queue is closed (job not accepted)
  bool push(const Job& job);
  bool push(Job&& job);

  // Blocking pop:
  // 1) blocks while queue is empty and not closed
  // 2) returns true if a job was popped into out
  // 3) returns false if queue is closed and empty (no more jobs will arrive)
  bool pop(Job& out);

  // Close queue:
  // 1) stops further pushes
  // 2) wakes all waiting pop() calls
  void close();

  bool is_closed() const;
  std::size_t size() const;

private:
  mutable std::mutex mtx_;
  std::condition_variable cv_;
  std::deque<Job> q_;
  bool closed_{false};
};

} // namespace task_engine
