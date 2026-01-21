#include "task_engine/job_queue.h"

namespace task_engine {

bool JobQueue::push(const Job& job) {
  {
    std::lock_guard<std::mutex> lock(mtx_);
    if (closed_) return false;
    q_.push_back(job);
  }
  cv_.notify_one();
  return true;
}

bool JobQueue::push(Job&& job) {
  {
    std::lock_guard<std::mutex> lock(mtx_);
    if (closed_) return false;
    q_.push_back(std::move(job));
  }
  cv_.notify_one();
  return true;
}

bool JobQueue::pop(Job& out) {
  std::unique_lock<std::mutex> lock(mtx_);

  // Wait until we have data or the queue is closed
  cv_.wait(lock, [&] { return closed_ || !q_.empty(); });

  // If closed and empty, we are done
  if (q_.empty()) {
    return false;
  }

  out = std::move(q_.front());
  q_.pop_front();
  return true;
}

void JobQueue::close() {
  {
    std::lock_guard<std::mutex> lock(mtx_);
    closed_ = true;
  }
  cv_.notify_all();
}

bool JobQueue::is_closed() const {
  std::lock_guard<std::mutex> lock(mtx_);
  return closed_;
}

std::size_t JobQueue::size() const {
  std::lock_guard<std::mutex> lock(mtx_);
  return q_.size();
}

} // namespace task_engine
