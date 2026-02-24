#include "task_engine/job_queue.h"

namespace task_engine {

static std::deque<Job>& bucket_for(Job& job, std::deque<Job>& high, std::deque<Job>& normal, std::deque<Job>& low) {
  if (job.priority() == Priority::High) return high;
  if (job.priority() == Priority::Low) return low;
  
  return normal;
}

bool JobQueue::push(const Job& job) {
  {
    std::lock_guard<std::mutex> lock(mtx_);
    if (closed_) return false;

    if (job.priority() == Priority::High) high_.push_back(job);
    else if (job.priority() == Priority::Low) low_.push_back(job);
    else normal_.push_back(job);
  }
  cv_.notify_one();
  return true;
}

bool JobQueue::push(Job&& job) {
  {
    std::lock_guard<std::mutex> lock(mtx_);
    if (closed_) return false;

    const auto pri = job.priority();
    if (pri == Priority::High) high_.push_back(std::move(job));
    else if (pri == Priority::Low) low_.push_back(std::move(job));
    else normal_.push_back(std::move(job));
  }
  cv_.notify_one();
  return true;
}

bool JobQueue::pop(Job& out) {
  std::unique_lock<std::mutex> lock(mtx_);

  cv_.wait(lock, [&] {
    return closed_ || !high_.empty() || !normal_.empty() || !low_.empty();
  });

  if (high_.empty() && normal_.empty() && low_.empty()) {
    return false;
  }

  if (!high_.empty()) {
    out = std::move(high_.front());
    high_.pop_front();
    return true;
  }
  if (!normal_.empty()) {
    out = std::move(normal_.front());
    normal_.pop_front();
    return true;
  }

  out = std::move(low_.front());
  low_.pop_front();
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
  return high_.size() + normal_.size() + low_.size();
}

} // namespace task_engine
