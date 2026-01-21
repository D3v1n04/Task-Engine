#include "task_engine/thread_pool.h"

#include <utility>

namespace task_engine {

ThreadPool::ThreadPool(std::size_t num_workers) : configured_workers_(num_workers) {}

ThreadPool::~ThreadPool() {
  stop();
}

void ThreadPool::start() {
  if (configured_workers_ == 0) configured_workers_ = std::thread::hardware_concurrency();
  if (configured_workers_ == 0) configured_workers_ = 2; // fallback
  start(configured_workers_);
}

void ThreadPool::start(std::size_t num_workers) {
  if (running_.load()) return; // already running

  configured_workers_ = num_workers;
  stop_requested_.store(false);
  running_.store(true);

  workers_.reserve(num_workers);
  for (std::size_t i = 0; i < num_workers; i++) {
    workers_.emplace_back([this, i] { worker_loop(i); });
  }
}

bool ThreadPool::submit(const Job& job) {
  if (!running_.load() || stop_requested_.load()) return false;
  return queue_.push(job);
}

bool ThreadPool::submit(Job&& job) {
  if (!running_.load() || stop_requested_.load()) return false;
  return queue_.push(std::move(job));
}

void ThreadPool::stop() {
  // If never started, there is nothing to do
  if (!running_.load()) return;

  stop_requested_.store(true);
  queue_.close();

  for (auto& t : workers_) {
    if (t.joinable()) t.join();
  }
  workers_.clear();

  running_.store(false);
}

void ThreadPool::worker_loop(std::size_t worker_index) {
  Job job(0, "dummy", [] { return JobResult::Success(); });

  while (queue_.pop(job)) {
    auto res = job.run_once();

    // Thread-safe structured logging (no interleaving mess)
    if (!res.ok && logger_) {
      logger_->warn(job.id(),
                    "worker " + std::to_string(worker_index) + " saw failure: " + res.error);
    }
  }
}

} // namespace task_engine
