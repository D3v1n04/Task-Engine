#pragma once

#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

#include "task_engine/job.h"
#include "task_engine/job_queue.h"
#include "task_engine/logger.h"

namespace task_engine {

class ThreadPool {
public:
  ThreadPool() = default;
  explicit ThreadPool(std::size_t num_workers);

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  ~ThreadPool();

  // Start worker threads (call once)
  void start(std::size_t num_workers);
  void start(); // uses num_workers if provided

  // Submit a Job into the internal queue
  // Returns false if pool is stopping/closed
  bool submit(const Job& job);
  bool submit(Job&& job);

  // Stop the pool:
  // 1) closes queue
  // 2) joins workers
  void stop();

  bool is_running() const { return running_.load(); }
  std::size_t worker_count() const { return workers_.size(); }

  // Logger
  void set_logger(Logger* logger) { logger_ = logger; }

private:
  void worker_loop(std::size_t worker_index);

  JobQueue queue_;
  std::vector<std::thread> workers_;
  std::atomic<bool> running_{false};
  std::atomic<bool> stop_requested_{false};
  std::size_t configured_workers_{0};

  Logger* logger_{nullptr};
};

} // namespace task_engine
