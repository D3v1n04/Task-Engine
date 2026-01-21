#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include "task_engine/job.h"
#include "task_engine/logger.h"
#include "task_engine/result.h"
#include "task_engine/thread_pool.h"
#include "task_engine/types.h"

namespace task_engine {

struct JobSnapshot {
  JobId id{};
  std::string name;
  JobState state{JobState::Pending};
  int attempts{0};
  int max_retries{0};
  JobResult last_result{};
  std::chrono::milliseconds last_duration{0};
};

struct SchedulerStats {
  std::uint64_t jobs_submitted{0};
  std::uint64_t jobs_succeeded{0};
  std::uint64_t jobs_failed{0};
  std::uint64_t retries_performed{0};
  std::uint64_t timeouts_recorded{0};

  std::uint64_t completed_jobs{0};
  std::uint64_t total_runtime_ms{0};
};

class Scheduler {
public:
  explicit Scheduler(std::size_t num_workers);
  ~Scheduler();

  Scheduler(const Scheduler&) = delete;
  Scheduler& operator=(const Scheduler&) = delete;

  void start();

  JobId submit(std::string name, Job::Fn fn, RetryPolicy retry = {}, TimeoutPolicy timeout = {});

  JobSnapshot snapshot(JobId id) const;

  void wait_all();
  void shutdown_graceful();

  Logger& logger() { return logger_; }

  // Stats
  SchedulerStats stats() const;

private:
  struct Record {
    JobId id{};
    std::string name;

    JobState state{JobState::Pending};
    int attempts{0};
    int max_retries{0};

    JobResult last_result{JobResult::Success()};
    std::chrono::milliseconds last_duration{0};
  };

  bool should_retry(const Record& r, const JobResult& res) const;

  void enqueue_attempt(JobId id, const std::string& name, const Job::Fn& user_fn, RetryPolicy retry, TimeoutPolicy timeout);

  std::size_t num_workers_;
  ThreadPool pool_;
  Logger logger_;

  // Stats stored once per scheduler
  SchedulerStats stats_;

  std::atomic<JobId> next_id_{1};
  std::atomic<bool> accepting_{true};
  std::atomic<bool> started_{false};

  mutable std::mutex mtx_;
  std::unordered_map<JobId, Record> records_;

  mutable std::condition_variable cv_;
  std::size_t active_logical_jobs_{0};
};

} // namespace task_engine
