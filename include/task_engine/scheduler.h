#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

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

struct SchedulerLimits {
  std::size_t max_concurrent_running{0}; // 0 = unlimited
  double dispatch_rate_per_sec{0.0};     // 0.0 = unlimited
  std::size_t dispatch_burst{0};         // 0 => default
};

class Scheduler {
public:
  explicit Scheduler(std::size_t num_workers);
  ~Scheduler();

  Scheduler(const Scheduler&) = delete;
  Scheduler& operator=(const Scheduler&) = delete;

  void set_limits(const SchedulerLimits& limits);
  SchedulerLimits limits() const;

  void start();

  JobId submit(std::string name, Job::Fn fn, RetryPolicy retry = {}, TimeoutPolicy timeout = {});

  JobId submit(std::string name, Job::Fn fn, Priority pri, RetryPolicy retry = {}, TimeoutPolicy timeout = {});

  bool cancel(JobId id);

  JobSnapshot snapshot(JobId id) const;

  void wait_all();
  void shutdown_graceful();

  Logger& logger() { return logger_; }

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

    bool cancel_requested{false};
  };

  struct Attempt {
    JobId id{};
    std::string name;
    Job::Fn user_fn;
    RetryPolicy retry{};
    TimeoutPolicy timeout{};
    Priority pri{Priority::Normal};

    std::chrono::steady_clock::time_point ready_at{std::chrono::steady_clock::now()};
  };

  // Dispatcher
  std::thread dispatcher_;
  std::atomic<bool> dispatcher_stop_{false};

  mutable std::mutex d_mtx_;
  std::condition_variable d_cv_;

  std::deque<std::shared_ptr<Attempt>> ready_high_;
  std::deque<std::shared_ptr<Attempt>> ready_normal_;
  std::deque<std::shared_ptr<Attempt>> ready_low_;

  std::size_t running_now_dispatcher_{0};

  void dispatcher_loop_();
  void push_ready_(std::shared_ptr<Attempt> a);
  std::shared_ptr<Attempt> pop_next_ready_();

  std::vector<std::shared_ptr<Attempt>> delayed_;
  static std::chrono::milliseconds compute_backoff_delay_(int next_attempt_num, const RetryPolicy& retry);

  class TokenBucket {
  public:
    void configure(double rate_per_sec, std::size_t burst) {
      std::lock_guard<std::mutex> lock(m_);
      rate_ = rate_per_sec;
      burst_ = (burst == 0 ? 1 : burst);
      tokens_ = static_cast<double>(burst_);
      last_ = std::chrono::steady_clock::now();
    }

    bool enabled() const {
      std::lock_guard<std::mutex> lock(m_);
      return rate_ > 0.0;
    }

    void acquire_blocking() {
      for (;;) {
        if (try_acquire()) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }

    bool try_consume_one() { return try_acquire(); }

  private:
    bool try_acquire() {
      std::lock_guard<std::mutex> lock(m_);

      if (rate_ <= 0.0) return true;

      const auto now = std::chrono::steady_clock::now();
      const std::chrono::duration<double> dt = now - last_;
      last_ = now;

      tokens_ += dt.count() * rate_;
      const double cap = static_cast<double>(burst_);
      if (tokens_ > cap) tokens_ = cap;

      if (tokens_ >= 1.0) {
        tokens_ -= 1.0;
        return true;
      }
      return false;
    }

    mutable std::mutex m_;
    double rate_{0.0};
    std::size_t burst_{1};
    double tokens_{0.0};
    std::chrono::steady_clock::time_point last_{std::chrono::steady_clock::now()};
  };

  bool should_retry(const Record& r, const JobResult& res) const;

  void enqueue_attempt(JobId id, const std::string& name, const Job::Fn& user_fn, RetryPolicy retry, TimeoutPolicy timeout, Priority pri);

private:
  std::size_t num_workers_;
  ThreadPool pool_;
  Logger logger_;

  SchedulerLimits limits_{};
  TokenBucket dispatch_bucket_;

  SchedulerStats stats_{};

  std::atomic<JobId> next_id_{1};
  std::atomic<bool> accepting_{true};
  std::atomic<bool> started_{false};

  mutable std::mutex mtx_;
  std::unordered_map<JobId, Record> records_;

  mutable std::condition_variable cv_;
  std::size_t active_logical_jobs_{0};
};

} // namespace task_engine
