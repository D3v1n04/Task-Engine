#include "task_engine/scheduler.h"

#include <utility>
#include <thread>
#include <algorithm>
#include <cstdint>

namespace task_engine {

Scheduler::Scheduler(std::size_t num_workers)
    : num_workers_(num_workers), pool_(num_workers) {}

Scheduler::~Scheduler() {
  shutdown_graceful();
}

void Scheduler::start() {
  bool expected = false;
  if (!started_.compare_exchange_strong(expected, true)) return;

  pool_.start();
  pool_.set_logger(&logger_);

  logger_.info("scheduler started with " + std::to_string(num_workers_) + " workers");
}

JobId Scheduler::submit(std::string name, Job::Fn fn, RetryPolicy retry, TimeoutPolicy timeout) {
  if (!started_.load()) start();

  if (!accepting_.load()) {
    return 0; // 0 = invalid id in this MVP
  }

  const JobId id = next_id_.fetch_add(1);

  {
    std::lock_guard<std::mutex> lock(mtx_);
    Record r;
    r.id = id;
    r.name = name;
    r.state = JobState::Pending;
    r.attempts = 0;
    r.max_retries = retry.max_retries;
    r.last_result = JobResult::Success();
    r.last_duration = std::chrono::milliseconds(0);

    records_[id] = std::move(r);
    active_logical_jobs_++;

    // Metrics
    stats_.jobs_submitted++;
  }

  logger_.info(id, "submitted: " + name);

  enqueue_attempt(id, name, fn, retry, timeout);
  return id;
}

bool Scheduler::should_retry(const Record& r, const JobResult& res) const {
  if (res.ok) return false;

  const int max_attempts = 1 + r.max_retries;
  return r.attempts < max_attempts;
}

void Scheduler::enqueue_attempt(JobId id, const std::string& name, const Job::Fn& user_fn, RetryPolicy retry, TimeoutPolicy timeout) {
  Job j(id, name, [this, id, name, user_fn, retry, timeout]() -> JobResult {
        int attempt_num = 0;
        bool is_retry = false;

        {
          std::lock_guard<std::mutex> lock(mtx_);
          auto& r = records_.at(id);
          is_retry = (r.attempts > 0);
          r.state = is_retry ? JobState::Retrying : JobState::Running;
          r.attempts++;
          attempt_num = r.attempts;

          // Metrics
          if (attempt_num > 1) {
            stats_.retries_performed++;
          }
        }

        logger_.info(id, std::string(is_retry ? "retrying" : "starting") + " attempt " + std::to_string(attempt_num));

        // Backoff delay (only for retries)
        if (is_retry && retry.base_delay.count() > 0 && retry.backoff != BackoffType::None) {
          std::chrono::milliseconds delay{0};

          if (retry.backoff == BackoffType::Fixed) {
            delay = retry.base_delay;
          } else if (retry.backoff == BackoffType::Exponential) {
            // attempt_num: 2 => 1x, 3 => 2x, 4 => 4x, ...
            const int exp = std::max(0, attempt_num - 2);
            const int capped = std::min(exp, 20);        // clamp exponent
            const std::uint64_t mult = (1ULL << capped);

            const long long base = retry.base_delay.count();
            delay = std::chrono::milliseconds(base * static_cast<long long>(mult));
          }

          // Safety clamp (MVP): max 5 seconds
          const auto max_delay = std::chrono::milliseconds(5000);
          if (delay > max_delay) delay = max_delay;

          logger_.debug(id, "backoff sleeping " + std::to_string(delay.count()) + "ms");
          std::this_thread::sleep_for(delay);
        }

        const auto t0 = std::chrono::steady_clock::now();

        JobResult res = JobResult::Failure("unknown error");
        try {
          res = user_fn();
        } catch (const std::exception& e) {
          res = JobResult::Failure(std::string("exception: ") + e.what());
        } catch (...) {
          res = JobResult::Failure("exception: unknown");
        }

        const auto t1 = std::chrono::steady_clock::now();
        const auto dur =
            std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);

        const bool timed_out =
            (timeout.timeout.count() > 0 && dur > timeout.timeout);

        if (timed_out) {
          res = JobResult::Failure("timeout");
        }

        bool retrying = false;
        bool terminal = false;

        {
          std::lock_guard<std::mutex> lock(mtx_);
          auto& r = records_.at(id);

          r.last_result = res;
          r.last_duration = dur;

          if (res.ok) {
            r.state = JobState::Success;
            terminal = true;
          } else {
            if (timed_out) {
              r.state = JobState::TimedOut;
            } else {
              r.state = JobState::Failed;
            }

            if (should_retry(r, res)) {
              retrying = true;
              r.state = JobState::Retrying;
            } else {
              terminal = true;
              r.state = JobState::Failed;
            }
          }
        }

        // Logging after state decision
        if (res.ok) {
          logger_.info(id, "success in " + std::to_string(dur.count()) + "ms");
        } else if (timed_out) {
          logger_.error(id, "timed out after " + std::to_string(dur.count()) + "ms");
        } else if (retrying) {
          logger_.warn(id, "failed (" + res.error + "), retrying...");
        } else {
          logger_.error(id, "failed (" + res.error + "), giving up");
        }

        if (retrying && accepting_.load()) {
          enqueue_attempt(id, name, user_fn, retry, timeout);
        }

        if (terminal) {
          std::lock_guard<std::mutex> lock(mtx_);

          // Metrics (count once per job)
          if (res.ok) {
            stats_.jobs_succeeded++;
          } else {
            stats_.jobs_failed++;
          }

          stats_.completed_jobs++;
          stats_.total_runtime_ms += static_cast<std::uint64_t>(dur.count());

          if (timed_out) {
            stats_.timeouts_recorded++;
          }

          if (active_logical_jobs_ > 0) active_logical_jobs_--;
          cv_.notify_all();
        }

        return res;
      },
      retry,
      timeout);

  pool_.submit(std::move(j));
}

JobSnapshot Scheduler::snapshot(JobId id) const {
  std::lock_guard<std::mutex> lock(mtx_);
  JobSnapshot s;

  auto it = records_.find(id);
  if (it == records_.end()) return s;

  const Record& r = it->second;
  s.id = r.id;
  s.name = r.name;
  s.state = r.state;
  s.attempts = r.attempts;
  s.max_retries = r.max_retries;
  s.last_result = r.last_result;
  s.last_duration = r.last_duration;
  return s;
}

SchedulerStats Scheduler::stats() const {
  std::lock_guard<std::mutex> lock(mtx_);
  return stats_;
}

void Scheduler::wait_all() {
  std::unique_lock<std::mutex> lock(mtx_);
  cv_.wait(lock, [&] { return active_logical_jobs_ == 0; });
}

void Scheduler::shutdown_graceful() {
  accepting_.store(false);

  if (started_.load()) {
    logger_.info("scheduler shutting down...");
    pool_.stop();
    logger_.info("scheduler stopped");
  }

  {
    std::lock_guard<std::mutex> lock(mtx_);
    cv_.notify_all();
  }
}

} // namespace task_engine
