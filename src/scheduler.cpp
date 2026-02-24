#include "task_engine/scheduler.h"

#include <algorithm>
#include <cstdint>
#include <thread>
#include <utility>

namespace task_engine {

// Dispatchers

void Scheduler::push_ready_(std::shared_ptr<Attempt> a) {
  if (!a) return;
  if (a->pri == Priority::High) ready_high_.push_back(std::move(a));
  else if (a->pri == Priority::Low) ready_low_.push_back(std::move(a));
  else ready_normal_.push_back(std::move(a));
}

std::shared_ptr<Scheduler::Attempt> Scheduler::pop_next_ready_() {
  if (!ready_high_.empty()) {
    auto a = ready_high_.front();
    ready_high_.pop_front();
    return a;
  }
  if (!ready_normal_.empty()) {
    auto a = ready_normal_.front();
    ready_normal_.pop_front();
    return a;
  }
  if (!ready_low_.empty()) {
    auto a = ready_low_.front();
    ready_low_.pop_front();
    return a;
  }
  return {};
}

std::chrono::milliseconds Scheduler::compute_backoff_delay_(int next_attempt_num,const RetryPolicy& retry) {
  // next_attempt_num is 1 based. Attempt 1 => no backoff.
  if (next_attempt_num <= 1) return std::chrono::milliseconds(0);
  if (retry.base_delay.count() <= 0) return std::chrono::milliseconds(0);
  if (retry.backoff == BackoffType::None) return std::chrono::milliseconds(0);

  std::chrono::milliseconds delay{0};

  if (retry.backoff == BackoffType::Fixed) {
    delay = retry.base_delay;
  } else if (retry.backoff == BackoffType::Exponential) {
    const int exp = std::max(0, next_attempt_num - 2);
    const int capped = std::min(exp, 20);
    const std::uint64_t mult = (1ULL << capped);
    delay = std::chrono::milliseconds(
        retry.base_delay.count() * static_cast<long long>(mult));
  }

  const auto max_delay = std::chrono::milliseconds(5000);
  if (delay > max_delay) delay = max_delay;

  return delay;
}

void Scheduler::dispatcher_loop_() {
  std::unique_lock<std::mutex> lk(d_mtx_);

  while (!dispatcher_stop_.load()) {
    // Promote delayed -> ready when their time arrives
    const auto now = std::chrono::steady_clock::now();
    if (!delayed_.empty()) {
      std::vector<std::shared_ptr<Attempt>> still_waiting;
      still_waiting.reserve(delayed_.size());

      for (auto& a : delayed_) {
        if (a && a->ready_at <= now) {
          push_ready_(a);
        } else {
          still_waiting.push_back(a);
        }
      }
      delayed_.swap(still_waiting);
    }

    // Compute next delayed wake time
    auto next_wake = std::chrono::steady_clock::time_point::max();
    for (const auto& a : delayed_) {
      if (a && a->ready_at < next_wake) next_wake = a->ready_at;
    }

    // Wait for work OR until the next delayed attempt becomes ready
    if (next_wake == std::chrono::steady_clock::time_point::max()) {
      d_cv_.wait(lk, [&] {
        return dispatcher_stop_.load() || !ready_high_.empty() || !ready_normal_.empty() || !ready_low_.empty() || !delayed_.empty();
      });
    } else {
      d_cv_.wait_until(lk, next_wake, [&] {
        return dispatcher_stop_.load() || !ready_high_.empty() || !ready_normal_.empty() || !ready_low_.empty();
      });
    }

    if (dispatcher_stop_.load()) break;

    // After waking, loop back to promote delayed
    // Try dispatching as much as possible before going back to sleep
    for (;;) {
      auto a = pop_next_ready_();
      if (!a) break;

      // Read cap safely: unlock d_mtx_ while grabbing mtx_
      lk.unlock();
      std::size_t cap = 0;
      {
        std::lock_guard<std::mutex> slk(mtx_);
        cap = limits_.max_concurrent_running;
      }
      lk.lock();

      // Concurrency gate
      if (cap > 0 && running_now_dispatcher_ >= cap) {
        // Put back at the front
        if (a->pri == Priority::High) ready_high_.push_front(a);
        else if (a->pri == Priority::Low) ready_low_.push_front(a);
        else ready_normal_.push_front(a);

        d_cv_.wait(lk, [&] {
          if (dispatcher_stop_.load()) return true;
          if (cap == 0) return true;
          return running_now_dispatcher_ < cap;
        });
        if (dispatcher_stop_.load()) return;
        continue;
      }

      // Rate gate
      if (dispatch_bucket_.enabled()) {
        if (!dispatch_bucket_.try_consume_one()) {
          // Put back at front and wait briefly
          if (a->pri == Priority::High) ready_high_.push_front(a);
          else if (a->pri == Priority::Low) ready_low_.push_front(a);
          else ready_normal_.push_front(a);

          lk.unlock();
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          lk.lock();
          continue;
        }
      }

      // Reserve slot BEFORE dispatch
      running_now_dispatcher_++;

      // Build the actual worker job
      Job j(
          a->id,
          a->name,
          [this, a]() -> JobResult {
            // Always free the dispatcher slot when this attempt finishes
            struct DonePing {
              Scheduler* s{nullptr};
              ~DonePing() {
                std::lock_guard<std::mutex> lk(s->d_mtx_);
                if (s->running_now_dispatcher_ > 0) s->running_now_dispatcher_--;
                s->d_cv_.notify_one();
              }
            } ping{this};

            int attempt_num = 0;
            bool is_retry = false;

            // Early cancel check and bookkeeping
            bool cancelled_now = false;
            {
              std::lock_guard<std::mutex> lock(mtx_);
              auto& r = records_.at(a->id);

              if (r.cancel_requested || r.state == JobState::Cancelled) {
                r.state = JobState::Cancelled;

                stats_.jobs_failed++;
                stats_.completed_jobs++;

                if (active_logical_jobs_ > 0) active_logical_jobs_--;
                cv_.notify_all();

                cancelled_now = true;
              } else {
                is_retry = (r.attempts > 0);
                r.state = is_retry ? JobState::Retrying : JobState::Running;
                r.attempts++;
                attempt_num = r.attempts;

                if (attempt_num > 1) stats_.retries_performed++;
              }
            }

            if (cancelled_now) {
              logger_.warn(a->id, "cancelled before execution");
              return JobResult::Failure("cancelled");
            }
            // 
            logger_.info(a->id, is_retry ? "retrying" : "starting", {
              {"event", "job_start"},
              {"attempt", std::to_string(attempt_num)}
            });

            // Backoff is handled by dispatcher (NO sleeping)
            // Execute user function
            const auto t0 = std::chrono::steady_clock::now();

            JobResult res = JobResult::Failure("unknown error");
            try {
              res = a->user_fn();
            } catch (const std::exception& e) {
              res = JobResult::Failure(std::string("exception: ") + e.what());
            } catch (...) {
              res = JobResult::Failure("exception: unknown");
            }

            const auto t1 = std::chrono::steady_clock::now();
            const auto dur = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0);

            // Timeout handling
            const bool timed_out = (a->timeout.timeout.count() > 0 && dur > a->timeout.timeout);
            if (timed_out) res = JobResult::Failure("timeout");

            bool retrying = false;
            bool terminal = false;

            {
              std::lock_guard<std::mutex> lock(mtx_);
              auto& r = records_.at(a->id);

              r.last_result = res;
              r.last_duration = dur;

              if (res.ok) {
                r.state = JobState::Success;
                terminal = true;
              } else if (timed_out) {
                r.state = JobState::TimedOut;
                terminal = true;
              } else {
                if (should_retry(r, res)) {
                  retrying = true;
                  r.state = JobState::Retrying;
                } else {
                  terminal = true;
                  r.state = JobState::Failed;
                }
              }
            }
            
            const std::string attempt_s = std::to_string(attempt_num);
            const std::string dur_s = std::to_string(dur.count());
            // Logging
            if (res.ok) {
              logger_.info(a->id, "success", {
                {"event","job_finish"},
                {"attempt", attempt_s},
                {"duration_ms", dur_s},
                {"state","Success"},
                {"name", a->name},
                {"pri", (a->pri == Priority::High ? "High" : (a->pri == Priority::Low ? "Low" : "Normal"))}
              });
            } else if (timed_out) {
              logger_.error(a->id, "timed_out", {
                {"event","job_finish"},
                {"attempt", attempt_s},
                {"duration_ms", dur_s},
                {"state","TimedOut"},
                {"error","timeout"},
                {"name", a->name},
                {"pri", (a->pri == Priority::High ? "High" : (a->pri == Priority::Low ? "Low" : "Normal"))}
              });
            } else if (retrying) {
              logger_.warn(a->id, "retry_scheduled", {
                {"event","job_retry"},
                {"attempt", attempt_s},
                {"error", res.error},
                {"name", a->name},
                {"pri", (a->pri == Priority::High ? "High" : (a->pri == Priority::Low ? "Low" : "Normal"))}
              });
            } else {
              logger_.error(a->id, "failed", {
                {"event","job_finish"},
                {"attempt", attempt_s},
                {"state","Failed"},
                {"error", res.error},
                {"duration_ms", dur_s},
                {"name", a->name},
                {"pri", (a->pri == Priority::High ? "High" : (a->pri == Priority::Low ? "Low" : "Normal"))}
              });
            }

            // Prevent retries after cancel requested
            bool cancel_now = false;
            {
              std::lock_guard<std::mutex> lock(mtx_);
              cancel_now = records_.at(a->id).cancel_requested;
            }

            // Preserve priority on retries
            if (retrying && accepting_.load() && !cancel_now) {
              enqueue_attempt(a->id, a->name, a->user_fn, a->retry, a->timeout, a->pri);
            }

            // Terminal accounting
            if (terminal) {
              std::lock_guard<std::mutex> lock(mtx_);

              if (res.ok) stats_.jobs_succeeded++;
              else stats_.jobs_failed++;

              stats_.completed_jobs++;
              stats_.total_runtime_ms += static_cast<std::uint64_t>(dur.count());

              if (timed_out) stats_.timeouts_recorded++;

              if (active_logical_jobs_ > 0) active_logical_jobs_--;
              cv_.notify_all();
            }

            return res;
          },
          a->retry,
          a->timeout,
          a->pri);

      // Submit without holding dispatcher lock
      lk.unlock();
      pool_.submit(std::move(j));
      lk.lock();
    }
  }
}

// Scheduler
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

  dispatcher_stop_.store(false);
  dispatcher_ = std::thread([this] { dispatcher_loop_(); });

  logger_.info("scheduler started with " + std::to_string(num_workers_) + " workers");
}

// ---------------------- Limits API ----------------------

void Scheduler::set_limits(const SchedulerLimits& limits) {
  {
    std::lock_guard<std::mutex> lock(mtx_);
    limits_ = limits;
  }

  // Configure token bucket outside scheduler lock
  if (limits.dispatch_rate_per_sec > 0.0) {
    std::size_t burst = limits.dispatch_burst;
    if (burst == 0) {
      burst = static_cast<std::size_t>(std::max(1.0, limits.dispatch_rate_per_sec));
    }
    dispatch_bucket_.configure(limits.dispatch_rate_per_sec, burst);
  } else {
    dispatch_bucket_.configure(0.0, 1); // disable
  }
}

SchedulerLimits Scheduler::limits() const {
  std::lock_guard<std::mutex> lock(mtx_);
  return limits_;
}

// Existing submit: defaults to Priority::Normal
JobId Scheduler::submit(std::string name, Job::Fn fn, RetryPolicy retry, TimeoutPolicy timeout) {
  return submit(std::move(name), std::move(fn), Priority::Normal, retry, timeout);
}

JobId Scheduler::submit(std::string name, Job::Fn fn, Priority pri, RetryPolicy retry, TimeoutPolicy timeout) {
  if (!started_.load()) start();
  if (!accepting_.load()) return 0;

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
    r.cancel_requested = false;

    records_[id] = std::move(r);
    active_logical_jobs_++;

    stats_.jobs_submitted++;
  }

  logger_.info(id, "submitted: " + name);
  enqueue_attempt(id, name, fn, retry, timeout, pri);
  return id;
}

bool Scheduler::should_retry(const Record& r, const JobResult& res) const {
  if (res.ok) return false;
  const int max_attempts = 1 + r.max_retries;
  return r.attempts < max_attempts;
}

bool Scheduler::cancel(JobId id) {
  bool did_request = false;

  {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = records_.find(id);
    if (it == records_.end()) return false;

    auto& r = it->second;

    if (r.state == JobState::Success ||
        r.state == JobState::Failed ||
        r.state == JobState::TimedOut ||
        r.state == JobState::Cancelled) {
      return false;
    }

    r.cancel_requested = true;
    r.state = JobState::Cancelled; // MVP behavior: cancel immediately in record
    did_request = true;
  }

  if (did_request) logger_.warn(id, "cancel requested");
  return did_request;
}

// Enqueue_attempt enqueues into dispatcher ready queues (not directly into pool)
void Scheduler::enqueue_attempt(JobId id, const std::string& name,const Job::Fn& user_fn, 
                                RetryPolicy retry, TimeoutPolicy timeout, Priority pri) {
  auto a = std::make_shared<Attempt>();
  a->id = id;
  a->name = name;
  a->user_fn = user_fn;
  a->retry = retry;
  a->timeout = timeout;
  a->pri = pri;

  // Determine the next attempt number (attempts increments at attempt start)
  int next_attempt_num = 1;
  {
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = records_.find(id);
    if (it != records_.end()) {
      next_attempt_num = it->second.attempts + 1;
    }
  }

  // Compute dispatcher side backoff for retries (attempt >= 2)
  std::chrono::milliseconds delay{0};
  if (next_attempt_num >= 2 &&
      retry.base_delay.count() > 0 &&
      retry.backoff != BackoffType::None) {
    if (retry.backoff == BackoffType::Fixed) {
      delay = retry.base_delay;
    } else if (retry.backoff == BackoffType::Exponential) {
      const int exp = std::max(0, next_attempt_num - 2);
      const int capped = std::min(exp, 20);
      const std::uint64_t mult = (1ULL << capped);

      const long long base = retry.base_delay.count();
      delay = std::chrono::milliseconds(base * static_cast<long long>(mult));
    }

    const auto max_delay = std::chrono::milliseconds(5000);
    if (delay > max_delay) delay = max_delay;
  }

  a->ready_at = std::chrono::steady_clock::now() + delay;

  if (delay.count() > 0) {
    logger_.debug(id, "backoff scheduled " + std::to_string(delay.count()) + "ms");
  }

  {
    std::lock_guard<std::mutex> lk(d_mtx_);
    if (delay.count() == 0) {
      push_ready_(std::move(a));
    } else {
      delayed_.push_back(std::move(a));
    }
  }
  d_cv_.notify_one();
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

    dispatcher_stop_.store(true);
    {
      std::lock_guard<std::mutex> lk(d_mtx_);
      d_cv_.notify_all();
    }
    if (dispatcher_.joinable()) dispatcher_.join();

    pool_.stop();
    logger_.info("scheduler stopped");
  }

  {
    std::lock_guard<std::mutex> lock(mtx_);
    cv_.notify_all();
  }
}

} // namespace task_engine
