#pragma once
#include <chrono>
#include <functional>
#include <optional>
#include <string>

#include "task_engine/result.h"
#include "task_engine/types.h"

namespace task_engine {

enum class BackoffType {
  None,
  Fixed,
  Exponential
};

struct RetryPolicy {
  int max_retries = 0; // retries after the first attempt

  BackoffType backoff = BackoffType::None;

  // For Fixed and Exponential backoff
  // If 0ms, it behaves like None.
  std::chrono::milliseconds base_delay{0};
};


struct TimeoutPolicy {
  std::chrono::milliseconds timeout{0}; // 0 = no timeout (MVP)
};

class Job {
public:
  using Fn = std::function<JobResult()>; // returning JobResult makes retries easy

  Job(JobId id, std::string name, Fn fn, RetryPolicy retry = {}, TimeoutPolicy timeout = {});

  JobId id() const { return id_; }
  const std::string& name() const { return name_; }

  JobState state() const { return state_; }
  int attempts() const { return attempts_; }

  int max_retries() const { return retry_.max_retries; }
  std::chrono::milliseconds timeout() const { return timeout_.timeout; }

  // Executes one attempt (scheduler will call this later)
  JobResult run_once();

private:
  JobId id_;
  std::string name_;
  Fn fn_;

  RetryPolicy retry_;
  TimeoutPolicy timeout_;

  JobState state_{JobState::Pending};
  int attempts_{0};

  std::chrono::steady_clock::time_point created_at_;
  std::optional<std::chrono::steady_clock::time_point> last_start_;
  std::optional<std::chrono::steady_clock::time_point> last_finish_;
};

} // namespace task_engine
