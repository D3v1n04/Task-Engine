#include "task_engine/job.h"
#include <utility>

namespace task_engine {

Job::Job(JobId id, std::string name, Fn fn, RetryPolicy retry, TimeoutPolicy timeout)
    : id_(id),
      name_(std::move(name)),
      fn_(std::move(fn)),
      retry_(retry),
      timeout_(timeout),
      created_at_(std::chrono::steady_clock::now()) {}

JobResult Job::run_once() {
  state_ = JobState::Running;
  attempts_++;

  last_start_ = std::chrono::steady_clock::now();

  JobResult res = JobResult::Failure("unknown error");
  try {
    res = fn_();
  } catch (const std::exception& e) {
    res = JobResult::Failure(std::string("exception: ") + e.what());
  } catch (...) {
    res = JobResult::Failure("exception: unknown");
  }

  last_finish_ = std::chrono::steady_clock::now();

  state_ = res.ok ? JobState::Success : JobState::Failed;
  return res;
}

} // namespace task_engine
