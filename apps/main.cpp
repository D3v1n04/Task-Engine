#include <chrono>
#include <iostream>
#include <thread>
#include <memory>

#include "task_engine/scheduler.h"

using namespace task_engine;

static std::string state_to_string(JobState s) {
  switch (s) {
    case JobState::Pending: return "Pending";
    case JobState::Running: return "Running";
    case JobState::Success: return "Success";
    case JobState::Failed: return "Failed";
    case JobState::Retrying: return "Retrying";
    case JobState::Cancelled: return "Cancelled";
    case JobState::TimedOut: return "TimedOut";
    default: return "Unknown";
  }
}

int main() {
  Scheduler sched(3);

  // Backoff messages
  sched.logger().set_level(LogLevel::Debug);

  sched.start();

  JobId ids[8]{};

  for (int i = 0; i < 5; i++) {
    ids[i] = sched.submit(
        "fast_success_" + std::to_string(i),
        []() -> JobResult {
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
          return JobResult::Success();
        });
  }

  {
    auto counter = std::make_shared<int>(0);
    ids[5] = sched.submit(
        "fails_twice_then_ok",
        [counter]() -> JobResult {
          (*counter)++;
          if (*counter <= 2) return JobResult::Failure("planned fail");
          return JobResult::Success();
        },
        RetryPolicy{2, BackoffType::Fixed, std::chrono::milliseconds(100)}
    );
  }

  ids[6] = sched.submit(
      "always_fails",
      []() -> JobResult { return JobResult::Failure("always fails"); },
      RetryPolicy{2, BackoffType::Exponential, std::chrono::milliseconds(200)}
  );

  ids[7] = sched.submit(
      "long_timeout",
      []() -> JobResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        return JobResult::Success();
      },
      RetryPolicy{0},
      TimeoutPolicy{std::chrono::milliseconds(150)});

  sched.wait_all();
  sched.shutdown_graceful();

  std::cout << "\n Summary \n";
  for (JobId id : ids) {
    auto s = sched.snapshot(id);
    std::cout << "id=" << s.id
              << " name=" << s.name
              << " state=" << state_to_string(s.state)
              << " attempts=" << s.attempts
              << " last_ok=" << (s.last_result.ok ? "true" : "false")
              << " last_err=" << s.last_result.error
              << " last_ms=" << s.last_duration.count()
              << "\n";
  }

  auto st = sched.stats();

  std::cout << "\n Metrics \n";
  std::cout << "jobs_submitted=" << st.jobs_submitted << "\n";
  std::cout << "jobs_succeeded=" << st.jobs_succeeded << "\n";
  std::cout << "jobs_failed=" << st.jobs_failed << "\n";
  std::cout << "retries_performed=" << st.retries_performed << "\n";
  std::cout << "timeouts_recorded=" << st.timeouts_recorded << "\n";

  double avg = 0.0;
  if (st.completed_jobs > 0) {
    avg = static_cast<double>(st.total_runtime_ms) / static_cast<double>(st.completed_jobs);
  }
  std::cout << "avg_runtime_ms=" << avg << "\n";

  return 0;
}
