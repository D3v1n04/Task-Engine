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
    case JobState::CancelRequested: return "CancelRequested";
    case JobState::Cancelled: return "Cancelled";
    case JobState::TimedOut: return "TimedOut";
    default: return "Unknown";
  }
}

int main() {
  Scheduler sched(3);

  sched.logger().set_level(LogLevel::Debug);
  sched.logger().set_json(true);

  sched.start();

  SchedulerLimits lim;
  lim.max_concurrent_running = 1;
  lim.dispatch_rate_per_sec = 5.0;
  lim.dispatch_burst = 2;
  sched.set_limits(lim);

  // Priority demo
  for (int i = 0; i < 25; i++) {
    sched.submit(
        "low_" + std::to_string(i),
        []() -> JobResult {
          std::this_thread::sleep_for(std::chrono::milliseconds(30));
          return JobResult::Success();
        },
        Priority::Low
    );
  }

  JobId high_ping = sched.submit(
      "HIGH_ping",
      []() -> JobResult { return JobResult::Success(); },
      Priority::High
  );

  // Cancel demo
  JobId cancel_me = sched.submit(
      "cancel_me",
      []() -> JobResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(800));
        return JobResult::Success();
      },
      Priority::Normal
  );
  sched.cancel(cancel_me);

  // Demo jobs
  JobId ids[8]{};

  for (int i = 0; i < 5; i++) {
    ids[i] = sched.submit(
        "fast_success_" + std::to_string(i),
        []() -> JobResult {
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
          return JobResult::Success();
        },
        Priority::Normal
    );
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
        Priority::Normal,
        RetryPolicy{2, BackoffType::Fixed, std::chrono::milliseconds(100)}
    );
  }

  ids[6] = sched.submit(
      "always_fails",
      []() -> JobResult { return JobResult::Failure("always fails"); },
      Priority::Normal,
      RetryPolicy{2, BackoffType::Exponential, std::chrono::milliseconds(200)}
  );

  ids[7] = sched.submit(
      "long_timeout",
      []() -> JobResult {
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        return JobResult::Success();
      },
      Priority::Normal,
      RetryPolicy{0},
      TimeoutPolicy{std::chrono::milliseconds(150)}
  );

  // Wait until jobs are done
    sched.wait_all();

  // JSON summaries
  auto log_snapshot = [&](const char* group, JobId id) {
    auto s = sched.snapshot(id);

    sched.logger().info(id, "final_snapshot", {
      {"event", "final_snapshot"},
      {"group", group},
      {"name", s.name},
      {"state", state_to_string(s.state)},
      {"attempts", std::to_string(s.attempts)},
      {"last_ok", (s.last_result.ok ? "true" : "false")},
      {"last_err", s.last_result.error},
      {"last_ms", std::to_string(s.last_duration.count())}
    });
  };

  // Priority / cancel summary
  log_snapshot("priority_cancel", high_ping);
  log_snapshot("priority_cancel", cancel_me);

  // Original summary jobs
  for (JobId id : ids) {
    log_snapshot("original", id);
  }

  // Metrics summary (JSON)
  auto st = sched.stats();

  double avg = 0.0;
  if (st.completed_jobs > 0) {
    avg = static_cast<double>(st.total_runtime_ms) /
          static_cast<double>(st.completed_jobs);
  }

  sched.logger().info("final_metrics", {
    {"event", "final_metrics"},
    {"jobs_submitted", std::to_string(st.jobs_submitted)},
    {"jobs_succeeded", std::to_string(st.jobs_succeeded)},
    {"jobs_failed", std::to_string(st.jobs_failed)},
    {"retries_performed", std::to_string(st.retries_performed)},
    {"timeouts_recorded", std::to_string(st.timeouts_recorded)},
    {"avg_runtime_ms", std::to_string(avg)}
  });

  return 0;
}
