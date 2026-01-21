#pragma once
#include <cstdint>

namespace task_engine {

using JobId = std::uint64_t;

enum class JobState {
  Pending,
  Running,
  Success,
  Failed,
  Retrying,
  Cancelled,
  TimedOut
};

} // namespace task_engine