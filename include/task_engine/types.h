#pragma once
#include <cstdint>

namespace task_engine {

using JobId = std::uint64_t;

enum class Priority : std::uint8_t {
  High = 0,
  Normal = 1,
  Low = 2
};

enum class JobState {
  Pending,
  Running,
  Success,
  Failed,
  Retrying,
  CancelRequested,
  Cancelled,
  TimedOut
};

} // namespace task_engine
