#pragma once
#include <string>

namespace task_engine {

struct JobResult {
  bool ok = true;
  std::string error;

  static JobResult Success() { return JobResult{true, ""}; }
  static JobResult Failure(std::string msg) { return JobResult{false, std::move(msg)}; }
};

} // namespace task_engine
