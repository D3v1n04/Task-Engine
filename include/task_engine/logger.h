#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "task_engine/types.h"

namespace task_engine {

enum class LogLevel { Debug, Info, Warn, Error };

class Logger {
public:
  using Field  = std::pair<std::string, std::string>;
  using Fields = std::vector<Field>;

  Logger();
  explicit Logger(std::ostream& out);

  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

  void set_level(LogLevel lvl);

  void set_json(bool on);
  bool json_enabled() const;

  void debug(const std::string& msg);
  void info (const std::string& msg);
  void warn (const std::string& msg);
  void error(const std::string& msg);

  void debug(JobId job_id, const std::string& msg);
  void info (JobId job_id, const std::string& msg);
  void warn (JobId job_id, const std::string& msg);
  void error(JobId job_id, const std::string& msg);

  // Structured fields
  void debug(const std::string& msg, Fields fields);
  void info (const std::string& msg, Fields fields);
  void warn (const std::string& msg, Fields fields);
  void error(const std::string& msg, Fields fields);

  void debug(JobId job_id, const std::string& msg, Fields fields);
  void info (JobId job_id, const std::string& msg, Fields fields);
  void warn (JobId job_id, const std::string& msg, Fields fields);
  void error(JobId job_id, const std::string& msg, Fields fields);

private:
  void log_(LogLevel lvl, JobId job_id, const std::string& msg, const Fields* fields);

  static const char* level_to_cstr(LogLevel lvl);
  static std::string now_timestamp();
  static std::string json_escape(const std::string& s);

private:
  std::ostream* out_{nullptr};
  LogLevel min_level_{LogLevel::Info};

  std::atomic<bool> json_{false};
  std::atomic<std::uint64_t> seq_{0};

  std::mutex mtx_;
};

} // namespace task_engine
