#pragma once

#include <cstdint>
#include <mutex>
#include <ostream>
#include <string>

#include "task_engine/types.h"

namespace task_engine {

enum class LogLevel {
  Debug,
  Info,
  Warn,
  Error
};

class Logger {
public:
  // By default logs to std::cout
  Logger();
  explicit Logger(std::ostream& out);

  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

  void set_level(LogLevel lvl);

  void debug(const std::string& msg);
  void info(const std::string& msg);
  void warn(const std::string& msg);
  void error(const std::string& msg);

  void debug(JobId job_id, const std::string& msg);
  void info(JobId job_id, const std::string& msg);
  void warn(JobId job_id, const std::string& msg);
  void error(JobId job_id, const std::string& msg);

private:
  void log(LogLevel lvl, const std::string& msg);
  void log(LogLevel lvl, JobId job_id, const std::string& msg);

  static const char* level_to_cstr(LogLevel lvl);
  static std::string now_timestamp();

private:
  std::ostream* out_;
  LogLevel min_level_{LogLevel::Info};
  std::mutex mtx_;
};

} // namespace task_engine
