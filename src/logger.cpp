#include "task_engine/logger.h"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>

namespace task_engine {

Logger::Logger() : out_(&std::cout) {}
Logger::Logger(std::ostream& out) : out_(&out) {}

void Logger::set_level(LogLevel lvl) {
  std::lock_guard<std::mutex> lock(mtx_);
  min_level_ = lvl;
}

void Logger::debug(const std::string& msg) { 
    log(LogLevel::Debug, msg); 
}
void Logger::info(const std::string& msg) { 
    log(LogLevel::Info, msg); 
}
void Logger::warn(const std::string& msg) { 
    log(LogLevel::Warn, msg); 
}
void Logger::error(const std::string& msg) { 
    log(LogLevel::Error, msg); 
}

void Logger::debug(JobId job_id, const std::string& msg) { 
    log(LogLevel::Debug, job_id, msg); 
}
void Logger::info(JobId job_id, const std::string& msg) { 
    log(LogLevel::Info, job_id, msg); 
}
void Logger::warn(JobId job_id, const std::string& msg) { 
    log(LogLevel::Warn, job_id, msg); 
}
void Logger::error(JobId job_id, const std::string& msg) { 
    log(LogLevel::Error, job_id, msg); 
}

const char* Logger::level_to_cstr(LogLevel lvl) {
  switch (lvl) {
    case LogLevel::Debug: return "DEBUG";
    case LogLevel::Info: return "INFO ";
    case LogLevel::Warn: return "WARN ";
    case LogLevel::Error: return "ERROR";
    default:  return "INFO ";
  }
}

std::string Logger::now_timestamp() {
  using namespace std::chrono;

  const auto now = system_clock::now();
  const auto t = system_clock::to_time_t(now);
  const auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;

  std::tm tm{};
#if defined(_WIN32)
  localtime_s(&tm, &t);
#else
  localtime_r(&t, &tm);
#endif

  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
  oss << "." << std::setw(3) << std::setfill('0') << ms.count();
  return oss.str();
}

void Logger::log(LogLevel lvl, const std::string& msg) {
  log(lvl, 0, msg);
}

void Logger::log(LogLevel lvl, JobId job_id, const std::string& msg) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (static_cast<int>(lvl) < static_cast<int>(min_level_)) return;

  // Single-line log output (avoids interleaving between threads)
  (*out_) << "[" << now_timestamp() << "] "
          << "[" << level_to_cstr(lvl) << "] "
          << "[tid=" << std::this_thread::get_id() << "] ";

  if (job_id != 0) {
    (*out_) << "[job=" << job_id << "] ";
  }

  (*out_) << msg << "\n";
  out_->flush();
}

} // namespace task_engine
