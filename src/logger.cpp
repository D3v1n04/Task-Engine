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

void Logger::set_json(bool on) {
  json_.store(on, std::memory_order_relaxed);
}

bool Logger::json_enabled() const {
  return json_.load(std::memory_order_relaxed);
}

void Logger::debug(const std::string& msg) { log_(LogLevel::Debug, 0, msg, nullptr); }
void Logger::info (const std::string& msg) { log_(LogLevel::Info,  0, msg, nullptr); }
void Logger::warn (const std::string& msg) { log_(LogLevel::Warn,  0, msg, nullptr); }
void Logger::error(const std::string& msg) { log_(LogLevel::Error, 0, msg, nullptr); }

void Logger::debug(JobId job_id, const std::string& msg) { log_(LogLevel::Debug, job_id, msg, nullptr); }
void Logger::info (JobId job_id, const std::string& msg) { log_(LogLevel::Info,  job_id, msg, nullptr); }
void Logger::warn (JobId job_id, const std::string& msg) { log_(LogLevel::Warn,  job_id, msg, nullptr); }
void Logger::error(JobId job_id, const std::string& msg) { log_(LogLevel::Error, job_id, msg, nullptr); }

// Structured fields
void Logger::debug(const std::string& msg, Fields fields) { log_(LogLevel::Debug, 0, msg, &fields); }
void Logger::info (const std::string& msg, Fields fields) { log_(LogLevel::Info,  0, msg, &fields); }
void Logger::warn (const std::string& msg, Fields fields) { log_(LogLevel::Warn,  0, msg, &fields); }
void Logger::error(const std::string& msg, Fields fields) { log_(LogLevel::Error, 0, msg, &fields); }

void Logger::debug(JobId job_id, const std::string& msg, Fields fields) { log_(LogLevel::Debug, job_id, msg, &fields); }
void Logger::info (JobId job_id, const std::string& msg, Fields fields) { log_(LogLevel::Info,  job_id, msg, &fields); }
void Logger::warn (JobId job_id, const std::string& msg, Fields fields) { log_(LogLevel::Warn,  job_id, msg, &fields); }
void Logger::error(JobId job_id, const std::string& msg, Fields fields) { log_(LogLevel::Error, job_id, msg, &fields); }

const char* Logger::level_to_cstr(LogLevel lvl) {
  switch (lvl) {
    case LogLevel::Debug: return "DEBUG";
    case LogLevel::Info: return "INFO";
    case LogLevel::Warn: return "WARN";
    case LogLevel::Error: return "ERROR";
    default:  return "INFO";
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

std::string Logger::json_escape(const std::string& s) {
  std::string out;
  out.reserve(s.size() + 16);

  for (unsigned char c : s) {
    switch (c) {
      case '\"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\b': out += "\\b"; break;
      case '\f': out += "\\f"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (c < 0x20) {
          static const char* hex = "0123456789abcdef";
          out += "\\u00";
          out += hex[(c >> 4) & 0xF];
          out += hex[c & 0xF];
        } else {
          out += static_cast<char>(c);
        }
    }
  }

  return out;
}

void Logger::log_(LogLevel lvl, JobId job_id, const std::string& msg, const Fields* fields) {
  std::lock_guard<std::mutex> lock(mtx_);
  if (static_cast<int>(lvl) < static_cast<int>(min_level_)) return;

  const std::string ts = now_timestamp();
  const char* level = level_to_cstr(lvl);

  std::ostringstream tid_ss;
  tid_ss << std::this_thread::get_id();
  const std::string tid_str = tid_ss.str();

  const std::uint64_t seq = seq_.fetch_add(1, std::memory_order_relaxed);

  if (json_enabled()) {
    (*out_) << "{"
            << "\"ts\":\"" << json_escape(ts) << "\","
            << "\"seq\":" << seq << ","
            << "\"level\":\"" << json_escape(level) << "\","
            << "\"tid\":\"" << json_escape(tid_str) << "\"";

    if (job_id != 0) {
      (*out_) << ",\"job\":" << job_id;
    }

    if (fields) {
      for (const auto& kv : *fields) {
        (*out_) << ",\"" << json_escape(kv.first) << "\":\"" << json_escape(kv.second) << "\"";
      }
    }

    (*out_) << ",\"msg\":\"" << json_escape(msg) << "\"}\n";
    out_->flush();
    return;
  }

  // Format
  (*out_) << "[" << ts << "] "
          << "[" << level << "] "
          << "[tid=" << tid_str << "] ";

  if (job_id != 0) {
    (*out_) << "[job=" << job_id << "] ";
  }

  (*out_) << msg;

  if (fields && !fields->empty()) {
    (*out_) << " {";
    bool first = true;
    for (const auto& kv : *fields) {
      if (!first) (*out_) << ", ";
      first = false;
      (*out_) << kv.first << "=" << kv.second;
    }
    (*out_) << "}";
  }

  (*out_) << "\n";
  out_->flush();
}

} // namespace task_engine
