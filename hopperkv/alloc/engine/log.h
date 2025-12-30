#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

namespace hare::log {

inline void config_logger(std::string logger_name, std::string log_filename,
                          std::string log_level) {
  if (!log_filename.empty()) {
    auto file_logger = spdlog::basic_logger_mt(logger_name, log_filename);
    // Set the file logger as the default global logger
    spdlog::set_default_logger(file_logger);
  }

  if (!log_level.empty()) {
    auto logger = spdlog::default_logger();
    if (log_level == "trace") {
      logger->set_level(spdlog::level::trace);
    } else if (log_level == "debug") {
      logger->set_level(spdlog::level::debug);
    } else if (log_level == "info") {
      logger->set_level(spdlog::level::info);
    } else if (log_level == "warn") {
      logger->set_level(spdlog::level::warn);
    } else if (log_level == "error") {
      logger->set_level(spdlog::level::err);
    } else if (log_level == "critical") {
      logger->set_level(spdlog::level::critical);
    } else {
      throw std::invalid_argument("Invalid log level: " + log_level);
    }
  }
}

} // namespace hare::log
