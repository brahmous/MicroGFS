#include "./logger.h"
#include <memory>
#include "spdlog/sinks/stdout_color_sinks.h"

namespace GFSLogger {

std::shared_ptr<spdlog::logger> Logger::s_MainLogger;

void Logger::init() {
	spdlog::set_pattern("%^[%T] %n: %v%$");
	s_MainLogger = spdlog::stdout_color_mt("GFS MAIN");
}

} // namespace GFSLogger
