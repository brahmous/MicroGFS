#ifndef GFS_LOGGER_H
#define GFS_LOGGER_H

#include <memory>
#include <spdlog/logger.h>
#include <spdlog/spdlog.h>

namespace GFSLogger {

class Logger {
	public:
	static void init();

	inline static std::shared_ptr<spdlog::logger>& GetMainLogger() {return s_MainLogger;};

	private:
	static std::shared_ptr<spdlog::logger> s_MainLogger;
};
}
#define MAINLOG_TRACE(...) 	::GFSLogger::Logger::GetMainLogger()->trace(__VA_ARGS__);
#define MAINLOG_INFO(...) 	::GFSLogger::Logger::GetMainLogger()->info(__VA_ARGS__);
#define MAINLOG_WARN(...) 	::GFSLogger::Logger::GetMainLogger()->warn(__VA_ARGS__);
#define MAINLOG_ERROR(...) 	::GFSLogger::Logger::GetMainLogger()->error(__VA_ARGS__);
#define MAINLOG_FATAl(...) 	::GFSLogger::Logger::GetMainLogger()->fatal(__VA_ARGS__);
#endif
