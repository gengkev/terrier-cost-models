#pragma once

#include <memory>

#include "loggers/loggers_util.h"

namespace terrier::storage {
extern std::shared_ptr<spdlog::logger> storage_logger;  // NOLINT

void InitStorageLogger();
}  // namespace terrier::storage

#define STORAGE_LOG_TRACE(...) ::terrier::storage::storage_logger->trace(__VA_ARGS__);

#define STORAGE_LOG_DEBUG(...) ::terrier::storage::storage_logger->debug(__VA_ARGS__);

#define STORAGE_LOG_INFO(...) ::terrier::storage::storage_logger->info(__VA_ARGS__);

#define STORAGE_LOG_WARN(...) ::terrier::storage::storage_logger->warn(__VA_ARGS__);

#define STORAGE_LOG_ERROR(...) ::terrier::storage::storage_logger->error(__VA_ARGS__);
