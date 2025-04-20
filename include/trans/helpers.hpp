#pragma once

#include <cstdlib>
#include <string>
#include <limits>
#include <unistd.h>

namespace rf
{
namespace trans
{
inline constexpr uint64_t kUnthrottled = std::numeric_limits<uint64_t>::max();

inline bool getEnv(const std::string& name, std::string& value)
{
    char* v = std::getenv(name.c_str());
    if (v) {
        value = v;
        return true;
    }
    return false;
}

inline unsigned int getProcessId()
{
    return ::getpid();
}

} // namespace trans
} // namespace rf