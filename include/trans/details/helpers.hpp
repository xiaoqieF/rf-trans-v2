#pragma once

#include <cstdlib>
#include <string>
#include <limits>
#include <random>
#include <sstream>
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

inline std::string generateUuidV4() {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);
    std::uniform_int_distribution<> dis2(8, 11); // for variant

    std::stringstream ss;
    ss << std::hex;

    for (int i = 0; i < 8; ++i) ss << dis(gen);
    ss << "-";
    for (int i = 0; i < 4; ++i) ss << dis(gen);
    ss << "-4"; // version 4
    for (int i = 0; i < 3; ++i) ss << dis(gen);
    ss << "-";
    ss << dis2(gen); // variant
    for (int i = 0; i < 3; ++i) ss << dis(gen);
    ss << "-";
    for (int i = 0; i < 12; ++i) ss << dis(gen);

    return ss.str();
}

} // namespace trans
} // namespace rf