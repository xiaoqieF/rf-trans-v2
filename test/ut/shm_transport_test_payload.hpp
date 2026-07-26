#pragma once

#include <cstddef>
#include <string>

namespace rf
{
namespace trans
{
namespace test
{

/// \brief Deterministic non-uniform payload of `size` bytes, used by the shm
/// transport test and its subscriber helper process to verify a large
/// message's content survives the shm round trip byte-for-byte.
inline std::string buildShmTestPayload(const size_t size)
{
    std::string payload(size, '\0');
    for (size_t i = 0; i < size; ++i) {
        payload[i] = static_cast<char>(static_cast<unsigned char>((i * 131 + 7) % 251));
    }
    return payload;
}

} // namespace test
} // namespace trans
} // namespace rf
