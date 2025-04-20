#pragma once

#include <string>
#include <string_view>
#include <functional>
#include <map>
#include <chrono>

namespace rf
{
namespace trans
{
constexpr inline std::string_view kGenericMessageType = "google.protobuf.Message";

using Timestamp = std::chrono::steady_clock::time_point;

template<typename Pub>
using DiscoveryCallback = std::function<void(const Pub&)>;

/// \brief Map that stores all generic publishers.
/// The keys are the process uuids of the nodes. For each uuid key, the
/// value contains the list of publishers advertising the topic within the
/// same process uuid.
template<typename Pub>
using AddressMap = std::map<std::string, std::vector<Pub>>;

} // namespace trans
} // namespace rf