#pragma once

#include <string>
#include <string_view>
#include <functional>
#include <map>
#include <chrono>

#include "google/protobuf/message.h"
#include "trans/message_info.hpp"

namespace rf
{
namespace trans
{

class IReqHandler;
class IRepHandler;
class ISubscriptionHandler;

constexpr inline std::string_view kGenericMessageType = "google.protobuf.Message";
constexpr int kDefaultRcvHwm = 1000;
constexpr int kDefaultSndHwm = 1000;

using Timestamp = std::chrono::steady_clock::time_point;
using ProtoMsg = google::protobuf::Message;
using ProtoMsgPtr = std::shared_ptr<ProtoMsg>;
using ISubscriptionHandlerPtr = std::shared_ptr<ISubscriptionHandler>;
using IRepHandlerPtr = std::shared_ptr<IRepHandler>;
using IReqHandlerPtr = std::shared_ptr<IReqHandler>;

using DeallocFunc = void(void* data, void *hint);

template<typename Pub>
using DiscoveryCallback = std::function<void(const Pub&)>;

template<typename MsgType>
using MsgCallback = std::function<void(const MsgType&, const MessageInfo&)>;

/// \brief Map that stores all generic publishers.
/// The keys are the process uuids of the nodes. For each uuid key, the
/// value contains the list of publishers advertising the topic within the
/// same process uuid.
template<typename Pub>
using AddressMap = std::map<std::string, std::vector<Pub>>;

} // namespace trans
} // namespace rf