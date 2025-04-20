#pragma once

#include "msgs/discovery.pb.h"

namespace rf
{
namespace msgs
{
inline std::string toString(const msgs::Discovery::Type& t) {
    switch (t)
    {
      default:
      case msgs::Discovery::UNINITIALIZED:
        return "UNINITIALIZED";
      case msgs::Discovery::ADVERTISE:
        return "ADVERTISE";
      case msgs::Discovery::SUBSCRIBE:
        return "SUBSCRIBE";
      case msgs::Discovery::UNADVERTISE:
        return "UNADVERTISE";
      case msgs::Discovery::HEARTBEAT:
        return "HEARTBEAT";
      case msgs::Discovery::BYE:
        return "BYE";
      case msgs::Discovery::NEW_CONNECTION:
        return "NEW_CONNECTION";
      case msgs::Discovery::END_CONNECTION:
        return "END_CONNECTION";
      case msgs::Discovery::SUBSCRIBERS_REQ:
        return "SUBSCRIBERS_REQ";
      case msgs::Discovery::SUBSCRIBERS_REP:
        return "SUBSCRIBERS_REP";
    };
    return "UNKNOWN";
}

} // namespace msgs
} // namespace rf