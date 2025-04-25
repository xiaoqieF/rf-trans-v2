#pragma once

#include <string>

namespace rf
{
namespace trans
{
class MessageInfo
{
public:
    MessageInfo() = default;
    MessageInfo(const MessageInfo&) = default;
    MessageInfo(MessageInfo&&) = default;
    ~MessageInfo() = default;

    std::string getTopic() const { return topic_; }
    std::string getMsgType() const { return msg_type_; }
    void setTopic(const std::string t) { topic_ = t; }
    void setMsgType(const std::string msg_type) { msg_type_ = msg_type; }

private:
    std::string topic_;
    std::string msg_type_;
};

} // namespace trans
} // namespace rf