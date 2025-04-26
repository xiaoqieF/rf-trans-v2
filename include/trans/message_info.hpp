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
    MessageInfo(const std::string& topic, const std::string& msg_type)
        : topic_(topic), msg_type_(msg_type) {}
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

struct RemoteMsg
{
    std::string topic;
    std::string sender;
    std::string data;
    std::string msg_type;
};

struct RemoteRequest
{
    std::string topic;
    std::string sender;
    std::string node_uuid;
    std::string req_uuid;
    std::string req_data;
    std::string dst_id;
    std::string req_type;
    std::string rep_type;
};

struct RemoteResponse
{
    std::string topic;
    std::string node_uuid;
    std::string req_uuid;
    std::string rep_data;
    std::string result_str;
};

} // namespace trans
} // namespace rf