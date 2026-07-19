#pragma once

#include <string>

#include "msgs/discovery.pb.h"
#include "trans/advertise_options.hpp"

namespace rf
{
namespace trans
{

class PublisherInfo
{
public:
    PublisherInfo() = default;
    PublisherInfo(const std::string& topic,
              const std::string& addr,
              const std::string& process_uuid,
              const std::string& node_uuid,
              const AdvertiseOptions& opts);
    virtual ~PublisherInfo() = default;

    std::string getTopic() const { return topic_; }
    void setTopic(const std::string topic) { topic_ = topic; }

    std::string getAddr() const { return addr_; }
    void setAddr(const std::string& addr) { addr_ = addr; }

    std::string getProcessUuid() const { return process_uuid_;}
    void setProcessUuid(const std::string& p_uuid) { process_uuid_ = p_uuid; }

    std::string getNodeUuid() const { return node_uuid_; }
    void setNodeUuid(const std::string& n_uuid) { node_uuid_ = n_uuid; }

    virtual const AdvertiseOptions& getOptions() const { return opts_; }
    void setOptions(const AdvertiseOptions& opts) { opts_ = opts; }

    bool operator==(const PublisherInfo& rhs) const;
    bool operator!=(const PublisherInfo& rhs) const { return !(*this == rhs); }
    friend std::ostream& operator<<(std::ostream& out, const PublisherInfo& msg);

    virtual void fillDiscovery(msgs::Discovery& msg) const;
    virtual void setFromDiscovery(const msgs::Discovery& msg);

protected:
    std::string topic_;
    std::string addr_;        // ZeroMQ address of the publisher
    std::string process_uuid_;
    std::string node_uuid_;

private:
    AdvertiseOptions opts_;   // This member is not used when we have a derived publisher
};

class MessagePublisherInfo : public PublisherInfo
{
public:
    MessagePublisherInfo() = default;
    MessagePublisherInfo(const std::string& topic,
                         const std::string& addr,
                         const std::string& ctrl,
                         const std::string& process_uuid,
                         const std::string& node_uuid,
                         const std::string& msg_type,
                         const AdvertiseMessageOptions& opts);
    virtual ~MessagePublisherInfo() = default;

    std::string getCtrl() const { return ctrl_; }
    void setCtrl(const std::string& ctrl) { ctrl_ = ctrl; }

    std::string getMsgType() const { return msg_type_; }
    void setMsgType(const std::string& msg_type) { msg_type_ = msg_type; }

    const AdvertiseMessageOptions& getOptions() const override { return msg_opts_; }
    void setOptions(const AdvertiseMessageOptions& opts) { msg_opts_ = opts; }

    bool operator==(const MessagePublisherInfo& pub) const;
    bool operator!=(const MessagePublisherInfo& pub) const { return !(*this == pub); }

    void fillDiscovery(msgs::Discovery& msg) const override final;
    void setFromDiscovery(const msgs::Discovery& msg) override final;

    friend std::ostream& operator<<(std::ostream& out, const MessagePublisherInfo& msg);

private:
    std::string ctrl_;
    std::string msg_type_;
    AdvertiseMessageOptions msg_opts_;
};

class ServicePublisherInfo : public PublisherInfo
{
public:
    ServicePublisherInfo() = default;
    ServicePublisherInfo(const std::string& topic,
                     const std::string& addr,
                     const std::string& socket_id,
                     const std::string& process_uuid,
                     const std::string& node_uuid,
                     const std::string& req_type,
                     const std::string& rep_type,
                     const AdvertiseServiceOptions& opts);
    virtual ~ServicePublisherInfo() = default;

    std::string getSocketId() const { return socket_id_; }
    void setSocketId(const std::string& sock_id) { socket_id_ = sock_id; }

    std::string getReqTypeName() const { return req_type_name_; }
    void setReqTypeName(const std::string& req_type) { req_type_name_ = req_type; }

    std::string getRepTypeName() const { return rep_type_name_; }
    void setRepTypeName(const std::string& rep_type) { rep_type_name_ = rep_type; }

    const AdvertiseServiceOptions& getOptions() const override { return srv_opts_; }
    void setOptions(const AdvertiseServiceOptions& opts) { srv_opts_ = opts; }

    bool operator==(const ServicePublisherInfo& rhs) const;
    bool operator!=(const ServicePublisherInfo& rhs) const { return !(*this == rhs); }

    void fillDiscovery(msgs::Discovery& msg) const override final;
    void setFromDiscovery(const msgs::Discovery& msg) override final;

    friend std::ostream& operator<<(std::ostream& out, const ServicePublisherInfo& srv);

private:
    std::string socket_id_;
    std::string req_type_name_;
    std::string rep_type_name_;
    AdvertiseServiceOptions srv_opts_;
};

} // namespace trans
} // namespace rf
