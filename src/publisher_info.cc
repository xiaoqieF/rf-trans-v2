#include "trans/details/publisher_info.hpp"

namespace rf
{
namespace trans
{
PublisherInfo::PublisherInfo(const std::string& topic,
                             const std::string& addr,
                             const std::string& process_uuid,
                             const std::string& node_uuid,
                             const AdvertiseOptions& opts)
    : topic_(topic),
      addr_(addr),
      process_uuid_(process_uuid),
      node_uuid_(node_uuid),
      opts_(opts)
{
}

bool PublisherInfo::operator==(const PublisherInfo& rhs) const
{
    return topic_ == rhs.topic_ && addr_ == rhs.addr_ &&
           process_uuid_ == rhs.process_uuid_ && node_uuid_ == rhs.node_uuid_ &&
           opts_ == rhs.opts_;
}

std::ostream& operator<<(std::ostream& out, const PublisherInfo& msg)
{
    out << "Publisher:\n"
        << "\tTopic: [" << msg.topic_ << "]\n"
        << "\tAddress: " << msg.addr_ << "\n"
        << "\tProcess UUID: " << msg.process_uuid_ << "\n"
        << "\tNode UUID: " << msg.node_uuid_ << "\n"
        << msg.opts_;
    return out;
}

void PublisherInfo::fillDiscovery(msgs::Discovery& msg) const
{
    msgs::Discovery::Publisher* pub = msg.mutable_pub();
    pub->set_topic(topic_);
    pub->set_address(addr_);
    pub->set_node_uuid(node_uuid_);

    switch (opts_.getScope()) {
        case Scope::PROCESS:
            pub->set_scope(msgs::Discovery::Publisher::PROCESS);
            break;
        case Scope::HOST:
            pub->set_scope(msgs::Discovery::Publisher::HOST);
            break;
        case Scope::ALL:
            pub->set_scope(msgs::Discovery::Publisher::ALL);
            break;
        default:
            pub->set_scope(msgs::Discovery::Publisher::ALL);
            break;
    }
}

void PublisherInfo::setFromDiscovery(const msgs::Discovery& msg)
{
    if (msg.has_sub()) {
        topic_ = msg.sub().topic();
    } else if (msg.has_pub()) {
        topic_ = msg.pub().topic();
        addr_ = msg.pub().address();
        process_uuid_ = msg.process_uuid();
        process_name_ = msg.process_name();
        node_uuid_ = msg.pub().node_uuid();

        switch (msg.pub().scope()) {
            case msgs::Discovery::Publisher::PROCESS:
                opts_.setScope(Scope::PROCESS);
                break;
            case msgs::Discovery::Publisher::HOST:
                opts_.setScope(Scope::HOST);
                break;
            case msgs::Discovery::Publisher::ALL:
                opts_.setScope(Scope::ALL);
                break;
            default:
                opts_.setScope(Scope::ALL);
        }
    }
}

MessagePublisherInfo::MessagePublisherInfo(const std::string& topic,
                     const std::string& addr,
                     const std::string& ctrl,
                     const std::string& process_uuid,
                     const std::string& node_uuid,
                     const std::string& msg_type,
                     const AdvertiseMessageOptions& opts)
    : PublisherInfo(topic, addr, process_uuid, node_uuid, opts),
      ctrl_(ctrl),
      msg_type_(msg_type),
      msg_opts_(opts)
{
}

bool MessagePublisherInfo::operator==(const MessagePublisherInfo& pub) const
{
    return PublisherInfo::operator==(pub) && ctrl_ == pub.ctrl_ &&
        msg_type_ == pub.msg_type_ && msg_opts_ == pub.msg_opts_;
}

void MessagePublisherInfo::fillDiscovery(msgs::Discovery& msg) const
{
    PublisherInfo::fillDiscovery(msg);
    msgs::Discovery::Publisher* pub = msg.mutable_pub();

    pub->mutable_msg_pub()->set_ctrl(ctrl_);
    pub->mutable_msg_pub()->set_msg_type(msg_type_);
    pub->mutable_msg_pub()->set_throttled(msg_opts_.throttled());
    pub->mutable_msg_pub()->set_msgs_per_sec(msg_opts_.getMsgsPerSec());
}
void MessagePublisherInfo::setFromDiscovery(const msgs::Discovery& msg)
{
    PublisherInfo::setFromDiscovery(msg);
    ctrl_ = msg.pub().msg_pub().ctrl();
    msg_type_ = msg.pub().msg_pub().msg_type();
    msg_opts_.setScope(PublisherInfo::getOptions().getScope());
    if (!msg.pub().msg_pub().throttled()) {
        msg_opts_.setMsgsPerSec(kUnthrottled);
    } else {
        msg_opts_.setMsgsPerSec(msg.pub().msg_pub().msgs_per_sec());
    }
}

std::ostream& operator<<(std::ostream& out, const MessagePublisherInfo& msg)
{
    out << "Publisher:\n"
        << "\tTopic: [" << msg.topic_ << "]\n"
        << "\tAddress: " << msg.addr_ << "\n"
        << "\tProcess UUID: " << msg.process_uuid_ << "\n"
        << "\tNode UUID: " << msg.node_uuid_ << "\n"
        << "\tControl addr: " << msg.ctrl_ << "\n"
        << "\tMessage type: " << msg.msg_type_ << "\n"
        << msg.msg_opts_;
    return out;
}

ServicePublisherInfo::ServicePublisherInfo(const std::string& topic,
                    const std::string& addr,
                    const std::string& socket_id,
                    const std::string& process_uuid,
                    const std::string& node_uuid,
                    const std::string& req_type,
                    const std::string& rep_type,
                    const AdvertiseServiceOptions& opts)
    : PublisherInfo(topic, addr, process_uuid, node_uuid, opts),
      socket_id_(socket_id),
      req_type_name_(req_type),
      rep_type_name_(rep_type),
      srv_opts_(opts)
{
}

bool ServicePublisherInfo::operator==(const ServicePublisherInfo& rhs) const
{
    return PublisherInfo::operator==(rhs) && socket_id_ == rhs.socket_id_ &&
        req_type_name_ == rhs.req_type_name_ && rep_type_name_ == rhs.rep_type_name_;
}

void ServicePublisherInfo::fillDiscovery(msgs::Discovery& msg) const
{
    PublisherInfo::fillDiscovery(msg);
    msgs::Discovery::Publisher* pub = msg.mutable_pub();

    pub->mutable_srv_pub()->set_socket_id(socket_id_);
    pub->mutable_srv_pub()->set_request_type(req_type_name_);
    pub->mutable_srv_pub()->set_response_type(rep_type_name_);
}

void ServicePublisherInfo::setFromDiscovery(const msgs::Discovery& msg)
{
    PublisherInfo::setFromDiscovery(msg);
    srv_opts_.setScope(PublisherInfo::getOptions().getScope());
    socket_id_ = msg.pub().srv_pub().socket_id();
    req_type_name_ = msg.pub().srv_pub().request_type();
    rep_type_name_ = msg.pub().srv_pub().response_type();
}

std::ostream& operator<<(std::ostream& out, const ServicePublisherInfo& msg)
{
    out << "Publisher:\n"
        << "\tTopic: [" << msg.topic_ << "]\n"
        << "\tAddress: " << msg.addr_ << "\n"
        << "\tProcess UUID: " << msg.process_uuid_ << "\n"
        << "\tNode UUID: " << msg.node_uuid_ << "\n"
        << "\tSocket ID: " << msg.socket_id_ << "\n"
        << "\t Request type: " << msg.req_type_name_ << "\n"
        << "\t Response type: " << msg.rep_type_name_ << "\n"
        << msg.srv_opts_;
    return out;
}

} // namespace trans
} // namespace rf
