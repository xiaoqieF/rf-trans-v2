#include <gtest/gtest.h>

#include "trans/advertise_options.hpp"
#include "trans/publisher_info.hpp"
#include "msgs/example.pb.h"

using rf::trans::MessagePublisherInfo;
using rf::trans::ServicePublisherInfo;


TEST(publisher_info, construct)
{
    MessagePublisherInfo pub1("topic1", "tcp://localhost:5555", "unused", "pid1", "node1",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});

    EXPECT_EQ(pub1.getTopic(), "topic1");
    EXPECT_EQ(pub1.getAddr(), "tcp://localhost:5555");
    EXPECT_EQ(pub1.getProcessUuid(), "pid1");
    EXPECT_EQ(pub1.getNodeUuid(), "node1");
    EXPECT_EQ(pub1.getMsgType(), rf::msgs::ExampleMsg::descriptor()->full_name());
    EXPECT_EQ(pub1.getOptions().getScope(), rf::trans::Scope::ALL);
}

TEST(publisher_info, discovery_fill_set)
{
    MessagePublisherInfo pub1("topic1", "tcp://localhost:5555", "unused", "pid1", "node1",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});
    rf::msgs::Discovery discovery_msg;
    pub1.fillDiscovery(discovery_msg);
    EXPECT_EQ(discovery_msg.pub().topic(), "topic1");
    EXPECT_EQ(discovery_msg.pub().address(), "tcp://localhost:5555");
    EXPECT_EQ(discovery_msg.pub().node_uuid(), "node1");
    EXPECT_EQ(discovery_msg.pub().msg_pub().msg_type(), rf::msgs::ExampleMsg::descriptor()->full_name());

    discovery_msg.set_type(rf::msgs::Discovery::ADVERTISE);
    discovery_msg.set_process_uuid("pid1");

    MessagePublisherInfo pub2;
    pub2.setFromDiscovery(discovery_msg);
    EXPECT_EQ(pub2.getTopic(), "topic1");
    EXPECT_EQ(pub2.getAddr(), "tcp://localhost:5555");
    EXPECT_EQ(pub2.getProcessUuid(), "pid1");
    EXPECT_EQ(pub2.getNodeUuid(), "node1");
    EXPECT_EQ(pub2.getMsgType(), rf::msgs::ExampleMsg::descriptor()->full_name());
    EXPECT_EQ(pub2.getOptions().getScope(), rf::trans::Scope::ALL);
}

TEST(publisher_info, equality)
{
    MessagePublisherInfo pub1("topic1", "tcp://localhost:5555", "unused", "pid1", "node1",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});
    MessagePublisherInfo pub2("topic1", "tcp://localhost:5555", "unused", "pid1", "node1",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});

    EXPECT_TRUE(pub1 == pub2);
    EXPECT_FALSE(pub1 != pub2);
    pub2.setNodeUuid("node2");
    EXPECT_FALSE(pub1 == pub2);
    EXPECT_TRUE(pub1 != pub2);
}

TEST(service_publisher_info, construct)
{
    ServicePublisherInfo srv1("topic1", "tcp://localhost:5555", "socket_id_123", "pid1", "node1",
        "RequestType", "ResponseType", rf::trans::AdvertiseServiceOptions{});
    EXPECT_EQ(srv1.getTopic(), "topic1");
    EXPECT_EQ(srv1.getAddr(), "tcp://localhost:5555");
    EXPECT_EQ(srv1.getProcessUuid(), "pid1");
    EXPECT_EQ(srv1.getNodeUuid(), "node1");
    EXPECT_EQ(srv1.getSocketId(), "socket_id_123");
    EXPECT_EQ(srv1.getReqTypeName(), "RequestType");
    EXPECT_EQ(srv1.getRepTypeName(), "ResponseType");
    EXPECT_EQ(srv1.getOptions().getScope(), rf::trans::Scope::ALL);
}

TEST(service_publisher_info, discovery_fill_set)
{
    ServicePublisherInfo srv1("topic1", "tcp://localhost:5555", "socket_id_123", "pid1", "node1",
        "RequestType", "ResponseType", rf::trans::AdvertiseServiceOptions{});
    rf::msgs::Discovery discovery_msg;
    srv1.fillDiscovery(discovery_msg);
    EXPECT_EQ(discovery_msg.pub().topic(), "topic1");
    EXPECT_EQ(discovery_msg.pub().address(), "tcp://localhost:5555");
    EXPECT_EQ(discovery_msg.pub().node_uuid(), "node1");
    EXPECT_EQ(discovery_msg.pub().srv_pub().socket_id(), "socket_id_123");
    EXPECT_EQ(discovery_msg.pub().srv_pub().request_type(), "RequestType");
    EXPECT_EQ(discovery_msg.pub().srv_pub().response_type(), "ResponseType");

    discovery_msg.set_type(rf::msgs::Discovery::ADVERTISE);
    discovery_msg.set_process_uuid("pid1");
    ServicePublisherInfo srv2;
    srv2.setFromDiscovery(discovery_msg);
    EXPECT_EQ(srv2.getTopic(), "topic1");
    EXPECT_EQ(srv2.getAddr(), "tcp://localhost:5555");
    EXPECT_EQ(srv2.getProcessUuid(), "pid1");
    EXPECT_EQ(srv2.getNodeUuid(), "node1");
    EXPECT_EQ(srv2.getSocketId(), "socket_id_123");
    EXPECT_EQ(srv2.getReqTypeName(), "RequestType");
    EXPECT_EQ(srv2.getRepTypeName(), "ResponseType");
    EXPECT_EQ(srv2.getOptions().getScope(), rf::trans::Scope::ALL);
}

TEST(service_publisher_info, equality)
{
    ServicePublisherInfo srv1("topic1", "tcp://localhost:5555", "socket_id_123", "pid1", "node1",
        "RequestType", "ResponseType", rf::trans::AdvertiseServiceOptions{});
    ServicePublisherInfo srv2("topic1", "tcp://localhost:5555", "socket_id_123", "pid1", "node1",
        "RequestType", "ResponseType", rf::trans::AdvertiseServiceOptions{});
    EXPECT_TRUE(srv1 == srv2);
    EXPECT_FALSE(srv1 != srv2);
    srv2.setNodeUuid("node2");
    EXPECT_FALSE(srv1 == srv2);
    EXPECT_TRUE(srv1 != srv2);

    srv2.setNodeUuid("node1");
    srv2.setSocketId("socket_id_456");
    EXPECT_FALSE(srv1 == srv2);
    EXPECT_TRUE(srv1 != srv2);

    srv2.setSocketId("socket_id_123");
    srv2.setReqTypeName("NewRequestType");
    EXPECT_FALSE(srv1 == srv2);
    EXPECT_TRUE(srv1 != srv2);

    srv2.setReqTypeName("RequestType");
    srv2.setRepTypeName("NewResponseType");
    EXPECT_FALSE(srv1 == srv2);
    EXPECT_TRUE(srv1 != srv2);
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}