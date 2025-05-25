#include <gtest/gtest.h>

#include "trans/advertise_options.hpp"
#include "trans/topic_storage.hpp"
#include "trans/publisher_info.hpp"
#include "msgs/example.pb.h"


using rf::trans::MessagePublisherInfo;
using rf::trans::ServicePublisherInfo;
using MsgTopicStorage = rf::trans::TopicStorage<MessagePublisherInfo>;
using SrvTopicStorage = rf::trans::TopicStorage<ServicePublisherInfo>;

TEST(topic_storage, add_publisher)
{
    MsgTopicStorage storage;

    MessagePublisherInfo pub1("topic1", "tcp://localhost:5555", "unused", "pid1", "node1",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});

    MessagePublisherInfo pub2("topic1", "tcp://localhost:5555", "unused", "pid1", "node2",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});

    storage.addPublisher(pub1);
    storage.addPublisher(pub2);

    EXPECT_TRUE(storage.hasTopic("topic1"));
    EXPECT_TRUE(storage.hasTopic("topic1", rf::msgs::ExampleMsg::descriptor()->full_name()));
    EXPECT_TRUE(storage.hasPublisher("tcp://localhost:5555"));
    EXPECT_TRUE(storage.hasAnyPublishers("topic1", "pid1"));

    EXPECT_FALSE(storage.hasTopic("topic2"));
    EXPECT_FALSE(storage.hasTopic("topic2", rf::msgs::ExampleMsg::descriptor()->full_name()));
    EXPECT_FALSE(storage.hasPublisher("tcp://localhost:6666"));
    EXPECT_FALSE(storage.hasAnyPublishers("topic2", "pid1"));

    MessagePublisherInfo pub3("topic2", "tcp://localhost:6666", "unused", "pid2", "node3",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});

    storage.addPublisher(pub3);
    storage.print(std::cout);

    EXPECT_TRUE(storage.hasTopic("topic2"));
    EXPECT_TRUE(storage.hasTopic("topic2", rf::msgs::ExampleMsg::descriptor()->full_name()));
    EXPECT_TRUE(storage.hasPublisher("tcp://localhost:6666"));
    EXPECT_TRUE(storage.hasAnyPublishers("topic2", "pid2"));
    EXPECT_FALSE(storage.hasAnyPublishers("topic2", "pid1"));
    EXPECT_FALSE(storage.hasAnyPublishers("topic1", "pid2"));
}

TEST(topic_storage, get_publishers)
{
    MsgTopicStorage storage;

    MessagePublisherInfo pub1("topic1", "tcp://localhost:5555", "unused", "pid1", "node1",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});

    MessagePublisherInfo pub2("topic1", "tcp://localhost:5555", "unused", "pid1", "node2",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});

    MessagePublisherInfo pub3("topic2", "tcp://localhost:6666", "unused", "pid2", "node3",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});

    storage.addPublisher(pub1);
    storage.addPublisher(pub2);
    storage.addPublisher(pub3);

    EXPECT_EQ(storage.getTopicList().size(), 2);

    MessagePublisherInfo found_pub;
    EXPECT_TRUE(storage.getPublisher("topic1", "pid1", "node1", found_pub));
    EXPECT_EQ(found_pub.getAddr(), "tcp://localhost:5555");
    EXPECT_EQ(found_pub.getNodeUuid(), "node1");

    EXPECT_TRUE(storage.getPublisher("topic1", "pid1", "node2", found_pub));
    EXPECT_EQ(found_pub.getAddr(), "tcp://localhost:5555");
    EXPECT_EQ(found_pub.getNodeUuid(), "node2");

    EXPECT_FALSE(storage.getPublisher("topic1", "pid2", "node1", found_pub));
    EXPECT_FALSE(storage.getPublisher("topic2", "pid1", "node1", found_pub));

    EXPECT_TRUE(storage.getPublisher("topic2", "pid2", "node3", found_pub));
    EXPECT_EQ(found_pub.getAddr(), "tcp://localhost:6666");
    EXPECT_EQ(found_pub.getNodeUuid(), "node3");

    EXPECT_FALSE(storage.getPublisher("topic2", "pid1", "node3", found_pub));
    EXPECT_FALSE(storage.getPublisher("topic3", "pid1", "node1", found_pub));

    MessagePublisherInfo not_found_pub;
    EXPECT_FALSE(storage.getPublisher("topic3", "pid1", "node1", not_found_pub));
    EXPECT_FALSE(storage.getPublisher("topic1", "pid2", "node3", not_found_pub));
    EXPECT_FALSE(storage.getPublisher("topic2", "pid1", "node3", not_found_pub));
    EXPECT_FALSE(storage.getPublisher("topic2", "pid2", "node1", not_found_pub));

    MsgTopicStorage::ProcessMap info;
    EXPECT_TRUE(storage.getPublishers("topic1", info));
    EXPECT_EQ(info.size(), 1);
    EXPECT_EQ(info["pid1"].size(), 2);
    EXPECT_TRUE(storage.getPublishers("topic2", info));
    EXPECT_EQ(info.size(), 1);
    EXPECT_EQ(info["pid2"].size(), 1);
    EXPECT_EQ(info["pid2"][0].getNodeUuid(), "node3");
    EXPECT_FALSE(storage.getPublishers("topic3", info));
    storage.print(std::cout);

    storage.clear();
    EXPECT_TRUE(storage.getTopicList().empty());
    EXPECT_FALSE(storage.hasTopic("topic1"));
    EXPECT_FALSE(storage.hasTopic("topic2"));
    EXPECT_FALSE(storage.hasTopic("topic3"));
    EXPECT_FALSE(storage.hasPublisher("tcp://localhost:5555"));
    EXPECT_FALSE(storage.hasPublisher("tcp://localhost:6666"));
    EXPECT_FALSE(storage.hasAnyPublishers("topic1", "pid1"));
    EXPECT_FALSE(storage.hasAnyPublishers("topic2", "pid2"));
    EXPECT_FALSE(storage.hasAnyPublishers("topic3", "pid1"));
    EXPECT_FALSE(storage.hasAnyPublishers("topic1", "pid2"));
    EXPECT_FALSE(storage.hasAnyPublishers("topic2", "pid1"));
    EXPECT_FALSE(storage.hasAnyPublishers("topic3", "pid2"));
    EXPECT_TRUE(storage.getTopicList().empty());
}

TEST(topic_storage, del_publishers)
{
    MsgTopicStorage storage;

    MessagePublisherInfo pub1("topic1", "tcp://localhost:5555", "unused", "pid1", "node1",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});

    MessagePublisherInfo pub2("topic1", "tcp://localhost:5555", "unused", "pid1", "node2",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});

    MessagePublisherInfo pub3("topic2", "tcp://localhost:6666", "unused", "pid2", "node3",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});

    storage.addPublisher(pub1);
    storage.addPublisher(pub2);
    storage.addPublisher(pub3);

    EXPECT_TRUE(storage.hasTopic("topic1"));
    EXPECT_TRUE(storage.hasTopic("topic2"));

    EXPECT_TRUE(storage.delPublishersByProc("pid1"));

    EXPECT_FALSE(storage.hasTopic("topic1"));
    EXPECT_TRUE(storage.hasTopic("topic2"));
    EXPECT_FALSE(storage.hasPublisher("tcp://localhost:5555"));
    EXPECT_FALSE(storage.hasAnyPublishers("topic1", "pid1"));

    MessagePublisherInfo pub4("topic1", "tcp://localhost:5555", "unused", "pid1", "node4",
        rf::msgs::ExampleMsg::descriptor()->full_name(), rf::trans::AdvertiseMessageOptions{});
    storage.addPublisher(pub4);

    EXPECT_TRUE(storage.hasTopic("topic1"));
    EXPECT_TRUE(storage.delPublishersByNode("topic1", "pid1", "node4"));
    storage.print(std::cout);
    EXPECT_FALSE(storage.hasTopic("topic1"));

    EXPECT_TRUE(storage.delPublishersByNode("topic2", "pid2", "node3"));
    EXPECT_FALSE(storage.hasTopic("topic2"));
    EXPECT_FALSE(storage.hasPublisher("tcp://localhost:6666"));

    EXPECT_TRUE(storage.getTopicList().empty());
}

int main(int argc, char** argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}