#include "msgs/header.pb.h"
#include "msgs/example.pb.h"
#include "trans/node.hpp"

using namespace std::chrono_literals;
using namespace rf::trans;

int main(int argc, char** argv)
{
    auto pub_node = Node();
    auto publisher = pub_node.advertise<rf::msgs::ExampleMsg>("/test_topic");

    // create a local subscriber
    auto sub_node = Node();
    sub_node.subscribe<rf::msgs::ExampleMsg>("/test_topic", [](const std::shared_ptr<const rf::msgs::ExampleMsg> msg) {
        elog::info("recv msg: {}", msg->ShortDebugString());
    });

    while (true) {
        std::this_thread::sleep_for(2s);
        // rf::msgs::ExampleMsg msg;
        // msg.set_age(12);
        // msg.set_name("bob");
        auto msg = std::make_unique<rf::msgs::ExampleMsg>();
        msg->set_age(12);
        msg->set_name("nancy");
        publisher.publish(std::move(msg));
        elog::info("pub a msg");
    }

    return 0;
}