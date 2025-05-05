#include "trans/node.hpp"
#include "msgs/example.pb.h"

using namespace rf::trans;
using namespace std::chrono_literals;

int main()
{
    auto node = Node();
    node.advertise<rf::msgs::ExampleMsg, rf::msgs::ExampleMsg>("/service_topic",
        [](const std::shared_ptr<const rf::msgs::ExampleMsg>req, std::shared_ptr<rf::msgs::ExampleMsg> res) {
        elog::info("receive request: {}", req->ShortDebugString());
        res->set_age(222);
        res->set_name("response name");
        return true;
    });

    auto node1 = Node();
    node1.advertise<rf::msgs::ExampleMsg>("/service_topic_no_response",
        [](const std::shared_ptr<const rf::msgs::ExampleMsg>req) {
        elog::info("receive request: {}", req->ShortDebugString());
        return true;
    });

    node1.advertise<rf::msgs::ExampleMsg>("/service_topic_no_request",
        [](std::shared_ptr<rf::msgs::ExampleMsg> rep) {
        elog::info("receive empty request.");
        rep->set_age(12);
        rep->set_name("hello");
        return true;
    });


    // local request
    auto node2 = Node();
    for (;;) {
        auto request = std::make_shared<rf::msgs::ExampleMsg>();
        request->set_age(20);
        request->set_name("bob");
        node2.request<rf::msgs::ExampleMsg, rf::msgs::ExampleMsg>("/service_topic", request,
            [](const std::shared_ptr<const rf::msgs::ExampleMsg> reply, const bool result) {
            elog::info("[service_topic]requset success, result: {}, response: {}", result, reply->ShortDebugString());
            return;
        });

        node2.request<rf::msgs::ExampleMsg>("/service_topic_no_response", request, [](const bool result) {
            elog::info("[no_response]request success. result: {}", result);
            return;
        });

        node2.request<rf::msgs::ExampleMsg>("/service_topic_no_request",
            [](const std::shared_ptr<const rf::msgs::ExampleMsg> reply, const bool result) {
            elog::info("[no_request]request success. result: {}, response: {}", result, reply->ShortDebugString());
            return;
        });

        std::this_thread::sleep_for(1s);
    }
}