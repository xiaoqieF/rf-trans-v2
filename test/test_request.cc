#include "trans/node.hpp"

#include "msgs/example.pb.h"

using namespace rf::trans;
using namespace std::chrono_literals;

int main()
{
    auto node2 = Node();
    for (;;) {
        auto request = std::make_shared<rf::msgs::ExampleMsg>();
        request->set_age(33);
        request->set_name("from remote");
        node2.request<rf::msgs::ExampleMsg, rf::msgs::ExampleMsg>("/service_topic", request,
            [](const std::shared_ptr<const rf::msgs::ExampleMsg> reply, const bool result) {
            elog::info("[normal_service]requset to remote success, result: {}, response: {}", result, reply->ShortDebugString());
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

        std::this_thread::sleep_for(200ms);
    }
}
