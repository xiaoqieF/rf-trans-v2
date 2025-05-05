#include "trans/node.hpp"
#include "msgs/example.pb.h"

using namespace rf::trans;
using namespace std::chrono_literals;

int main()
{
    auto node = Node();
    node.advertise<rf::msgs::ExampleMsg, rf::msgs::ExampleMsg>("/service_topic",
        [](const std::shared_ptr<const rf::msgs::ExampleMsg>req, std::shared_ptr<rf::msgs::ExampleMsg> res) {
        elog::info("receive request: {}", req->DebugString());
        res->set_age(222);
        res->set_name("response name");
        return true;
    });

    // local request
    auto node2 = Node();
    for (;;) {
        auto request = std::make_shared<rf::msgs::ExampleMsg>();
        request->set_age(0);
        request->set_name("bob");
        node2.request<rf::msgs::ExampleMsg, rf::msgs::ExampleMsg>("/service_topic", request, [](const std::shared_ptr<const rf::msgs::ExampleMsg> reply, const bool result) {
            elog::info("requset success, result: {}, response: {}", result, reply->DebugString());
            return;
        });

        std::this_thread::sleep_for(200ms);
    }
}