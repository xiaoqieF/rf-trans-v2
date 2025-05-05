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
            elog::info("requset to remote success, result: {}, response: {}", result, reply->DebugString());
            return;
        });

        std::this_thread::sleep_for(200ms);
    }
}
