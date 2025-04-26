#include "trans/node_shared.hpp"

using namespace std::chrono_literals;

int main()
{
    auto& node_shared = rf::trans::NodeShared::getInstance();

    // std::this_thread::sleep_for(100s);

    return 0;
}