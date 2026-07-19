# rf-trans

`rf-trans` 是一个基于 C++17、Protocol Buffers 和 ZeroMQ 的轻量级进程间通信库。它将消息和服务以主题（topic）组织，通过 UDP 组播自动发现同一网络中的端点，并在本机或远端透明地分发数据。

项目当前提供库目标 `rf-trans`，以及位于 `test/` 下的发布订阅、服务调用和发现示例。它还没有安装规则、命令行工具或 Python 绑定。

## 已实现的能力

- **发布订阅**：`Node::advertise<MessageT>()` 创建发布者，`Node::subscribe<MessageT>()` 注册订阅回调；消息类型必须继承 `google::protobuf::Message`。
- **本地和远端传输**：同一进程内的订阅通过内部队列投递；远端消息通过 ZeroMQ `PUB/SUB` 传输。发布 `std::unique_ptr<ProtoMsg>` 时，本地投递可复用消息所有权，减少一次拷贝。
- **自动发现**：消息和服务分别使用 UDP 组播发现，默认组播地址为 `239.255.0.7`，端口分别为 `10319` 和 `10320`。发现到远端端点后自动建立 ZeroMQ 连接。
- **服务请求响应**：支持异步回调和带超时的同步调用，也支持“仅请求”和“仅响应”的服务形式。请求会优先调用同一进程内匹配的服务端。
- **发布选项**：可指定可见范围 `PROCESS`、`HOST` 或 `ALL`（默认）；消息发布还可通过 `setMsgsPerSec()` 限速。
- **端点查询和清理**：可查询已发现的 topic / service，等待服务上线，并取消订阅或撤销已发布服务。`Node` 析构时会清理其订阅和服务声明。

## 架构概览

```text
Node / Publisher
    |                         UDP multicast
    +-- advertise / subscribe <---- Discovery ----> remote processes
    |                                   |
    +-- local callback queue           +-- establish ZeroMQ connections
                                        |
                             PUB/SUB messages, ROUTER-based services
```

每个进程共享一个内部 `NodeShared` 实例，因此在一个进程中创建多个 `Node` 时，它们复用网络套接字、发现器和后台处理线程；每个 `Node` 仍有独立的 node UUID、订阅和服务声明。

## 依赖与平台

- Linux（代码使用 POSIX socket API 和 `pthread_setname_np`）
- CMake >= 3.22
- 支持 C++17 的编译器
- Protocol Buffers：开发库和 `protoc`
- ZeroMQ 开发库（`libzmq`）以及 C++ 头文件 `zmq.hpp`（通常由 `cppzmq` 开发包提供）
- `pkg-config`

在 Debian / Ubuntu 上，通常可安装如下依赖：

```bash
sudo apt install build-essential cmake pkg-config protobuf-compiler libprotobuf-dev libzmq3-dev cppzmq-dev
```

## 构建

库会在构建目录中从 `msgs/*.proto` 生成 C++ 源码和头文件。下列命令关闭单元测试，适合只构建库和示例：

```bash
cmake -S . -B build -DBUILD_TESTING=OFF
cmake --build build -j
```

生成的目标包括：

```text
build/librf-trans.a
build/test/examples/example_pub
build/test/examples/example_sub
build/test/examples/example_request
build/test/examples/example_response
build/test/examples/example_discovery
```

启用单元测试时，CMake 会通过 `FetchContent` 下载 GoogleTest，因此首次配置需要能访问 GitHub：

```bash
cmake -S . -B build -DBUILD_TESTING=ON
cmake --build build -j
ctest --test-dir build --output-on-failure
```

## 快速开始

示例消息定义在 `msgs/example.proto`：

```proto
message ExampleMsg {
    string name = 1;
    int32 age = 2;
}
```

### 发布和订阅

创建发布者和订阅者时，模板参数必须是相同的 Protobuf 消息类型。下面的进程内示例也可拆分为两个独立进程；在组播可达的网络中，它们会自动互相发现。

```cpp
#include "msgs/example.pb.h"
#include "trans/node.hpp"

using rf::trans::Node;

int main()
{
    Node publisher_node;
    auto publisher = publisher_node.advertise<rf::msgs::ExampleMsg>("/greeting");

    Node subscriber_node;
    subscriber_node.subscribe<rf::msgs::ExampleMsg>("/greeting",
        [](const std::shared_ptr<const rf::msgs::ExampleMsg>& message) {
            elog::info("received: {}", message->ShortDebugString());
        });

    rf::msgs::ExampleMsg message;
    message.set_name("Nancy");
    message.set_age(12);
    publisher.publish(message);
}
```

`Publisher::publish()` 会检查运行时消息类型是否与 `advertise()` 时的类型一致。可使用 `Publisher::hasConnections()` 判断是否有本地或远端订阅者；对于高频消息，可配置限速：

```cpp
rf::trans::AdvertiseMessageOptions options;
options.setMsgsPerSec(30);
auto publisher = node.advertise<rf::msgs::ExampleMsg>("/status", options);
```

### 服务调用

服务端用 `advertise<RequestT, ReplyT>()` 注册回调；回调返回值表示业务处理是否成功。客户端可异步传入回调，或使用带超时的重载进行同步等待。

```cpp
// Server
Node server;
server.advertise<rf::msgs::ExampleMsg, rf::msgs::ExampleMsg>("/echo",
    [](const std::shared_ptr<const rf::msgs::ExampleMsg>& request,
       std::shared_ptr<rf::msgs::ExampleMsg> reply) {
        reply->CopyFrom(*request);
        return true;
    });

// Client
Node client;
auto request = std::make_shared<rf::msgs::ExampleMsg>();
request->set_name("hello");
client.request<rf::msgs::ExampleMsg, rf::msgs::ExampleMsg>("/echo", request,
    [](const std::shared_ptr<const rf::msgs::ExampleMsg>& reply, bool success) {
        if (success) {
            elog::info("reply: {}", reply->ShortDebugString());
        }
    });
```

以下简化形式也可用：

- `advertise<RequestT>(topic, callback)`：服务只接收请求，不返回业务响应。
- `advertise<ReplyT>(topic, callback)`：服务不接收业务请求，只返回响应。
- 与上述两种服务对应的 `request()` 重载会自动使用 `rf::msgs::Empty`。
- `waitForService(topic, timeout)` 可在请求前等待本地或远端服务被发现。

完整的可执行示例见 `test/examples/`。构建后可在不同终端运行，例如：

```bash
./build/test/examples/example_sub
./build/test/examples/example_pub
```

## 性能基准

性能基准位于 `test/benchmark/`，使用 Google Benchmark。默认不构建，避免常规构建增加依赖；开启后会优先使用系统安装的 Google Benchmark，不存在时由 CMake 下载 v1.8.3。

```bash
cmake -S . -B build-benchmark -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=ON -DCMAKE_BUILD_TYPE=Release
cmake --build build-benchmark -j
./build-benchmark/test/benchmark/benchmark_pubsub_service \
  --benchmark_repetitions=10 --benchmark_report_aggregates_only=true
```

每种负载分别测试 32 B、1 KiB 和 64 KiB 的 `ExampleMsg` payload：

- `BM_PubSubEndToEndLatency`：单个在途消息从 `publish()` 到订阅回调完成的延迟。
- `BM_PubSubSustainedThroughput`：128 条一批的发布订阅吞吐，计时至该批回调全部完成。
- `BM_ServiceLocalRoundTrip`：请求、服务处理器和响应回调的本地往返延迟。
- `BM_PubSubInterprocessEndToEndLatency`：基准程序自动启动同机订阅端子进程后，从 `publish()` 到远端订阅回调确认的端到端延迟。
- `BM_ServiceInterprocessRoundTrip`：基准程序自动启动同机服务端子进程后，客户端到远端服务端再返回客户端的往返延迟。

前三项使用进程内端点，以隔离库本身的数据路径。两个跨进程基准会在计时前完成子进程启动、UDP 组播发现、ZeroMQ 连接和一次预热通信；计时区间仅包含一次消息或服务请求及已校验的远端确认。它们需要同机进程可使用 UDP 组播发现，必要时为运行基准的进程设置可达的 `RF_HOST_IP`。消息对象构造、端点创建、连接就绪检查和销毁不计入计时。

启用 `BUILD_BENCHMARKS` 时，单配置生成器会强制使用 `Release`；对 Visual Studio、Xcode 等多配置生成器，请使用 `cmake --build build-benchmark --config Release`。

## 网络与部署注意事项

- 自动发现依赖 IPv4 UDP 组播。跨主机运行时，主机、防火墙、容器网络和交换机必须允许组播流量及动态分配的 TCP 端口。
- Discovery 和 TCP endpoint 使用同一张网卡。默认选择枚举到的第一个可用 IPv4 组播地址；多网卡、VPN 或容器环境应设置 `RF_HOST_IP` 指定可达的本机 IPv4 地址，例如：`RF_HOST_IP=192.168.1.10 ./build/test/examples/example_pub`。
- 若组播不可用，远端自动发现和通信无法建立；同一进程内的发布订阅和服务调用不依赖远端发现。
- `Scope::PROCESS` 不向其他进程广播，`Scope::HOST` 仅允许来自所选网卡或 loopback 的本机发现，`Scope::ALL` 允许网络内发现；同机进程应使用一致的 `RF_HOST_IP` 配置。
- topic 字符串和 Protobuf 全限定类型名共同参与匹配；同名 topic 上类型不一致的消息或服务不会作为匹配端点使用。

## 目录说明

```text
include/trans/  对外 C++ 接口（node、publisher 和 advertise options）
include/trans/details/  内部运行时、发现机制和模板实现
src/            Node、Publisher 和共享运行时实现
msgs/           Discovery 协议与示例 Protobuf 消息
test/examples/  可执行通信示例
test/benchmark/ 发布订阅与服务基准
test/ut/        GoogleTest 单元测试
deps/elog/      随源码附带的日志库
todo.md         后续计划
```

## 当前边界

项目仍处于开发阶段。`todo.md` 中列出了 Python 绑定、命令行工具、性能基准和清理待办等计划；目前 CMake 也尚未提供 `install()` / `find_package()` 集成。将其嵌入其他项目时，建议先通过 `add_subdirectory()` 引入，并确保消费者能找到构建目录生成的 `msgs/*.pb.h` 文件。
