#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <benchmark/benchmark.h>

#include "msgs/example.pb.h"
#include "trans/node.hpp"

namespace
{
using namespace std::chrono_literals;
using rf::msgs::ExampleMsg;
using rf::trans::AdvertiseMessageOptions;
using rf::trans::AdvertiseServiceOptions;
using rf::trans::Node;
using rf::trans::Publisher;
using rf::trans::Scope;

constexpr auto kCallbackTimeout = 5s;
constexpr int kPublishBatchSize = 128;

std::atomic<uint64_t> next_topic_id{0};

std::string makeTopic(const char* benchmark_name)
{
    return std::string{"/benchmark/"} + benchmark_name + "/" + std::to_string(next_topic_id++);
}

class PubSubBenchmark
{
public:
    explicit PubSubBenchmark(const size_t payload_size)
        : topic_(makeTopic("pubsub")), state_(std::make_shared<CallbackState>(payload_size))
    {
        if (!subscriber_.subscribe<ExampleMsg>(topic_, [state = state_](const std::shared_ptr<const ExampleMsg>& message) {
                std::lock_guard lock(state->callback_mutex);
                const uint64_t sequence = state->received.fetch_add(1, std::memory_order_acq_rel) + 1;
                if (!message || message->name() != state->payload ||
                    message->age() != static_cast<int>(sequence)) {
                    state->callback_valid.store(false, std::memory_order_release);
                }
                state->callback_cv.notify_one();
            })) {
            throw std::runtime_error("failed to subscribe benchmark receiver");
        }

        AdvertiseMessageOptions options;
        options.setScope(Scope::PROCESS);
        publisher_ = std::make_unique<Publisher>(publisher_node_.advertise<ExampleMsg>(topic_, options));
        if (!publisher_->valid()) {
            throw std::runtime_error("failed to advertise benchmark publisher");
        }
        if (!waitForConnections()) {
            throw std::runtime_error("benchmark publisher did not observe its local subscriber");
        }
    }

    ~PubSubBenchmark()
    {
        subscriber_.unsubscribe(topic_);
        publisher_node_.unadvertise(topic_);
    }

    std::unique_ptr<ExampleMsg> makeMessage(const int sequence) const
    {
        auto message = std::make_unique<ExampleMsg>();
        message->set_name(state_->payload);
        message->set_age(sequence);
        return message;
    }

    bool publish(std::unique_ptr<ExampleMsg> message)
    {
        return publisher_->publish(std::move(message));
    }

    uint64_t received() const
    {
        return state_->received.load(std::memory_order_acquire);
    }

    bool waitFor(const uint64_t expected)
    {
        std::unique_lock lock(state_->callback_mutex);
        return state_->callback_cv.wait_for(lock, kCallbackTimeout, [state = state_, expected] {
            return state->received.load(std::memory_order_acquire) >= expected;
        });
    }

    bool callbackValid() const { return state_->callback_valid.load(std::memory_order_acquire); }
    size_t payloadSize() const { return state_->payload.size(); }

private:
    struct CallbackState
    {
        explicit CallbackState(const size_t payload_size) : payload(payload_size, 'x') {}

        const std::string payload;
        std::atomic<uint64_t> received{0};
        std::atomic<bool> callback_valid{true};
        std::mutex callback_mutex;
        std::condition_variable callback_cv;
    };

    bool waitForConnections() const
    {
        const auto deadline = std::chrono::steady_clock::now() + kCallbackTimeout;
        while (!publisher_->hasConnections() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(1ms);
        }
        return publisher_->hasConnections();
    }

    Node publisher_node_;
    Node subscriber_;
    const std::string topic_;
    const std::shared_ptr<CallbackState> state_;
    std::unique_ptr<Publisher> publisher_;
};

PubSubBenchmark& pubSubFixture(const size_t payload_size)
{
    static std::mutex fixture_mutex;
    // Google Benchmark reinvokes a benchmark while calibrating iteration counts.
    // Keep one endpoint pair per payload so calibration measures message flow,
    // not repeated endpoint churn.
    static auto* fixtures = new std::map<size_t, std::unique_ptr<PubSubBenchmark>>;

    std::lock_guard lock(fixture_mutex);
    auto& fixture = (*fixtures)[payload_size];
    if (!fixture) {
        fixture = std::make_unique<PubSubBenchmark>(payload_size);
    }
    return *fixture;
}

void BM_PubSubEndToEndLatency(benchmark::State& state)
{
    auto& fixture = pubSubFixture(static_cast<size_t>(state.range(0)));

    for (auto _ : state) {
        (void)_;
        const uint64_t expected = fixture.received() + 1;
        auto message = fixture.makeMessage(static_cast<int>(expected));

        const auto start = std::chrono::steady_clock::now();
        if (!fixture.publish(std::move(message))) {
            state.SkipWithError("publish failed");
            return;
        }
        if (!fixture.waitFor(expected)) {
            state.SkipWithError("timed out waiting for subscriber callback");
            return;
        }
        state.SetIterationTime(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());
    }

    if (!fixture.callbackValid()) {
        state.SkipWithError("subscriber received an unexpected message");
        return;
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(fixture.payloadSize()));
    state.counters["payload_bytes"] = static_cast<double>(fixture.payloadSize());
}

void BM_PubSubSustainedThroughput(benchmark::State& state)
{
    auto& fixture = pubSubFixture(static_cast<size_t>(state.range(0)));

    for (auto _ : state) {
        (void)_;
        const uint64_t first_sequence = fixture.received() + 1;
        std::vector<std::unique_ptr<ExampleMsg>> messages;
        messages.reserve(kPublishBatchSize);
        for (int index = 0; index < kPublishBatchSize; ++index) {
            messages.push_back(fixture.makeMessage(static_cast<int>(first_sequence + index)));
        }

        const auto start = std::chrono::steady_clock::now();
        for (auto& message : messages) {
            if (!fixture.publish(std::move(message))) {
                state.SkipWithError("publish failed");
                return;
            }
        }
        if (!fixture.waitFor(first_sequence + kPublishBatchSize - 1)) {
            state.SkipWithError("timed out waiting for subscriber batch");
            return;
        }
        state.SetIterationTime(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());
    }

    if (!fixture.callbackValid()) {
        state.SkipWithError("subscriber received an unexpected message");
        return;
    }
    const int64_t messages = state.iterations() * kPublishBatchSize;
    state.SetItemsProcessed(messages);
    state.SetBytesProcessed(messages * static_cast<int64_t>(fixture.payloadSize()));
    state.counters["payload_bytes"] = static_cast<double>(fixture.payloadSize());
}

void BM_ServiceLocalRoundTrip(benchmark::State& state)
{
    const size_t payload_size = static_cast<size_t>(state.range(0));
    const std::string topic = makeTopic("service");
    const std::string payload(payload_size, 'x');
    Node server;
    Node client;

    AdvertiseServiceOptions service_options;
    service_options.setScope(Scope::PROCESS);
    if (!server.advertise<ExampleMsg, ExampleMsg>(topic,
            [](const std::shared_ptr<const ExampleMsg>& request, std::shared_ptr<ExampleMsg> reply) {
                reply->set_name(request->name());
                reply->set_age(request->age() + 1);
                return true;
            }, service_options)) {
        state.SkipWithError("failed to advertise benchmark service");
        return;
    }
    if (!client.waitForService(topic, kCallbackTimeout)) {
        state.SkipWithError("benchmark service did not become available");
        return;
    }

    auto request = std::make_shared<ExampleMsg>();
    request->set_name(payload);
    request->set_age(41);

    for (auto _ : state) {
        (void)_;
        bool callback_called = false;
        bool callback_succeeded = false;
        const auto start = std::chrono::steady_clock::now();
        const bool accepted = client.request<ExampleMsg, ExampleMsg>(topic, request,
            [&callback_called, &callback_succeeded, &payload](const std::shared_ptr<const ExampleMsg>& reply,
                const bool success) {
                callback_called = true;
                int response_age = reply ? reply->age() : -1;
                callback_succeeded = success && reply && reply->name() == payload && response_age == 42;
                benchmark::DoNotOptimize(response_age);
            });
        state.SetIterationTime(std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());

        if (!accepted || !callback_called || !callback_succeeded) {
            state.SkipWithError("service request failed");
            return;
        }
    }

    state.SetItemsProcessed(state.iterations());
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(payload_size));
    state.counters["payload_bytes"] = static_cast<double>(payload_size);
}

BENCHMARK(BM_PubSubEndToEndLatency)->Arg(32)->Arg(1024)->Arg(65536)->UseManualTime();
BENCHMARK(BM_PubSubSustainedThroughput)->Arg(32)->Arg(1024)->Arg(65536)->UseManualTime();
BENCHMARK(BM_ServiceLocalRoundTrip)->Arg(32)->Arg(1024)->Arg(65536)->UseManualTime();
} // namespace

BENCHMARK_MAIN();
