#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "trans/discovery_observer.hpp"

namespace
{
using rf::trans::DiscoveryObserver;
using rf::trans::DiscoveryObserverOptions;
using rf::trans::MessageEndpointInfo;
using rf::trans::Scope;
using rf::trans::ServiceEndpointInfo;

constexpr int kExitUsage = 2;
constexpr int kExitNotFound = 3;
constexpr int kExitRuntime = 4;

volatile std::sig_atomic_t g_stop_requested = 0;

void requestStop(int)
{
    g_stop_requested = 1;
}

enum class OutputFormat { Table, Json, JsonLines };

struct Options
{
    DiscoveryObserverOptions discovery;
    OutputFormat output{OutputFormat::Table};
    unsigned int wait_ms{1200};
    unsigned int watch_interval_ms{500};
    std::string prefix;
    std::vector<std::string> positional;
};

std::string scopeName(const Scope scope)
{
    switch (scope) {
    case Scope::PROCESS: return "process";
    case Scope::HOST: return "host";
    case Scope::ALL: return "all";
    }
    return "unknown";
}

std::string jsonEscape(const std::string& value)
{
    std::ostringstream out;
    for (const unsigned char c : value) {
        switch (c) {
        case '"': out << "\\\""; break;
        case '\\': out << "\\\\"; break;
        case '\b': out << "\\b"; break;
        case '\f': out << "\\f"; break;
        case '\n': out << "\\n"; break;
        case '\r': out << "\\r"; break;
        case '\t': out << "\\t"; break;
        default:
            if (c < 0x20U) {
                out << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                    << static_cast<unsigned int>(c) << std::dec << std::setfill(' ');
            } else {
                out << c;
            }
        }
    }
    return out.str();
}

std::string quoteJson(const std::string& value)
{
    return "\"" + jsonEscape(value) + "\"";
}

std::string processDisplayName(const std::string& process_name)
{
    return process_name.empty() ? "unknown" : process_name;
}

std::string messageJson(const MessageEndpointInfo& endpoint, const std::string& event = "")
{
    std::ostringstream out;
    out << "{\"kind\":\"message\"";
    if (!event.empty()) out << ",\"event\":" << quoteJson(event);
    out << ",\"topic\":" << quoteJson(endpoint.topic)
        << ",\"address\":" << quoteJson(endpoint.address)
        << ",\"control_id\":" << quoteJson(endpoint.control_id)
        << ",\"process_name\":" << quoteJson(endpoint.process_name)
        << ",\"node_uuid\":" << quoteJson(endpoint.node_uuid)
        << ",\"message_type\":" << quoteJson(endpoint.message_type)
        << ",\"scope\":" << quoteJson(scopeName(endpoint.scope))
        << ",\"throttled\":" << (endpoint.throttled ? "true" : "false")
        << ",\"messages_per_second\":";
    if (endpoint.throttled) out << endpoint.messages_per_second;
    else out << "null";
    out << "}";
    return out.str();
}

std::string serviceJson(const ServiceEndpointInfo& endpoint, const std::string& event = "")
{
    std::ostringstream out;
    out << "{\"kind\":\"service\"";
    if (!event.empty()) out << ",\"event\":" << quoteJson(event);
    out << ",\"topic\":" << quoteJson(endpoint.topic)
        << ",\"address\":" << quoteJson(endpoint.address)
        << ",\"socket_id\":" << quoteJson(endpoint.socket_id)
        << ",\"process_name\":" << quoteJson(endpoint.process_name)
        << ",\"node_uuid\":" << quoteJson(endpoint.node_uuid)
        << ",\"request_type\":" << quoteJson(endpoint.request_type)
        << ",\"response_type\":" << quoteJson(endpoint.response_type)
        << ",\"scope\":" << quoteJson(scopeName(endpoint.scope)) << "}";
    return out.str();
}

bool startsWith(const std::string& value, const std::string& prefix)
{
    return prefix.empty() || value.rfind(prefix, 0) == 0;
}

template<typename Endpoint>
std::vector<Endpoint> filterByPrefix(std::vector<Endpoint> endpoints, const std::string& prefix)
{
    endpoints.erase(std::remove_if(endpoints.begin(), endpoints.end(), [&prefix](const Endpoint& endpoint) {
        return !startsWith(endpoint.topic, prefix);
    }), endpoints.end());
    return endpoints;
}

template<typename Endpoint>
std::vector<Endpoint> filterByTopic(std::vector<Endpoint> endpoints, const std::string& topic)
{
    endpoints.erase(std::remove_if(endpoints.begin(), endpoints.end(), [&topic](const Endpoint& endpoint) {
        return endpoint.topic != topic;
    }), endpoints.end());
    return endpoints;
}

void printMessageTable(const std::vector<MessageEndpointInfo>& endpoints, const std::string& event = "")
{
    const auto longest = [&endpoints](const std::string& heading, const auto& getter) {
        std::size_t width = heading.size();
        for (const auto& endpoint : endpoints) width = std::max(width, getter(endpoint).size());
        return width;
    };
    const auto topic_width = longest("TOPIC", [](const auto& endpoint) -> const std::string& { return endpoint.topic; });
    const auto type_width = longest("TYPE", [](const auto& endpoint) -> const std::string& { return endpoint.message_type; });
    const auto address_width = longest("ADDRESS", [](const auto& endpoint) -> const std::string& { return endpoint.address; });
    const auto scope_width = longest("SCOPE", [](const auto& endpoint) { return scopeName(endpoint.scope); });
    const auto process_width = longest("PROCESS", [](const auto& endpoint) {
        return processDisplayName(endpoint.process_name);
    });

    if (!event.empty()) std::cout << std::left << std::setw(7) << "EVENT" << "  ";
    std::cout << std::left << std::setw(static_cast<int>(topic_width)) << "TOPIC" << "  "
              << std::setw(static_cast<int>(type_width)) << "TYPE" << "  "
              << std::setw(static_cast<int>(address_width)) << "ADDRESS" << "  "
              << std::setw(static_cast<int>(scope_width)) << "SCOPE" << "  "
              << "PROCESS\n";
    for (const auto& endpoint : endpoints) {
        if (!event.empty()) std::cout << std::left << std::setw(7) << event << "  ";
        std::cout << std::left << std::setw(static_cast<int>(topic_width)) << endpoint.topic << "  "
                  << std::setw(static_cast<int>(type_width)) << endpoint.message_type << "  "
                  << std::setw(static_cast<int>(address_width)) << endpoint.address << "  "
                  << std::setw(static_cast<int>(scope_width)) << scopeName(endpoint.scope) << "  "
                  << std::setw(static_cast<int>(process_width))
                  << processDisplayName(endpoint.process_name) << '\n';
    }
}

void printServiceTable(const std::vector<ServiceEndpointInfo>& endpoints, const std::string& event = "")
{
    const auto longest = [&endpoints](const std::string& heading, const auto& getter) {
        std::size_t width = heading.size();
        for (const auto& endpoint : endpoints) width = std::max(width, getter(endpoint).size());
        return width;
    };
    const auto topic_width = longest("TOPIC", [](const auto& endpoint) -> const std::string& { return endpoint.topic; });
    const auto request_width = longest("REQUEST TYPE", [](const auto& endpoint) -> const std::string& { return endpoint.request_type; });
    const auto response_width = longest("RESPONSE TYPE", [](const auto& endpoint) -> const std::string& { return endpoint.response_type; });
    const auto address_width = longest("ADDRESS", [](const auto& endpoint) -> const std::string& { return endpoint.address; });
    const auto scope_width = longest("SCOPE", [](const auto& endpoint) { return scopeName(endpoint.scope); });
    const auto process_width = longest("PROCESS", [](const auto& endpoint) {
        return processDisplayName(endpoint.process_name);
    });

    if (!event.empty()) std::cout << std::left << std::setw(7) << "EVENT" << "  ";
    std::cout << std::left << std::setw(static_cast<int>(topic_width)) << "TOPIC" << "  "
              << std::setw(static_cast<int>(request_width)) << "REQUEST TYPE" << "  "
              << std::setw(static_cast<int>(response_width)) << "RESPONSE TYPE" << "  "
              << std::setw(static_cast<int>(scope_width)) << "SCOPE" << "  "
              << std::setw(static_cast<int>(address_width)) << "ADDRESS" << "  "
              << "PROCESS\n";
    for (const auto& endpoint : endpoints) {
        if (!event.empty()) std::cout << std::left << std::setw(7) << event << "  ";
        std::cout << std::left << std::setw(static_cast<int>(topic_width)) << endpoint.topic << "  "
                  << std::setw(static_cast<int>(request_width)) << endpoint.request_type << "  "
                  << std::setw(static_cast<int>(response_width)) << endpoint.response_type << "  "
                  << std::setw(static_cast<int>(scope_width)) << scopeName(endpoint.scope) << "  "
                  << std::setw(static_cast<int>(address_width)) << endpoint.address << "  "
                  << std::setw(static_cast<int>(process_width))
                  << processDisplayName(endpoint.process_name) << '\n';
    }
}

void printMessageDetails(const std::vector<MessageEndpointInfo>& endpoints)
{
    for (const auto& endpoint : endpoints) {
        std::cout << "TOPIC: " << endpoint.topic << '\n'
                  << "MESSAGE_TYPE: " << endpoint.message_type << '\n'
                  << "ADDRESS: " << endpoint.address << '\n'
                  << "CONTROL_ID: " << endpoint.control_id << '\n'
                  << "PROCESS_NAME: " << endpoint.process_name << '\n'
                  << "NODE_UUID: " << endpoint.node_uuid << '\n'
                  << "SCOPE: " << scopeName(endpoint.scope) << '\n'
                  << "MESSAGES_PER_SECOND: ";
        if (endpoint.throttled) std::cout << endpoint.messages_per_second;
        else std::cout << "unlimited";
        std::cout << "\n\n";
    }
}

void printServiceDetails(const std::vector<ServiceEndpointInfo>& endpoints)
{
    for (const auto& endpoint : endpoints) {
        std::cout << "TOPIC: " << endpoint.topic << '\n'
                  << "REQUEST_TYPE: " << endpoint.request_type << '\n'
                  << "RESPONSE_TYPE: " << endpoint.response_type << '\n'
                  << "ADDRESS: " << endpoint.address << '\n'
                  << "SOCKET_ID: " << endpoint.socket_id << '\n'
                  << "PROCESS_NAME: " << endpoint.process_name << '\n'
                  << "NODE_UUID: " << endpoint.node_uuid << '\n'
                  << "SCOPE: " << scopeName(endpoint.scope) << "\n\n";
    }
}

void printMessages(const std::vector<MessageEndpointInfo>& endpoints, const OutputFormat output,
    const std::string& event = "")
{
    if (output == OutputFormat::Table) {
        printMessageTable(endpoints, event);
        return;
    }
    if (output == OutputFormat::Json) {
        std::cout << "[";
        for (std::size_t i = 0; i < endpoints.size(); ++i) {
            if (i != 0) std::cout << ",";
            std::cout << messageJson(endpoints[i], event);
        }
        std::cout << "]\n";
        return;
    }
    for (const auto& endpoint : endpoints) std::cout << messageJson(endpoint, event) << '\n';
}

void printServices(const std::vector<ServiceEndpointInfo>& endpoints, const OutputFormat output,
    const std::string& event = "")
{
    if (output == OutputFormat::Table) {
        printServiceTable(endpoints, event);
        return;
    }
    if (output == OutputFormat::Json) {
        std::cout << "[";
        for (std::size_t i = 0; i < endpoints.size(); ++i) {
            if (i != 0) std::cout << ",";
            std::cout << serviceJson(endpoints[i], event);
        }
        std::cout << "]\n";
        return;
    }
    for (const auto& endpoint : endpoints) std::cout << serviceJson(endpoint, event) << '\n';
}

unsigned int parseUnsigned(const std::string& name, const std::string& value, const unsigned long max_value)
{
    std::size_t consumed = 0;
    unsigned long parsed = 0;
    try {
        parsed = std::stoul(value, &consumed, 10);
    } catch (const std::exception&) {
        throw std::invalid_argument(name + " must be a positive integer");
    }
    if (consumed != value.size() || parsed == 0 || parsed > max_value) {
        throw std::invalid_argument(name + " is outside its accepted range");
    }
    return static_cast<unsigned int>(parsed);
}

std::string takeValue(const std::vector<std::string>& arguments, std::size_t& index, const std::string& option)
{
    if (++index >= arguments.size()) throw std::invalid_argument(option + " requires a value");
    return arguments[index];
}

Options parseOptions(int argc, char** argv)
{
    Options options;
    std::vector<std::string> arguments(argv + 1, argv + argc);
    for (std::size_t index = 0; index < arguments.size(); ++index) {
        const std::string& argument = arguments[index];
        if (argument == "--output") {
            const auto value = takeValue(arguments, index, argument);
            if (value == "table") options.output = OutputFormat::Table;
            else if (value == "json") options.output = OutputFormat::Json;
            else if (value == "jsonl") options.output = OutputFormat::JsonLines;
            else throw std::invalid_argument("--output must be table, json, or jsonl");
        } else if (argument == "--wait-ms") {
            options.wait_ms = parseUnsigned(argument, takeValue(arguments, index, argument), 600000);
        } else if (argument == "--watch-interval-ms") {
            options.watch_interval_ms = parseUnsigned(argument, takeValue(arguments, index, argument), 600000);
        } else if (argument == "--host-ip") {
            options.discovery.host_address = takeValue(arguments, index, argument);
        } else if (argument == "--discovery-group") {
            options.discovery.multicast_group = takeValue(arguments, index, argument);
        } else if (argument == "--message-port") {
            options.discovery.message_port = static_cast<std::uint16_t>(
                parseUnsigned(argument, takeValue(arguments, index, argument), 65535));
        } else if (argument == "--service-port") {
            options.discovery.service_port = static_cast<std::uint16_t>(
                parseUnsigned(argument, takeValue(arguments, index, argument), 65535));
        } else if (argument == "--prefix") {
            options.prefix = takeValue(arguments, index, argument);
        } else if (argument.rfind("--", 0) == 0) {
            throw std::invalid_argument("unknown option: " + argument);
        } else {
            options.positional.push_back(argument);
        }
    }
    return options;
}

void printUsage(std::ostream& output)
{
    output << "Usage: rf-trans [options] <command>\n\n"
        << "Commands:\n"
        << "  topic list                 List discovered message endpoints\n"
        << "  topic info <topic>         Show endpoints for one message topic\n"
        << "  service list               List discovered service endpoints\n"
        << "  service info <topic>       Show endpoints for one service topic\n"
        << "  watch topics|services      Stream endpoint add/remove events\n"
        << "  doctor                     Report discovery configuration and endpoint counts\n\n"
        << "Options:\n"
        << "  --output table|json|jsonl  Output format (default: table)\n"
        << "  --wait-ms <ms>             Initial discovery window (default: 1200)\n"
        << "  --prefix <prefix>          Filter list/watch topics by prefix\n"
        << "  --host-ip <IPv4>           Select the local multicast interface\n"
        << "  --discovery-group <IPv4>   Discovery multicast group\n"
        << "  --message-port <port>      Message discovery port\n"
        << "  --service-port <port>      Service discovery port\n"
        << "  --watch-interval-ms <ms>   Snapshot interval for watch (default: 500)\n";
}

template<typename Endpoint>
std::map<std::string, Endpoint> endpointMap(const std::vector<Endpoint>& endpoints);

template<>
std::map<std::string, MessageEndpointInfo> endpointMap(const std::vector<MessageEndpointInfo>& endpoints)
{
    std::map<std::string, MessageEndpointInfo> result;
    for (const auto& endpoint : endpoints) {
        result.emplace(endpoint.topic + "\n" + endpoint.address + "\n" + endpoint.process_uuid + "\n" + endpoint.node_uuid,
            endpoint);
    }
    return result;
}

template<>
std::map<std::string, ServiceEndpointInfo> endpointMap(const std::vector<ServiceEndpointInfo>& endpoints)
{
    std::map<std::string, ServiceEndpointInfo> result;
    for (const auto& endpoint : endpoints) {
        result.emplace(endpoint.topic + "\n" + endpoint.address + "\n" + endpoint.socket_id + "\n" + endpoint.process_uuid + "\n" + endpoint.node_uuid,
            endpoint);
    }
    return result;
}

template<typename Endpoint, typename Snapshot, typename Printer>
int watchEndpoints(const Options& options, Snapshot snapshot, Printer printer)
{
    auto previous = endpointMap(snapshot());
    std::vector<Endpoint> initial;
    for (const auto& item : previous) initial.push_back(item.second);
    if (!initial.empty()) printer(initial, "added");

    while (g_stop_requested == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(options.watch_interval_ms));
        const auto current = endpointMap(snapshot());
        std::vector<Endpoint> added;
        std::vector<Endpoint> removed;
        for (const auto& item : current) if (previous.count(item.first) == 0) added.push_back(item.second);
        for (const auto& item : previous) if (current.count(item.first) == 0) removed.push_back(item.second);
        if (!added.empty()) printer(added, "added");
        if (!removed.empty()) printer(removed, "removed");
        previous = current;
    }
    return 0;
}

int run(const Options& options)
{
    if (options.positional.empty()) {
        printUsage(std::cerr);
        return kExitUsage;
    }
    DiscoveryObserver observer(options.discovery);
    std::this_thread::sleep_for(std::chrono::milliseconds(options.wait_ms));

    const auto& command = options.positional[0];
    if (command == "doctor") {
        if (options.positional.size() != 1) {
            throw std::invalid_argument("doctor does not accept positional arguments");
        }
        const auto messages = filterByPrefix(observer.messageEndpoints(), options.prefix);
        const auto services = filterByPrefix(observer.serviceEndpoints(), options.prefix);
        if (options.output == OutputFormat::Table) {
            std::cout << "HOST_ADDRESS: " << observer.hostAddress() << '\n'
                      << "MULTICAST_GROUP: " << options.discovery.multicast_group << '\n'
                      << "MESSAGE_PORT: " << options.discovery.message_port << '\n'
                      << "SERVICE_PORT: " << options.discovery.service_port << '\n'
                      << "DISCOVERY_WINDOW_MS: " << options.wait_ms << '\n'
                      << "MESSAGE_ENDPOINTS: " << messages.size() << '\n'
                      << "SERVICE_ENDPOINTS: " << services.size() << '\n';
        } else {
            std::cout << "{\"host_address\":" << quoteJson(observer.hostAddress())
                      << ",\"multicast_group\":" << quoteJson(options.discovery.multicast_group)
                      << ",\"message_port\":" << options.discovery.message_port
                      << ",\"service_port\":" << options.discovery.service_port
                      << ",\"discovery_window_ms\":" << options.wait_ms
                      << ",\"message_endpoints\":" << messages.size()
                      << ",\"service_endpoints\":" << services.size() << "}\n";
        }
        return 0;
    }

    if (command == "topic" || command == "service") {
        if (options.positional.size() < 2 || options.positional.size() > 3) {
            throw std::invalid_argument(command + " requires list or info <topic>");
        }
        const bool is_topic = command == "topic";
        const auto& action = options.positional[1];
        if (action != "list" && action != "info") {
            throw std::invalid_argument(command + " action must be list or info");
        }
        if (action == "list" && options.positional.size() != 2) {
            throw std::invalid_argument(command + " list does not accept a topic");
        }
        if (action == "info" && options.positional.size() != 3) {
            throw std::invalid_argument(command + " info requires a topic");
        }

        if (is_topic) {
            auto endpoints = filterByPrefix(observer.messageEndpoints(), options.prefix);
            if (action == "info") endpoints = filterByTopic(std::move(endpoints), options.positional[2]);
            if (action == "info" && options.output == OutputFormat::Table) printMessageDetails(endpoints);
            else printMessages(endpoints, options.output);
            return action == "info" && endpoints.empty() ? kExitNotFound : 0;
        }
        auto endpoints = filterByPrefix(observer.serviceEndpoints(), options.prefix);
        if (action == "info") endpoints = filterByTopic(std::move(endpoints), options.positional[2]);
        if (action == "info" && options.output == OutputFormat::Table) printServiceDetails(endpoints);
        else printServices(endpoints, options.output);
        return action == "info" && endpoints.empty() ? kExitNotFound : 0;
    }

    if (command == "watch") {
        if (options.positional.size() != 2 ||
            (options.positional[1] != "topics" && options.positional[1] != "services")) {
            throw std::invalid_argument("watch requires topics or services");
        }
        std::signal(SIGINT, requestStop);
        std::signal(SIGTERM, requestStop);
        if (options.positional[1] == "topics") {
            return watchEndpoints<MessageEndpointInfo>(options,
                [&observer, &options] { return filterByPrefix(observer.messageEndpoints(), options.prefix); },
                [&options](const auto& endpoints, const std::string& event) {
                    printMessages(endpoints, options.output, event);
                });
        }
        return watchEndpoints<ServiceEndpointInfo>(options,
            [&observer, &options] { return filterByPrefix(observer.serviceEndpoints(), options.prefix); },
            [&options](const auto& endpoints, const std::string& event) {
                printServices(endpoints, options.output, event);
            });
    }

    throw std::invalid_argument("unknown command: " + command);
}
} // namespace

int main(int argc, char** argv)
{
    try {
        for (int index = 1; index < argc; ++index) {
            if (std::string(argv[index]) == "--help" || std::string(argv[index]) == "-h") {
                printUsage(std::cout);
                return 0;
            }
        }
        return run(parseOptions(argc, argv));
    } catch (const std::invalid_argument& error) {
        std::cerr << "rf-trans: " << error.what() << '\n';
        return kExitUsage;
    } catch (const std::exception& error) {
        std::cerr << "rf-trans: " << error.what() << '\n';
        return kExitRuntime;
    }
}
