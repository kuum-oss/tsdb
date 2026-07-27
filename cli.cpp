#include <iostream>
#include <string>
#include <vector>
#include <semaphore>
#include <mutex>
#include <cstring>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sstream>
#include <memory>
#include <chrono>
#include <random>
#include <array>
#include "storage.hpp"

import tsdb.protocol;

using namespace tsdb::protocol;

class ConnectionPool {
private:
    std::string host;
    uint16_t port;
    std::vector<int> pool;
    std::mutex pool_mutex;
    std::counting_semaphore<32> sem;

public:
    ConnectionPool(const std::string& host, uint16_t port)
        : host(host), port(port), sem(8) {} // Pool capacity of 8 connections

    ~ConnectionPool() {
        for (int fd : pool) {
            close(fd);
        }
    }

    int acquire() {
        sem.acquire();
        std::unique_lock lock(pool_mutex);
        if (!pool.empty()) {
            int fd = pool.back();
            pool.pop_back();
            return fd;
        }

        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return -1;

        struct hostent* he = gethostbyname(host.c_str());
        if (!he) {
            close(fd);
            return -1;
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        std::memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);

        if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            close(fd);
            return -1;
        }
        return fd;
    }

    void release(int fd) {
        if (fd < 0) {
            sem.release();
            return;
        }
        std::unique_lock lock(pool_mutex);
        pool.push_back(fd);
        sem.release();
    }
};

struct TargetNode {
    std::string host;
    uint16_t port;
};

int main() {
    std::string primary_env = "localhost:7700";
    if (const char* p = std::getenv("TSDB_PRIMARY")) {
        primary_env = p;
    }

    std::string replicas_env;
    if (const char* r = std::getenv("TSDB_REPLICAS")) {
        replicas_env = r;
    }

    auto parse_node = [](const std::string& s) -> TargetNode {
        size_t colon = s.find(':');
        if (colon != std::string::npos) {
            return {s.substr(0, colon), static_cast<uint16_t>(std::stoi(s.substr(colon + 1)))};
        }
        return {s, 7700};
    };

    TargetNode primary = parse_node(primary_env);
    std::vector<TargetNode> replicas;

    if (!replicas_env.empty()) {
        std::stringstream ss(replicas_env);
        std::string token;
        while (std::getline(ss, token, ',')) {
            replicas.push_back(parse_node(token));
        }
    }

    auto primary_pool = std::make_unique<ConnectionPool>(primary.host, primary.port);
    std::vector<std::unique_ptr<ConnectionPool>> replica_pools;
    for (const auto& r : replicas) {
        replica_pools.push_back(std::make_unique<ConnectionPool>(r.host, r.port));
    }

    std::random_device rd;
    std::mt19937 gen(rd());

    std::cout << "=== TSDB Interactive CLI ===\n";
    std::cout << "Commands:\n";
    std::cout << "  WRITE <metric> <host> <value>\n";
    std::cout << "  QUERY <query_string>\n";
    std::cout << "  EXIT\n\n";

    std::string line;
    while (std::cout << "> " && std::getline(std::cin, line)) {
        if (line == "EXIT" || line == "exit") {
            break;
        }

        std::stringstream ss(line);
        std::string cmd;
        ss >> cmd;

        if (cmd == "WRITE") {
            std::string name, host;
            double value;
            if (!(ss >> name >> host >> value)) {
                std::cout << "Usage: WRITE <metric> <host> <value>\n";
                continue;
            }

            uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch()
            ).count();

            MetricEntry entry{name, host, now, value};
            std::vector<uint8_t> payload;
            entry.serialize(payload);

            Frame frame{MsgType::WRITE, payload};
            auto frame_buf = serialize_frame(frame);

            int fd = primary_pool->acquire();
            if (fd < 0) {
                std::cout << "Error: Failed to connect to Primary node.\n";
                continue;
            }

            // Write all
            size_t total = 0;
            bool ok = true;
            while (total < frame_buf.size()) {
                ssize_t n = write(fd, frame_buf.data() + total, frame_buf.size() - total);
                if (n <= 0) {
                    ok = false;
                    break;
                }
                total += n;
            }

            if (!ok) {
                std::cout << "Error: Write failed.\n";
                primary_pool->release(-1);
                continue;
            }

            // Read response ACK
            uint8_t ack_header[10];
            total = 0;
            while (total < 10) {
                ssize_t n = read(fd, ack_header + total, 10 - total);
                if (n <= 0) {
                    ok = false;
                    break;
                }
                total += n;
            }

            if (!ok) {
                std::cout << "Error: Failed to read ACK.\n";
                primary_pool->release(-1);
                continue;
            }

            uint32_t payload_len = (static_cast<uint32_t>(ack_header[6]) << 24) |
                                   (static_cast<uint32_t>(ack_header[7]) << 16) |
                                   (static_cast<uint32_t>(ack_header[8]) << 8)  |
                                   static_cast<uint32_t>(ack_header[9]);

            std::vector<uint8_t> ack_payload(payload_len);
            if (payload_len > 0) {
                total = 0;
                while (total < payload_len) {
                    ssize_t n = read(fd, ack_payload.data() + total, payload_len - total);
                    if (n <= 0) {
                        ok = false;
                        break;
                    }
                    total += n;
                }
            }

            if (!ok) {
                std::cout << "Error: Failed to read ACK payload.\n";
                primary_pool->release(-1);
                continue;
            }

            if (static_cast<MsgType>(ack_header[5]) == MsgType::ACK) {
                if (payload_len == 8) {
                    uint64_t lsn = 0;
                    for (int i = 0; i < 8; ++i) {
                        lsn |= (static_cast<uint64_t>(ack_payload[i]) << (i * 8));
                    }
                    std::cout << "Write Success. LSN: " << lsn << "\n";
                } else {
                    std::string msg(ack_payload.begin(), ack_payload.end());
                    std::cout << msg << "\n";
                }
            } else {
                std::cout << "Error: Received unexpected message type.\n";
            }

            primary_pool->release(fd);

        } else if (cmd == "QUERY") {
            std::string query_str;
            std::getline(ss, query_str);
            if (!query_str.empty() && query_str.front() == ' ') {
                query_str = query_str.substr(1);
            }

            if (query_str.empty()) {
                std::cout << "Usage: QUERY <query_string>\n";
                continue;
            }

            Frame frame{MsgType::QUERY, std::vector<uint8_t>(query_str.begin(), query_str.end())};
            auto frame_buf = serialize_frame(frame);

            // Connect to either replica (read-scale) or primary if no replicas configured
            ConnectionPool* pool = primary_pool.get();
            if (!replica_pools.empty()) {
                std::uniform_int_distribution<size_t> dist(0, replica_pools.size() - 1);
                pool = replica_pools[dist(gen)].get();
            }

            int fd = pool->acquire();
            if (fd < 0) {
                std::cout << "Error: Failed to connect to node.\n";
                continue;
            }

            size_t total = 0;
            bool ok = true;
            while (total < frame_buf.size()) {
                ssize_t n = write(fd, frame_buf.data() + total, frame_buf.size() - total);
                if (n <= 0) {
                    ok = false;
                    break;
                }
                total += n;
            }

            if (!ok) {
                std::cout << "Error: Query request failed.\n";
                pool->release(-1);
                continue;
            }

            uint8_t resp_header[10];
            total = 0;
            while (total < 10) {
                ssize_t n = read(fd, resp_header + total, 10 - total);
                if (n <= 0) {
                    ok = false;
                    break;
                }
                total += n;
            }

            if (!ok) {
                std::cout << "Error: Failed to read response header.\n";
                pool->release(-1);
                continue;
            }

            uint32_t payload_len = (static_cast<uint32_t>(resp_header[6]) << 24) |
                                   (static_cast<uint32_t>(resp_header[7]) << 16) |
                                   (static_cast<uint32_t>(resp_header[8]) << 8)  |
                                   static_cast<uint32_t>(resp_header[9]);

            std::vector<uint8_t> resp_payload(payload_len);
            if (payload_len > 0) {
                total = 0;
                while (total < payload_len) {
                    ssize_t n = read(fd, resp_payload.data() + total, payload_len - total);
                    if (n <= 0) {
                        ok = false;
                        break;
                    }
                    total += n;
                }
            }

            if (!ok) {
                std::cout << "Error: Failed to read response payload.\n";
                pool->release(-1);
                continue;
            }

            std::string result(resp_payload.begin(), resp_payload.end());
            std::cout << result << "\n";

            pool->release(fd);
        } else {
            std::cout << "Unknown command: " << cmd << "\n";
        }
    }

    return 0;
}
