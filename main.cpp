#include <iostream>
#include <string>
#include <latch>
#include <thread>
#include <memory>
#include <cstring>
#include <array>
#include <netdb.h>
#include "storage.hpp"
#include "replication.hpp"
#include "network.hpp"

import tsdb.protocol;

using namespace tsdb::protocol;

void run_replica_client(const std::string& primary_host, uint16_t primary_port, StorageEngine& storage);

int main(int argc, char* argv[]) {
    std::string role = "primary";
    uint16_t port = 7700;
    std::string wal_dir = "/data/wal";
    std::string primary_addr;
    uint32_t ttl = 3600; // default 1 hour

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--role" && i + 1 < argc) {
            role = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--wal-dir" && i + 1 < argc) {
            wal_dir = argv[++i];
        } else if (arg == "--primary" && i + 1 < argc) {
            primary_addr = argv[++i];
        } else if (arg == "--ttl" && i + 1 < argc) {
            ttl = static_cast<uint32_t>(std::stoi(argv[++i]));
        }
    }

    std::cout << std::format("[System] Starting TSDB node as {} on port {}\n", role, port);

    std::unique_ptr<StorageEngine> storage;
    std::unique_ptr<ReplicationManager> replication = std::make_unique<ReplicationManager>();

    std::cout << "[System] Restoring from snapshot and replaying WAL...\n";
    storage = std::make_unique<StorageEngine>(wal_dir, ttl);
    std::cout << "[System] Database initialization complete. Last LSN: " << (storage->get_lsn() > 0 ? storage->get_lsn() - 1 : 0) << "\n";

    std::cout << "[System] Starting network listener...\n";
    NetworkServer server(port, role, *storage, *replication);
    server.start();

    // If replica, start replication worker
    std::jthread replica_client_thread;
    if (role == "replica" && !primary_addr.empty()) {
        size_t colon = primary_addr.find(':');
        if (colon != std::string::npos) {
            std::string host = primary_addr.substr(0, colon);
            uint16_t primary_port = static_cast<uint16_t>(std::stoi(primary_addr.substr(colon + 1)));
            replica_client_thread = std::jthread([host, primary_port, &storage]() {
                run_replica_client(host, primary_port, *storage);
            });
        }
    }

    // Keep running
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    return 0;
}

void run_replica_client(const std::string& primary_host, uint16_t primary_port, StorageEngine& storage) {
    while (true) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
        }

        struct hostent* he = gethostbyname(primary_host.c_str());
        if (!he) {
            close(fd);
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
        }

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(primary_port);
        std::memcpy(&addr.sin_addr, he->h_addr_list[0], he->h_length);

        if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            close(fd);
            std::this_thread::sleep_for(std::chrono::seconds(2));
            continue;
        }

        std::cout << "[Replica] Connected to Primary replication stream\n";

        uint64_t last_lsn = storage.get_lsn();
        if (last_lsn > 0) {
            last_lsn--; // Start from the next LSN
        }

        std::vector<uint8_t> payload(8);
        for (int i = 0; i < 8; ++i) {
            payload[i] = static_cast<uint8_t>((last_lsn >> (i * 8)) & 0xFF);
        }

        Frame req{MsgType::REPLICATE, payload};
        auto req_buf = serialize_frame(req);
        if (!write_all(fd, req_buf.data(), req_buf.size())) {
            close(fd);
            continue;
        }

        while (true) {
            uint8_t header[10];
            if (!read_all(fd, header, 10)) {
                break;
            }

            MsgType type = static_cast<MsgType>(header[5]);
            uint32_t payload_len = (static_cast<uint32_t>(header[6]) << 24) |
                                   (static_cast<uint32_t>(header[7]) << 16) |
                                   (static_cast<uint32_t>(header[8]) << 8)  |
                                   static_cast<uint32_t>(header[9]);

            std::vector<uint8_t> stream_payload(payload_len);
            if (payload_len > 0 && !read_all(fd, stream_payload.data(), payload_len)) {
                break;
            }

            if (type == MsgType::REPLICATE) {
                if (stream_payload.size() < 8) break;
                uint64_t entry_lsn = 0;
                for (int i = 0; i < 8; ++i) {
                    entry_lsn |= (static_cast<uint64_t>(stream_payload[i]) << (i * 8));
                }

                MetricEntry entry;
                if (entry.deserialize(stream_payload.data() + 8, stream_payload.size() - 8) > 0) {
                    // Write locally
                    auto write_task = [&storage, entry]() -> Task<void> {
                        co_await storage.write(entry);
                    };
                    auto run_coro = [](Task<void> t) {
                        auto handle = t.handle;
                        while (!handle.done()) {
                            handle.resume();
                        }
                    };
                    run_coro(write_task());
                }

                // Send ACK back
                std::vector<uint8_t> ack_payload(8);
                for (int i = 0; i < 8; ++i) {
                    ack_payload[i] = static_cast<uint8_t>((entry_lsn >> (i * 8)) & 0xFF);
                }
                Frame ack{MsgType::ACK, ack_payload};
                auto ack_buf = serialize_frame(ack);
                if (!write_all(fd, ack_buf.data(), ack_buf.size())) {
                    break;
                }
            }
        }
        close(fd);
        std::this_thread::sleep_for(std::chrono::seconds(2));
    }
}