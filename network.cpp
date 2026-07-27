#include "network.hpp"
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <cstring>
#include <netdb.h>
#include "query.hpp"

#if __has_include(<liburing.h>)
#define TSDB_HAS_URING 1
#include <liburing.h>
#else
#define TSDB_HAS_URING 0
#endif

#if defined(__linux__)
#define TSDB_HAS_EPOLL 1
#include <sys/epoll.h>
#else
#define TSDB_HAS_EPOLL 0
#endif

import tsdb.protocol;

using namespace tsdb::protocol;

void NetworkServer::start() {
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        throw std::runtime_error("Failed to create socket");
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        throw std::runtime_error("Failed to bind to port " + std::to_string(port));
    }

    if (listen(server_fd, 128) < 0) {
        throw std::runtime_error("Failed to listen on port " + std::to_string(port));
    }

    std::cout << std::format("[{}] Server listening on port {}\n", role, port);

    // Run network loop in a separate thread
    workers.emplace_back([this](std::stop_token stop) {
#if TSDB_HAS_URING
        run_io_uring_loop();
#elif TSDB_HAS_EPOLL
        run_epoll_loop();
#else
        run_blocking_loop();
#endif
    });
}

void NetworkServer::stop() {
    running = false;
    if (server_fd >= 0) {
        close(server_fd);
        server_fd = -1;
    }
    workers.clear();
}

void NetworkServer::run_blocking_loop() {
    while (running) {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (!running) break;
            continue;
        }

        // Spawn a thread per connection
        std::thread([this, client_fd]() {
            handle_connection(client_fd);
        }).detach();
    }
}

#if TSDB_HAS_EPOLL
void NetworkServer::run_epoll_loop() {
    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        run_blocking_loop();
        return;
    }

    epoll_event ev{}, events[64];
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) < 0) {
        close(epoll_fd);
        run_blocking_loop();
        return;
    }

    while (running) {
        int nfds = epoll_wait(epoll_fd, events, 64, 500);
        for (int i = 0; i < nfds; ++i) {
            if (events[i].data.fd == server_fd) {
                sockaddr_in client_addr{};
                socklen_t addr_len = sizeof(client_addr);
                int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &addr_len);
                if (client_fd >= 0) {
                    std::thread([this, client_fd]() {
                        handle_connection(client_fd);
                    }).detach();
                }
            }
        }
    }
    close(epoll_fd);
}
#endif

#if TSDB_HAS_URING
void NetworkServer::run_io_uring_loop() {
    struct io_uring ring;
    if (io_uring_queue_init(128, &ring, 0) < 0) {
        run_epoll_loop();
        return;
    }

    // Submit accept request
    struct sockaddr_in client_addr{};
    socklen_t addr_len = sizeof(client_addr);
    
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    io_uring_prep_accept(sqe, server_fd, (struct sockaddr*)&client_addr, &addr_len, 0);
    io_uring_submit(&ring);

    while (running) {
        struct io_uring_cqe* cqe;
        int ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret < 0) continue;

        if (cqe->res >= 0) {
            int client_fd = cqe->res;
            std::thread([this, client_fd]() {
                handle_connection(client_fd);
            }).detach();
        }

        io_uring_cqe_seen(&ring, cqe);

        // Submit next accept
        sqe = io_uring_get_sqe(&ring);
        io_uring_prep_accept(sqe, server_fd, (struct sockaddr*)&client_addr, &addr_len, 0);
        io_uring_submit(&ring);
    }
    io_uring_queue_exit(&ring);
}
#endif

void NetworkServer::handle_connection(int client_fd) {
    while (running) {
        uint8_t header[10];
        if (!read_all(client_fd, header, 10)) {
            break;
        }

        uint32_t magic = (static_cast<uint32_t>(header[0]) << 24) |
                         (static_cast<uint32_t>(header[1]) << 16) |
                         (static_cast<uint32_t>(header[2]) << 8)  |
                         static_cast<uint32_t>(header[3]);
        
        if (magic != MAGIC) {
            break;
        }

        MsgType type = static_cast<MsgType>(header[5]);
        uint32_t payload_len = (static_cast<uint32_t>(header[6]) << 24) |
                               (static_cast<uint32_t>(header[7]) << 16) |
                               (static_cast<uint32_t>(header[8]) << 8)  |
                               static_cast<uint32_t>(header[9]);

        std::vector<uint8_t> payload(payload_len);
        if (payload_len > 0 && !read_all(client_fd, payload.data(), payload_len)) {
            break;
        }

        if (type == MsgType::WRITE) {
            if (role != "primary") {
                // Replicas are read-only
                std::string err = "Error: WRITE requests are only allowed on Primary node";
                Frame resp{MsgType::ACK, std::vector<uint8_t>(err.begin(), err.end())};
                auto resp_buf = serialize_frame(resp);
                write_all(client_fd, resp_buf.data(), resp_buf.size());
                continue;
            }

            MetricEntry entry;
            if (entry.deserialize(payload.data(), payload.size()) > 0) {
                // Storage engine write (coroutine)
                auto write_task = [this, entry]() -> Task<void> {
                    co_await storage.write(entry);
                };
                // Resolve coroutine synchronously for network request flow
                auto run_coro = [](Task<void> t) {
                    auto handle = t.handle;
                    while (!handle.done()) {
                        handle.resume();
                    }
                };
                run_coro(write_task());

                // Add to replication stream
                uint64_t current_lsn = storage.get_lsn() - 1;
                replication.add_entry(current_lsn, entry);

                // ACK response
                std::vector<uint8_t> ack_payload(8);
                for (int i = 0; i < 8; ++i) {
                    ack_payload[i] = static_cast<uint8_t>((current_lsn >> (i * 8)) & 0xFF);
                }
                Frame resp{MsgType::ACK, ack_payload};
                auto resp_buf = serialize_frame(resp);
                write_all(client_fd, resp_buf.data(), resp_buf.size());
            }
        } 
        else if (type == MsgType::QUERY) {
            std::string query_str(reinterpret_cast<char*>(payload.data()), payload.size());
            auto parse_res = QueryParser::parse(query_str);

            std::string resp_str;
            if (std::holds_alternative<ParseError>(parse_res)) {
                resp_str = "Error: " + std::get<ParseError>(parse_res).message;
            } else {
                const auto& ast = std::get<QueryAST>(parse_res);
                uint64_t start_time = ast.start_time;
                uint64_t end_time = ast.end_time;
                if (ast.is_relative) {
                    uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::system_clock::now().time_since_epoch()
                    ).count();
                    start_time = now - ast.last_seconds;
                    end_time = now;
                }

                auto raw_series = storage.get_series(ast.metric, ast.host_filter, start_time, end_time);
                
                if (raw_series.empty()) {
                    resp_str = "No data found for: " + ast.metric;
                } else {
                    // Extract values for aggregations
                    std::vector<double> vals;
                    for (const auto& p : raw_series) vals.push_back(p.second);

                    if (ast.step_seconds > 0) {
                        // Downsampling
                        std::vector<std::pair<uint64_t, double>> binned;
                        for (uint64_t bucket = start_time; bucket < end_time; bucket += ast.step_seconds) {
                            std::vector<double> bucket_vals;
                            for (const auto& p : raw_series) {
                                if (p.first >= bucket && p.first < bucket + ast.step_seconds) {
                                    bucket_vals.push_back(p.second);
                                }
                            }
                            if (!bucket_vals.empty()) {
                                double agg_val = 0;
                                if (ast.agg.type == AggregationType::AVG) agg_val = aggregate_avg(bucket_vals);
                                else if (ast.agg.type == AggregationType::MAX) agg_val = aggregate_max(bucket_vals);
                                else if (ast.agg.type == AggregationType::MIN) agg_val = aggregate_min(bucket_vals);
                                else if (ast.agg.type == AggregationType::RATE) agg_val = aggregate_rate(bucket_vals);
                                else if (ast.agg.type == AggregationType::PERCENTILE) agg_val = aggregate_percentile(bucket_vals, ast.agg.percentile_val);
                                binned.push_back({bucket, agg_val});
                            }
                        }
                        std::stringstream ss;
                        for (const auto& p : binned) {
                            ss << p.first << ": " << p.second << "\n";
                        }
                        resp_str = ss.str();
                    } else {
                        // Full aggregation
                        double val = 0;
                        if (ast.agg.type == AggregationType::AVG) val = aggregate_avg(vals);
                        else if (ast.agg.type == AggregationType::MAX) val = aggregate_max(vals);
                        else if (ast.agg.type == AggregationType::MIN) val = aggregate_min(vals);
                        else if (ast.agg.type == AggregationType::RATE) val = aggregate_rate(vals);
                        else if (ast.agg.type == AggregationType::PERCENTILE) val = aggregate_percentile(vals, ast.agg.percentile_val);
                        resp_str = std::format("Result: {}\n", val);
                    }
                }
            }

            Frame resp{MsgType::ACK, std::vector<uint8_t>(resp_str.begin(), resp_str.end())};
            auto resp_buf = serialize_frame(resp);
            write_all(client_fd, resp_buf.data(), resp_buf.size());
        }
        else if (type == MsgType::REPLICATE) {
            // Replication request from replica
            if (role != "primary") {
                break;
            }
            if (payload.size() < 8) break;

            uint64_t last_acked_lsn = 0;
            for (int i = 0; i < 8; ++i) {
                last_acked_lsn |= (static_cast<uint64_t>(payload[i]) << (i * 8));
            }

            std::cout << "[Primary] Replica connected. Streaming from LSN " << last_acked_lsn << "\n";

            // Loop and stream logs
            std::stop_source stop_src;
            std::jthread sender([this, client_fd, last_acked_lsn, stop_token = stop_src.get_token()]() mutable {
                while (!stop_token.stop_requested()) {
                    auto entries = replication.get_entries_from(last_acked_lsn);
                    for (const auto& item : entries) {
                        if (item.lsn < last_acked_lsn) continue;

                        std::vector<uint8_t> payload_buf;
                        // LSN (8B)
                        for (int i = 0; i < 8; ++i) {
                            payload_buf.push_back(static_cast<uint8_t>((item.lsn >> (i * 8)) & 0xFF));
                        }
                        // Serialize entry
                        item.entry.serialize(payload_buf);

                        Frame stream_frame{MsgType::REPLICATE, payload_buf};
                        auto out_buf = serialize_frame(stream_frame);
                        if (!write_all(client_fd, out_buf.data(), out_buf.size())) {
                            return;
                        }

                        // Wait for ACK
                        uint8_t ack_hdr[10];
                        if (!read_all(client_fd, ack_hdr, 10)) {
                            return;
                        }
                        uint32_t payload_len = (static_cast<uint32_t>(ack_hdr[6]) << 24) |
                                               (static_cast<uint32_t>(ack_hdr[7]) << 16) |
                                               (static_cast<uint32_t>(ack_hdr[8]) << 8)  |
                                               static_cast<uint32_t>(ack_hdr[9]);
                        std::vector<uint8_t> ack_payload(payload_len);
                        if (payload_len > 0 && !read_all(client_fd, ack_payload.data(), payload_len)) {
                            return;
                        }
                        
                        uint64_t ack_lsn = 0;
                        if (ack_payload.size() >= 8) {
                            for (int i = 0; i < 8; ++i) {
                                ack_lsn |= (static_cast<uint64_t>(ack_payload[i]) << (i * 8));
                            }
                        }
                        replication.trim_log(ack_lsn);
                        last_acked_lsn = ack_lsn + 1;
                    }
                    // Wait for new entries
                    replication.wait_for_changes(last_acked_lsn, stop_token);
                }
            });
            break; // Finished handling client_fd on this thread, delegated to sender
        }
        else if (type == MsgType::HEARTBEAT) {
            Frame resp{MsgType::HEARTBEAT, {}};
            auto resp_buf = serialize_frame(resp);
            write_all(client_fd, resp_buf.data(), resp_buf.size());
        }
    }
    close(client_fd);
}
