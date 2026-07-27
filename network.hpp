#pragma once
#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <atomic>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include "storage.hpp"
#include "replication.hpp"

class NetworkServer {
private:
    int server_fd = -1;
    uint16_t port = 0;
    std::string role;
    StorageEngine& storage;
    ReplicationManager& replication;
    std::vector<std::jthread> workers;
    std::atomic<bool> running{true};

public:
    NetworkServer(uint16_t port, const std::string& role, StorageEngine& storage, ReplicationManager& replication)
        : port(port), role(role), storage(storage), replication(replication) {}

    ~NetworkServer() {
        stop();
    }

    void start();
    void stop();

private:
    void handle_connection(int client_fd);
    void run_io_uring_loop();
    void run_epoll_loop();
    void run_blocking_loop();
};

// Safe reading helper from POSIX socket
inline bool read_all(int fd, uint8_t* buf, size_t size) {
    size_t total = 0;
    while (total < size) {
        ssize_t n = read(fd, buf + total, size - total);
        if (n <= 0) return false;
        total += n;
    }
    return true;
}

// Safe writing helper to POSIX socket
inline bool write_all(int fd, const uint8_t* buf, size_t size) {
    size_t total = 0;
    while (total < size) {
        ssize_t n = write(fd, buf + total, size - total);
        if (n <= 0) return false;
        total += n;
    }
    return true;
}
