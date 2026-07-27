#pragma once
#include <string>
#include <vector>
#include <deque>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <condition_variable>
#include "storage.hpp"

#include <unordered_map>

struct ReplicaEntry {
    uint64_t lsn;
    MetricEntry entry;
};

class ReplicationManager {
private:
    std::deque<ReplicaEntry> replication_log;
    mutable std::shared_mutex log_mutex;
    std::condition_variable_any log_cv;
    std::atomic<uint64_t> last_lsn{0};
    
    // Tracks fd -> last acknowledged LSN
    std::unordered_map<int, uint64_t> replica_acks;

public:
    void add_entry(uint64_t lsn, const MetricEntry& entry) {
        std::unique_lock lock(log_mutex);
        replication_log.push_back({lsn, entry});
        last_lsn.store(lsn);
        log_cv.notify_all();
    }

    // Get replication entries starting from a given LSN
    std::vector<ReplicaEntry> get_entries_from(uint64_t lsn) const {
        std::shared_lock lock(log_mutex);
        std::vector<ReplicaEntry> res;
        for (const auto& item : replication_log) {
            if (item.lsn >= lsn) {
                res.push_back(item);
            }
        }
        return res;
    }

    // Wait until there's new entries after standard LSN or stop token is triggered
    bool wait_for_changes(uint64_t current_lsn, std::stop_token stop) {
        std::shared_lock lock(log_mutex);
        return log_cv.wait_for(lock, stop, std::chrono::milliseconds(500), [this, current_lsn]() {
            return last_lsn.load() >= current_lsn;
        });
    }

    uint64_t get_last_lsn() const {
        return last_lsn.load();
    }

    void register_replica(int fd, uint64_t initial_lsn) {
        std::unique_lock lock(log_mutex);
        replica_acks[fd] = initial_lsn;
    }

    void unregister_replica(int fd) {
        std::unique_lock lock(log_mutex);
        replica_acks.erase(fd);
        trim_to_min();
    }

    void update_ack(int fd, uint64_t ack_lsn) {
        std::unique_lock lock(log_mutex);
        replica_acks[fd] = ack_lsn;
        trim_to_min();
    }

private:
    void trim_to_min() {
        if (replica_acks.empty()) {
            return;
        }
        uint64_t min_lsn = std::numeric_limits<uint64_t>::max();
        for (const auto& [fd, ack] : replica_acks) {
            if (ack < min_lsn) {
                min_lsn = ack;
            }
        }
        while (!replication_log.empty() && replication_log.front().lsn <= min_lsn) {
            replication_log.pop_front();
        }
    }
};
