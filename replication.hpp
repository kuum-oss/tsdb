#pragma once
#include <string>
#include <vector>
#include <deque>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <condition_variable>
#include "storage.hpp"

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

    void trim_log(uint64_t ack_lsn) {
        std::unique_lock lock(log_mutex);
        while (!replication_log.empty() && replication_log.front().lsn <= ack_lsn) {
            replication_log.pop_front();
        }
    }
};
