#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include <concepts>
#include <memory>
#include <shared_mutex>
#include <atomic>
#include <future>
#include <chrono>
#include <span>
#include <string_view>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <iostream>
#include <array>
#include "coro.hpp"
#include "concepts.hpp"

// Aligned Allocator
template <typename T, size_t Alignment = 64>
struct AlignedAllocator {
    using value_type = T;

    AlignedAllocator() noexcept = default;
    template <typename U> AlignedAllocator(const AlignedAllocator<U, Alignment>&) noexcept {}

    template <typename U>
    struct rebind {
        using other = AlignedAllocator<U, Alignment>;
    };

    T* allocate(std::size_t n) {
        if (n == 0) return nullptr;
        size_t size = n * sizeof(T);
        if (size % Alignment != 0) {
            size = ((size / Alignment) + 1) * Alignment;
        }
#if defined(_MSC_VER)
        T* p = static_cast<T*>(_aligned_malloc(size, Alignment));
#else
        T* p = static_cast<T*>(std::aligned_alloc(Alignment, size));
#endif
        if (!p) throw std::bad_alloc();
        return p;
    }

    void deallocate(T* p, std::size_t) noexcept {
#if defined(_MSC_VER)
        _aligned_free(p);
#else
        std::free(p);
#endif
    }

    bool operator==(const AlignedAllocator&) const noexcept { return true; }
    bool operator!=(const AlignedAllocator&) const noexcept { return false; }
};

// Metric record representation
struct MetricEntry {
    std::string name;
    std::string host;
    uint64_t timestamp; // unix timestamp in ms or seconds (we'll use seconds or ms consistently)
    double value;

    void serialize(std::vector<uint8_t>& buf) const {
        uint16_t name_len = static_cast<uint16_t>(name.size());
        uint16_t host_len = static_cast<uint16_t>(host.size());
        
        buf.push_back(static_cast<uint8_t>(name_len & 0xFF));
        buf.push_back(static_cast<uint8_t>((name_len >> 8) & 0xFF));
        
        buf.insert(buf.end(), name.begin(), name.end());
        
        buf.push_back(static_cast<uint8_t>(host_len & 0xFF));
        buf.push_back(static_cast<uint8_t>((host_len >> 8) & 0xFF));
        
        buf.insert(buf.end(), host.begin(), host.end());
        
        // timestamp (8B)
        for (int i = 0; i < 8; ++i) {
            buf.push_back(static_cast<uint8_t>((timestamp >> (i * 8)) & 0xFF));
        }
        
        // value (8B)
        uint64_t val_bits;
        std::memcpy(&val_bits, &value, sizeof(double));
        for (int i = 0; i < 8; ++i) {
            buf.push_back(static_cast<uint8_t>((val_bits >> (i * 8)) & 0xFF));
        }
    }

    size_t deserialize(const uint8_t* ptr, size_t size) {
        if (size < 4) return 0;
        size_t offset = 0;
        
        uint16_t name_len = ptr[offset] | (static_cast<uint16_t>(ptr[offset + 1]) << 8);
        offset += 2;
        if (offset + name_len > size) return 0;
        name.assign(reinterpret_cast<const char*>(ptr + offset), name_len);
        offset += name_len;
        
        if (offset + 2 > size) return 0;
        uint16_t host_len = ptr[offset] | (static_cast<uint16_t>(ptr[offset + 1]) << 8);
        offset += 2;
        if (offset + host_len > size) return 0;
        host.assign(reinterpret_cast<const char*>(ptr + offset), host_len);
        offset += host_len;
        
        if (offset + 16 > size) return 0;
        
        timestamp = 0;
        for (int i = 0; i < 8; ++i) {
            timestamp |= (static_cast<uint64_t>(ptr[offset + i]) << (i * 8));
        }
        offset += 8;
        
        uint64_t val_bits = 0;
        for (int i = 0; i < 8; ++i) {
            val_bits |= (static_cast<uint64_t>(ptr[offset + i]) << (i * 8));
        }
        std::memcpy(&value, &val_bits, sizeof(double));
        offset += 8;
        
        return offset;
    }
};

static_assert(Serializable<MetricEntry>);

inline uint32_t crc32_ieee(const uint8_t* data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc ^= data[i];
        for (int j = 0; j < 8; ++j) {
            if (crc & 1) crc = (crc >> 1) ^ 0xEDB88320;
            else crc >>= 1;
        }
    }
    return ~crc;
}

// In-memory columnar storage
struct TimeSeries {
    std::vector<uint64_t, AlignedAllocator<uint64_t, 64>> timestamps;
    std::vector<double, AlignedAllocator<double, 64>> values;
};

struct MetricKey {
    std::string name;
    std::string host;
    bool operator==(const MetricKey&) const = default;
};

struct MetricKeyHash {
    size_t operator()(const MetricKey& k) const {
        return std::hash<std::string>{}(k.name) ^ (std::hash<std::string>{}(k.host) << 1);
    }
};

class WalManager;

class StorageEngine {
private:
    std::unordered_map<MetricKey, TimeSeries, MetricKeyHash> db;
    std::shared_mutex db_mutex;
    std::atomic<uint64_t> lsn_counter{0};
    std::unique_ptr<WalManager> wal;
    std::string snapshot_path;
    uint32_t ops_since_snapshot = 0;
    static constexpr uint32_t SNAPSHOT_INTERVAL = 10000;

    std::jthread retention_thread;
    std::jthread snapshot_scheduler_thread;

public:
    StorageEngine(const std::string& wal_dir, uint32_t ttl_seconds);
    ~StorageEngine();

    uint64_t get_lsn() const {
        return lsn_counter.load();
    }

    void insert_memory(const MetricEntry& entry);
    Task<void> write(const MetricEntry& entry);
    void trigger_async_snapshot();
    void load_snapshot();
    void prune_expired_data(uint32_t ttl_seconds);
    std::vector<std::pair<uint64_t, double>> get_series(const std::string& name, const std::string& host, uint64_t start_time, uint64_t end_time);
};
