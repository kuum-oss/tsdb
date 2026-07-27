#include "storage.hpp"
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <format>

namespace fs = std::filesystem;

class WalManager {
public:
    int fd = -1;
    uint8_t* mmap_ptr = nullptr;
    size_t mmap_size = 0;
    size_t write_offset = 0;
    std::string wal_path;
    std::shared_mutex mutex;

    static constexpr size_t CHUNK_SIZE = 16 * 1024 * 1024; // 16MB

    WalManager(const std::string& path) : wal_path(path) {}

    ~WalManager() {
        close_wal();
    }

    void open_for_write() {
        fd = open(wal_path.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd < 0) {
            throw std::runtime_error("Failed to open WAL file: " + wal_path);
        }
        
        struct stat st;
        if (fstat(fd, &st) == 0) {
            write_offset = st.st_size;
        } else {
            write_offset = 0;
        }

        grow_and_map(write_offset + CHUNK_SIZE);
    }

    void close_wal() {
        std::unique_lock lock(mutex);
        if (mmap_ptr && mmap_size > 0) {
            munmap(mmap_ptr, mmap_size);
            mmap_ptr = nullptr;
        }
        if (fd >= 0) {
            if (ftruncate(fd, write_offset) != 0) {
                // Ignore
            }
            close(fd);
            fd = -1;
        }
        mmap_size = 0;
    }

    void grow_and_map(size_t new_size) {
        if (mmap_ptr) {
            munmap(mmap_ptr, mmap_size);
        }
        if (ftruncate(fd, new_size) != 0) {
            throw std::runtime_error("Failed to truncate WAL file to size " + std::to_string(new_size));
        }
        mmap_size = new_size;
        mmap_ptr = static_cast<uint8_t*>(mmap(nullptr, mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
        if (mmap_ptr == MAP_FAILED) {
            mmap_ptr = nullptr;
            throw std::runtime_error("Failed to mmap WAL file");
        }
    }

    uint64_t append(uint64_t lsn, const std::vector<uint8_t>& data) {
        std::unique_lock lock(mutex);
        
        size_t record_len = data.size();
        size_t total_len = 8 + 4 + 4 + 1 + record_len; // 8B LSN, 4B CRC, 4B len, 1B sentinel, payload

        if (write_offset + total_len > mmap_size) {
            grow_and_map(mmap_size + CHUNK_SIZE + total_len);
        }

        std::span<uint8_t> span(mmap_ptr + write_offset, total_len);
        
        for (int i = 0; i < 8; ++i) {
            span[i] = static_cast<uint8_t>((lsn >> (i * 8)) & 0xFF);
        }

        // Write length (4 bytes starting at offset 12)
        span[12] = static_cast<uint8_t>(record_len & 0xFF);
        span[13] = static_cast<uint8_t>((record_len >> 8) & 0xFF);
        span[14] = static_cast<uint8_t>((record_len >> 16) & 0xFF);
        span[15] = static_cast<uint8_t>((record_len >> 24) & 0xFF);

        // Sentinel (1 byte at offset 16)
        span[16] = 0xAB;

        // Payload starting at offset 17
        std::memcpy(span.data() + 17, data.data(), record_len);

        // Calculate CRC starting from length (offset 12) through the end of payload
        uint32_t crc = crc32_ieee(span.data() + 12, 5 + record_len); // 4B len + 1B sentinel + payload_len

        // Write CRC at offset 8..11
        for (int i = 0; i < 4; ++i) {
            span[8 + i] = static_cast<uint8_t>((crc >> (i * 8)) & 0xFF);
        }

        write_offset += total_len;
        return lsn;
    }

    void flush_wal() {
        std::shared_lock lock(mutex);
        if (mmap_ptr && write_offset > 0) {
            msync(mmap_ptr, write_offset, MS_SYNC);
        }
    }

    void replay(const std::function<void(uint64_t, const MetricEntry&)>& cb, uint64_t& last_lsn) {
        std::unique_lock lock(mutex);
        int read_fd = open(wal_path.c_str(), O_RDONLY);
        if (read_fd < 0) return;

        struct stat st;
        if (fstat(read_fd, &st) != 0 || st.st_size == 0) {
            close(read_fd);
            return;
        }

        size_t size = st.st_size;
        void* addr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, read_fd, 0);
        if (addr == MAP_FAILED) {
            close(read_fd);
            return;
        }

        const uint8_t* ptr = static_cast<const uint8_t*>(addr);
        size_t offset = 0;

        while (offset + 17 <= size) {
            uint64_t lsn = 0;
            for (int i = 0; i < 8; ++i) {
                lsn |= (static_cast<uint64_t>(ptr[offset + i]) << (i * 8));
            }

            uint32_t expected_crc = 0;
            for (int i = 0; i < 4; ++i) {
                expected_crc |= (static_cast<uint32_t>(ptr[offset + 8 + i]) << (i * 8));
            }

            uint32_t len = ptr[offset + 12] |
                           (static_cast<uint32_t>(ptr[offset + 13]) << 8) |
                           (static_cast<uint32_t>(ptr[offset + 14]) << 16) |
                           (static_cast<uint32_t>(ptr[offset + 15]) << 24);
            uint8_t sentinel = ptr[offset + 16];

            if (offset + 17 + len > size) {
                break;
            }

            if (sentinel != 0xAB) {
                break;
            }

            uint32_t actual_crc = crc32_ieee(ptr + offset + 12, 5 + len);
            if (actual_crc != expected_crc) {
                break;
            }

            MetricEntry entry;
            if (entry.deserialize(ptr + offset + 17, len) == len) {
                cb(lsn, entry);
                last_lsn = std::max(last_lsn, lsn);
            }

            offset += 17 + len;
        }

        munmap(addr, size);
        close(read_fd);
    }
};

StorageEngine::StorageEngine(const std::string& wal_dir, uint32_t ttl_seconds) {
    fs::create_directories(wal_dir);
    wal = std::make_unique<WalManager>((fs::path(wal_dir) / "wal.bin").string());
    snapshot_path = (fs::path(wal_dir) / "snapshot.bin").string();

    load_snapshot();

    uint64_t last_lsn = 0;
    wal->replay([this](uint64_t lsn, const MetricEntry& entry) {
        insert_memory(entry);
    }, last_lsn);
    lsn_counter.store(last_lsn + 1);

    wal->open_for_write();

    retention_thread = std::jthread([this, ttl_seconds](std::stop_token stop) {
        while (!stop.stop_requested()) {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            prune_expired_data(ttl_seconds);
        }
    });
}

StorageEngine::~StorageEngine() {
    wal->close_wal();
}

void StorageEngine::insert_memory(const MetricEntry& entry) {
    std::unique_lock lock(db_mutex);
    MetricKey key{entry.name, entry.host};
    auto& ts = db[key];
    
    auto it = std::lower_bound(ts.timestamps.begin(), ts.timestamps.end(), entry.timestamp);
    auto idx = std::distance(ts.timestamps.begin(), it);
    ts.timestamps.insert(it, entry.timestamp);
    ts.values.insert(ts.values.begin() + idx, entry.value);
}

void StorageEngine::write(const MetricEntry& entry) {
    uint64_t cur_lsn = lsn_counter.fetch_add(1);
    std::vector<uint8_t> buf;
    entry.serialize(buf);
    wal->append(cur_lsn, buf);
    
    wal->flush_wal();

    insert_memory(entry);

    bool need_snap = false;
    {
        std::unique_lock lock(db_mutex);
        ops_since_snapshot++;
        if (ops_since_snapshot >= SNAPSHOT_INTERVAL) {
            ops_since_snapshot = 0;
            need_snap = true;
        }
    }
    if (need_snap) {
        trigger_async_snapshot();
    }
}

void StorageEngine::trigger_async_snapshot() {
    std::shared_lock lock(db_mutex);
    auto db_copy = db;
    uint64_t snap_lsn = lsn_counter.load() - 1;
    
    std::thread([this, db_copy = std::move(db_copy), snap_lsn]() {
        try {
            std::string temp_path = snapshot_path + ".tmp";
            std::ofstream out(temp_path, std::ios::binary);
            if (!out) return;
            
            out.write("TSBS", 4);
            out.write(reinterpret_cast<const char*>(&snap_lsn), 8);
            
            uint64_t num_series = db_copy.size();
            out.write(reinterpret_cast<const char*>(&num_series), 8);
            
            for (const auto& [key, ts] : db_copy) {
                uint16_t name_len = key.name.size();
                out.write(reinterpret_cast<const char*>(&name_len), 2);
                out.write(key.name.data(), name_len);
                
                uint16_t host_len = key.host.size();
                out.write(reinterpret_cast<const char*>(&host_len), 2);
                out.write(key.host.data(), host_len);
                
                uint64_t size = ts.timestamps.size();
                out.write(reinterpret_cast<const char*>(&size), 8);
                
                if (size > 0) {
                    out.write(reinterpret_cast<const char*>(ts.timestamps.data()), size * 8);
                    out.write(reinterpret_cast<const char*>(ts.values.data()), size * 8);
                }
            }
            out.close();
            fs::rename(temp_path, snapshot_path);
        } catch (...) {
        }
    }).detach();
}

void StorageEngine::load_snapshot() {
    if (!fs::exists(snapshot_path)) return;
    
    std::ifstream in(snapshot_path, std::ios::binary);
    if (!in) return;
    
    char magic[4];
    in.read(magic, 4);
    if (std::memcmp(magic, "TSBS", 4) != 0) return;
    
    uint64_t snap_lsn = 0;
    in.read(reinterpret_cast<char*>(&snap_lsn), 8);
    
    uint64_t num_series = 0;
    in.read(reinterpret_cast<char*>(&num_series), 8);
    
    for (uint64_t s = 0; s < num_series; ++s) {
        uint16_t name_len = 0;
        in.read(reinterpret_cast<char*>(&name_len), 2);
        std::string name(name_len, '\0');
        in.read(&name[0], name_len);
        
        uint16_t host_len = 0;
        in.read(reinterpret_cast<char*>(&host_len), 2);
        std::string host(host_len, '\0');
        in.read(&host[0], host_len);
        
        uint64_t size = 0;
        in.read(reinterpret_cast<char*>(&size), 8);
        
        MetricKey key{name, host};
        auto& ts = db[key];
        ts.timestamps.resize(size);
        ts.values.resize(size);
        
        if (size > 0) {
            in.read(reinterpret_cast<char*>(ts.timestamps.data()), size * 8);
            in.read(reinterpret_cast<char*>(ts.values.data()), size * 8);
        }
    }
    lsn_counter.store(snap_lsn + 1);
}

void StorageEngine::prune_expired_data(uint32_t ttl_seconds) {
    std::unique_lock lock(db_mutex);
    uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    uint64_t cutoff = now - ttl_seconds;

    for (auto& [key, ts] : db) {
        if (ts.timestamps.size() != ts.values.size()) {
            // Restore invariant if mismatched
            size_t min_sz = std::min(ts.timestamps.size(), ts.values.size());
            ts.timestamps.resize(min_sz);
            ts.values.resize(min_sz);
        }
        auto it = std::lower_bound(ts.timestamps.begin(), ts.timestamps.end(), cutoff);
        if (it != ts.timestamps.begin()) {
            size_t dist = std::distance(ts.timestamps.begin(), it);
            if (dist <= ts.timestamps.size() && dist <= ts.values.size()) {
                ts.timestamps.erase(ts.timestamps.begin(), it);
                ts.values.erase(ts.values.begin(), ts.values.begin() + dist);
            }
        }
    }
}

std::vector<std::pair<uint64_t, double>> StorageEngine::get_series(const std::string& name, const std::string& host, uint64_t start_time, uint64_t end_time) {
    std::shared_lock lock(db_mutex);
    std::vector<std::pair<uint64_t, double>> res;
    
    for (const auto& [key, ts] : db) {
        if (key.name == name && (host == "*" || key.host == host)) {
            for (size_t i = 0; i < ts.timestamps.size(); ++i) {
                if (ts.timestamps[i] >= start_time && ts.timestamps[i] <= end_time) {
                    res.push_back({ts.timestamps[i], ts.values[i]});
                }
            }
        }
    }
    std::sort(res.begin(), res.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });
    return res;
}
