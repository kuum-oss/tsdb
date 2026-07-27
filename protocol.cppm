module;
#include <vector>
#include <cstdint>
#include <optional>
#include <array>
#include <format>

export module tsdb.protocol;

export namespace tsdb::protocol {
    export constexpr uint32_t MAGIC = 0x54534442; // 'TSDB'
    export constexpr uint8_t VERSION = 1;

    export enum class MsgType : uint8_t {
        WRITE = 1,
        QUERY = 2,
        REPLICATE = 3,
        ACK = 4,
        HEARTBEAT = 5
    };

    export struct Frame {
        MsgType type;
        std::vector<uint8_t> payload;
    };

    export std::vector<uint8_t> serialize_frame(const Frame& frame) {
        std::vector<uint8_t> buf;
        buf.reserve(10 + frame.payload.size());
        
        // 4B Magic
        buf.push_back(static_cast<uint8_t>((MAGIC >> 24) & 0xFF));
        buf.push_back(static_cast<uint8_t>((MAGIC >> 16) & 0xFF));
        buf.push_back(static_cast<uint8_t>((MAGIC >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(MAGIC & 0xFF));
        
        // 1B Version
        buf.push_back(VERSION);
        
        // 1B Type
        buf.push_back(static_cast<uint8_t>(frame.type));
        
        // 4B Payload length
        uint32_t len = static_cast<uint32_t>(frame.payload.size());
        buf.push_back(static_cast<uint8_t>((len >> 24) & 0xFF));
        buf.push_back(static_cast<uint8_t>((len >> 16) & 0xFF));
        buf.push_back(static_cast<uint8_t>((len >> 8) & 0xFF));
        buf.push_back(static_cast<uint8_t>(len & 0xFF));
        
        // Payload
        buf.insert(buf.end(), frame.payload.begin(), frame.payload.end());
        return buf;
    }

    export struct ParseResult {
        std::optional<Frame> frame;
        size_t bytes_consumed;
    };

    export ParseResult parse_frame(const uint8_t* data, size_t size) {
        if (size < 10) {
            return {std::nullopt, 0};
        }
        
        uint32_t magic = (static_cast<uint32_t>(data[0]) << 24) |
                         (static_cast<uint32_t>(data[1]) << 16) |
                         (static_cast<uint32_t>(data[2]) << 8)  |
                         static_cast<uint32_t>(data[3]);
                         
        if (magic != MAGIC) {
            return {std::nullopt, 1}; // Bad magic
        }
        
        uint8_t version = data[4];
        if (version != VERSION) {
            return {std::nullopt, 1}; // Bad version
        }
        
        MsgType type = static_cast<MsgType>(data[5]);
        
        uint32_t payload_len = (static_cast<uint32_t>(data[6]) << 24) |
                               (static_cast<uint32_t>(data[7]) << 16) |
                               (static_cast<uint32_t>(data[8]) << 8)  |
                               static_cast<uint32_t>(data[9]);
                               
        if (size < 10 + payload_len) {
            return {std::nullopt, 0}; // Need more data
        }
        
        Frame frame;
        frame.type = type;
        frame.payload.assign(data + 10, data + 10 + payload_len);
        
        return {frame, 10 + payload_len};
    }
}
