#pragma once 

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>

inline uint32_t calculate_crc(const void* data, size_t length) {
    static std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> t{};
        const uint32_t polynomial = 0xEDB88320u;
        for(uint32_t i = 0; i < 256; i++) {
            uint32_t c = i;
            for(int j = 0; j < 8; j++) {
                if(c & 1) {
                    c = polynomial ^ (c >> 1);
                } else {
                    c = c >> 1;
                }
            }
            t[i] = c;
        }
        return t;
    }();

    const unsigned char* bytes = static_cast<const unsigned char*>(data);

    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < length; i++) {
        uint32_t lookup_index = (crc ^ bytes[i]) & 0xFF;
        crc = (crc >> 8) ^ table[lookup_index];
    }
    return ~crc;
}

inline uint32_t get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
    return static_cast<uint32_t>(seconds);
}