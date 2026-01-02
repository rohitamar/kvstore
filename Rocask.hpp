#pragma once 

#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "crc.hpp"
#include "utils.hpp"

namespace fs = std::filesystem;

const uint32_t MAX_FILE_SIZE = 8 * 1024 * 1024;

struct KeyDirEntry {
    uint32_t file_id;
    uint64_t value_size;
    uint64_t value_pos;
    uint32_t timestamp;
};

class Rocask {
    public:
    template<typename K, typename V>
    void write(const K& key, const V& value) {
        std::string raw_key = serialize<K>(key);
        std::string raw_value = serialize<V>(value);
        raw_write(raw_key, raw_value);
    }

    template<typename K, typename V>
    V read(const K& key) {
        std::string raw_key = deserialize<K>(key);

        std::string raw_value = raw_read(raw_key);       
        
        V value = deserialize<V>(raw_value);
        return value;
    }

    private:
    std::unordered_map<std::string, KeyDirEntry> _keydir;
    std::vector<std::string> files;

    void raw_write(std::string key, std::string value);
    std::string raw_read(std::string key);
};

void Rocask::raw_write(std::string key, std::string value) {
    uint32_t timestamp = get_timestamp();
    
    uint64_t key_size = static_cast<uint64_t>(key.size());
    uint64_t value_size = static_cast<uint64_t>(value.size());

    uint64_t before_value_size = sizeof(timestamp) + sizeof(key_size) + sizeof(value_size) + key_size;
    uint64_t buffer_size = before_value_size + value_size; 
    
    std::vector<char> buffer(buffer_size);
    char* buffer_ptr = buffer.data();
    
    std::memcpy(buffer_ptr, &timestamp, sizeof(timestamp));
    buffer_ptr += sizeof(timestamp);
    
    std::memcpy(buffer_ptr, &key_size, sizeof(key_size));
    buffer_ptr += sizeof(key_size);

    std::memcpy(buffer_ptr, &value_size, sizeof(value_size));
    buffer_ptr += sizeof(value_size);

    std::memcpy(buffer_ptr, key.data(), key_size);
    buffer_ptr += key_size;

    std::memcpy(buffer_ptr, value.data(), value_size);
    buffer_ptr += value_size;

    uint32_t crc = calculate_crc(buffer_ptr, buffer_size);
    char *crc_ptr = reinterpret_cast<char*>(&crc);

    // binary, at end, append
    std::ofstream file("./datafiles/active", std::ios::binary | std::ios::ate | std::ios::app);
    uint64_t file_size = file.tellp();
    uint64_t new_file_size = file_size + static_cast<uint64_t>(sizeof(crc)) + buffer_size;
    
    if(new_file_size > MAX_FILE_SIZE) {
        create_new_active();
    }

    uint64_t value_pos = static_cast<uint64_t>(file.tellp()) + sizeof(crc) + before_value_size;
    file.write(crc_ptr, sizeof(crc));
    file.write(buffer.data(), buffer_size);
    file.flush();

    // update in memory hashmap (keydir)
    KeyDirEntry entry = {
        static_cast<uint32_t>(files.size()),
        value_size,
        value_pos,
        timestamp
    };
    _keydir[key] = entry;
}

std::string Rocask::raw_read(std::string key) {
    if(_keydir.find(key) == _keydir.end()) {
        throw std::out_of_range("KeyError: " + key + " not found in map.");
    }
    KeyDirEntry entry = _keydir[key];
    std::string output;
    read_at_offset(
        "active",
        entry.value_pos,
        entry.value_size,
        output
    );
    return output;
}
