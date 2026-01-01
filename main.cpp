#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "helper.hpp"

struct KeyDirEntry {
    uint32_t file_id;
    uint64_t value_size;
    uint64_t value_pos;
    uint32_t timestamp;
};

std::unordered_map<std::string, KeyDirEntry> keydir;

bool read_at_offset(
    const std::string& filename,
    uint64_t offset, 
    uint64_t size, 
    std::string& output
) {
    std::ifstream file(filename, std::ios::binary);
    if(!file.is_open()) {
        return false;
    }
    
    file.seekg(offset, std::ios::beg);
    if(!file) {
        return false;
    }

    std::vector<char> output_ptr(size);
    file.read(output_ptr.data(), size);
    output = output_ptr.data();

    return true;
}


void raw_write(std::string key, std::string value) {
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
    std::ofstream file("active", std::ios::binary | std::ios::ate | std::ios::app);
    uint64_t value_pos = static_cast<uint64_t>(file.tellp()) + sizeof(crc) + before_value_size;
    file.write(crc_ptr, sizeof(crc));
    file.write(buffer.data(), buffer_size);
    file.flush();

    KeyDirEntry entry = {
        0,
        value_size,
        value_pos,
        timestamp
    };
    keydir[key] = entry;
}

std::string raw_read(std::string key) {
    if(keydir.find(key) == keydir.end()) {
        throw std::out_of_range("KeyError: " + key + " not found in map.");
    }
    KeyDirEntry entry = keydir[key];
    std::string output;
    read_at_offset(
        "active",
        entry.value_pos,
        entry.value_size,
        output
    );
    return output;
}

int main() {
    raw_write("Rohit", "12341");
    raw_write("Amarnath", "512315");
    std::cout << raw_read("Rohit") << "\n";
    std::cout << raw_read("asdasd") << "\n";
    return 0;
}