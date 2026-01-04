#pragma once 

#include <algorithm>
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
    uint64_t file_id;
    uint64_t value_size;
    uint64_t value_pos;
    uint32_t timestamp;
};

class Rocask {
    public:
    Rocask() {
        if(!fs::exists(_active_path)) {
            // create file, add to _files and close.
            std::ofstream tmp(_active_path);
            tmp.close();
            _datafiles.push_back(_active_path.string());
        } else {
            // get active and all old files from ./datafiles
            std::vector<std::string> datafiles_in_dir = get_datafiles();

            // process each datafile
            for(size_t i = 0; i < datafiles_in_dir.size(); i++) {
                process_datafile(datafiles_in_dir[i], i);
                _datafiles.push_back(datafiles_in_dir[i]);
            }
        }
    }

    template<typename K, typename V>
    void write(const K& key, const V& value);

    template<typename K, typename V>
    V read(const K& key);

    private:
    std::unordered_map<std::string, KeyDirEntry> _keydir;
    std::vector<std::string> _datafiles;
    fs::path _active_path = "datafiles/active";

    void process_datafile(const std::string &path, const uint32_t& file_id);

    void raw_write(const std::string& key, const std::string& value);
    std::string raw_read(std::string key);

    void compaction();
};

void Rocask::process_datafile(const std::string& path, const uint32_t& file_id) {
    std::ifstream fin(path, std::ios::binary);
    uint64_t cur_value_pos = 0;

    // crc - 4 bytes
    uint32_t crc;
    while(fin.read(reinterpret_cast<char*>(&crc), sizeof(crc))) {
        // timestamp - 4 bytes
        uint32_t timestamp;
        fin.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));

        // key_size - 8 bytes 
        uint64_t key_size;
        fin.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));

        // value_size - 8 bytes 
        uint64_t value_size;
        fin.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));             

        // key - key_size bytes 
        std::vector<char> key_bytes(key_size);
        fin.read(reinterpret_cast<char*>(key_bytes.data()), key_size);
        std::string key(key_bytes.data(), key_size);
        
        // adjust current value position
        cur_value_pos += sizeof(crc) + sizeof(timestamp) + sizeof(key_size) + sizeof(value_size) + key_size;
    
        // build and store entry
        KeyDirEntry entry = {
            file_id,
            value_size,
            cur_value_pos,
            timestamp
        };
        _keydir[key] = entry;
        // value - ignore value_size bytes, adjust value_pos for next iteration
        fin.ignore(value_size);
        cur_value_pos += value_size;
    }
}

void build_data_buffer(
    char *buffer_ptr, 
    uint32_t timestamp,
    uint64_t key_size,
    uint64_t value_size,
    const std::string& key,
    const std::string& value
) {
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
}

void Rocask::raw_write(const std::string& key, const std::string& value) {
    // get timestamp, key_size, value_size
    uint32_t timestamp = get_timestamp();    
    uint64_t key_size = static_cast<uint64_t>(key.size());
    uint64_t value_size = static_cast<uint64_t>(value.size());

    // before_value_size
    uint64_t before_value_size = sizeof(timestamp) + sizeof(key_size) + sizeof(value_size) + key_size;
    uint64_t buffer_size = before_value_size + value_size; 
    
    std::vector<char> buffer(buffer_size);
    build_data_buffer(
        buffer.data(),
        timestamp, 
        key_size,
        value_size,
        key,
        value
    );

    uint32_t crc = calculate_crc(buffer.data(), buffer_size);
    char *crc_ptr = reinterpret_cast<char*>(&crc);

    const std::string old_path = "datafiles/active";
    uint64_t file_size = fs::file_size(_active_path);
    uint64_t new_file_size = file_size + static_cast<uint64_t>(sizeof(crc)) + buffer_size;
    if(new_file_size > MAX_FILE_SIZE) {
        uint32_t count = num_data_files();
        std::string new_path = "datafiles/old-" + std::to_string(count);
        fs::rename(old_path, new_path);
        _datafiles.insert(_datafiles.end() - 1, new_path);
    }

    // binary, at end, append
    std::ofstream file(old_path, std::ios::binary | std::ios::ate | std::ios::app);
    uint64_t value_pos = static_cast<uint64_t>(file.tellp()) + sizeof(crc) + before_value_size;
    file.write(crc_ptr, sizeof(crc));
    file.write(buffer.data(), buffer_size);
    file.flush();

    // update in memory hashmap (keydir)
    KeyDirEntry entry = {
        static_cast<uint64_t>(_datafiles.size() - 1),
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
        _datafiles[entry.file_id],
        entry.value_pos,
        entry.value_size,
        output
    );
    return output;
}

template<typename K, typename V>
V Rocask::read(const K& key) {
    std::string raw_key = deserialize<K>(key);
    std::string raw_value = raw_read(raw_key);
    V value = deserialize<V>(raw_value);
    return value;
}

template<typename K, typename V>
void Rocask::write(const K& key, const V& value) {
    std::string raw_key = serialize<K>(key);
    std::string raw_value = serialize<V>(value);
    raw_write(raw_key, raw_value);
}

void Rocask::compaction() {
    std::vector<std::string> datafiles_in_dir = get_datafiles();
    size_t new_datafile_index = 1;
    for(size_t i = 0; i < datafiles_in_dir.size(); i++) {
        if(i == 0) {
            continue;
        }
        std::string datafile_path = datafiles_in_dir[i];
        std::ifstream fin_old(datafile_path, std::ios::binary);
        
        std::string new_datafile_path = "new-" + new_datafile_index;
        std::ofstream fout_new(new_datafile_path, std::ios::binary);

        uint64_t cur_value_pos = 0;
        // crc - 4 bytes
        uint32_t crc;
        while(fin_old.read(reinterpret_cast<char*>(&crc), sizeof(crc))) {
            // timestamp - 4 bytes
            uint32_t timestamp;
            fin_old.read(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));

            // key_size - 8 bytes 
            uint64_t key_size;
            fin_old.read(reinterpret_cast<char*>(&key_size), sizeof(key_size));

            // value_size - 8 bytes 
            uint64_t value_size;
            fin_old.read(reinterpret_cast<char*>(&value_size), sizeof(value_size));             

            // key - key_size bytes 
            std::vector<char> key_bytes(key_size);
            fin_old.read(reinterpret_cast<char*>(key_bytes.data()), key_size);
            std::string key(key_bytes.data(), key_size);
            
            // adjust val_pos
            cur_value_pos += sizeof(crc) + sizeof(timestamp) + sizeof(key_size) + sizeof(value_size) + key_size;
            
            std::vector<char> value_bytes(value_size);
            fin_old.read(reinterpret_cast<char*>(value_bytes.data()), value_size);
            std::string value(value_bytes.data(), value_size);

            KeyDirEntry entry = _keydir[key];
            if(entry.file_id == i && entry.timestamp == timestamp && entry.value_pos == cur_value_pos && entry.value_size == value_size) {
                // write to new-{i}
                size_t buffer_size = sizeof(crc) + sizeof(timestamp) + sizeof(key_size) + sizeof(value_size) + key_size + value_size; 
                std::vector<char> buffer(buffer_size);
                build_data_buffer(
                    buffer.data(),
                    timestamp,
                    key_size,
                    value_size,
                    key,
                    value
                );

                fout_new.write(reinterpret_cast<char*>(&crc), sizeof(crc));
                fout_new.write(buffer.data(), buffer_size);
                fout_new.flush();
            }
            
            // add remaining value_size bytes to position
            cur_value_pos += value_size;
        }
    }

    // delete old-1 to old-5
    // then new-1 to new-5??
}