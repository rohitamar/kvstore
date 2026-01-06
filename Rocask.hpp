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
    uint16_t file_id;
    uint64_t value_size;
    uint64_t value_pos;
    uint64_t timestamp;
};

class Rocask {
    public:
    Rocask() {
        if(!fs::exists(_active_path)) {
            // create file, add to _files and close.
            std::ofstream tmp(_active_path);
            tmp.close();

            // any writes to _datafiles probably will use this
            _datafiles[0] = _active_path;

            // only two types of writes:
            // writing file_id --> _active_path
            // writing file_id --> previously active_path
            // both cases, _active_path will be mapped to _datafiles.size() - 1
            // hence, funnily, always use that line for writes to _datafiles hashmap
            // (besides the first insertion)
        } else {
            // get active and all old files from ./datafiles
            std::vector<std::string> datafiles = get_datafiles();

            // process each datafile
            for(size_t i = 0; i < datafiles.size(); i++) {
                process_datafile(datafiles[i], i);
                _datafiles[i] = datafiles[i];
            }
        }
    }

    template<typename K, typename V>
    void write(const K& key, const V& value);

    // write, read and compaction/merge
    template<typename K, typename V>
    V read(const K& key);
    void compaction();

    private:
    std::unordered_map<std::string, KeyDirEntry> _keydir;
    std::unordered_map<uint16_t, std::string> _datafiles;
    const std::string _active_path = "datafiles/active";

    //helper
    void process_datafile(const std::string &path, const uint16_t& file_id);
    void build_data_buffer(
        char *buffer_ptr, 
        uint64_t timestamp,
        uint64_t key_size,
        uint64_t value_size,
        const std::string& key,
        const std::string& value
    );

    // helper as well, but write/read
    void raw_write(const std::string& key, const std::string& value);
    std::string raw_read(std::string key);
};

void Rocask::process_datafile(const std::string& path, const uint16_t& file_id) {
    std::ifstream fin(path, std::ios::binary);
    uint64_t cur_value_pos = 0;

    // crc - 4 bytes
    uint32_t crc;
    while(fin.read(reinterpret_cast<char*>(&crc), sizeof(crc))) {
        // timestamp - 4 bytes
        uint64_t timestamp;
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

void Rocask::build_data_buffer(
    char *buffer_ptr, 
    uint64_t timestamp,
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
    uint64_t timestamp = get_timestamp();    
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
    uint64_t file_size = fs::file_size(Rocask::_active_path);
    uint64_t new_file_size = file_size + static_cast<uint64_t>(sizeof(crc)) + buffer_size;
    if(new_file_size > MAX_FILE_SIZE) {
        std::string new_path = "datafiles/" + std::to_string(get_timestamp());
        fs::rename(old_path, new_path);

        // "new_path" is basically the current "active" file
        // the id of this is _datafiles.size() - 1
        _datafiles[_datafiles.size() - 1] = new_path;

        // _active_path's file_id is always _datafiles.size()
        _datafiles[_datafiles.size()] = _active_path;
    }

    // binary, at end, append
    std::ofstream file(old_path, std::ios::binary | std::ios::ate | std::ios::app);
    uint64_t value_pos = static_cast<uint64_t>(file.tellp()) + sizeof(crc) + before_value_size;
    file.write(crc_ptr, sizeof(crc));
    file.write(buffer.data(), buffer_size);
    file.flush();

    // update in memory hashmap (keydir)
    KeyDirEntry entry = {
        static_cast<uint16_t>(_datafiles.size() - 1),
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
    std::string cur_timestamp = std::to_string(get_timestamp());
    std::string new_datafile_path = "newdatafiles/" + cur_timestamp;
    std::string new_datafile_hint_path = "newdatafiles/" + cur_timestamp + ".hint";

    std::ofstream fout_new_datafile(new_datafile_path, std::ios::binary);
    std::ofstream fout_new_hint(new_datafile_hint_path, std::ios::binary);

    uint16_t new_datafile_file_id = _datafiles.size();
    _datafiles[new_datafile_file_id] = new_datafile_path;

    std::vector<std::string> hint_datafiles{new_datafile_hint_path};

    std::vector<std::string> datafiles_in_dir = get_datafiles();
    for(size_t i = 0; i < datafiles_in_dir.size(); i++) {
        if(i == datafiles_in_dir.size() - 1) {
            // skip active path, compact/merge only old files
            continue;
        }

        std::string datafile_path = datafiles_in_dir[i];
        // fin_old reads from old datafiles, fout_new writes to new datafiles
        std::ifstream fin_old(datafile_path, std::ios::binary);
        
        // same as what we did in raw_read
        uint64_t cur_value_pos = 0;
        // crc - 4 bytes
        uint32_t crc;
        while(fin_old.read(reinterpret_cast<char*>(&crc), sizeof(crc))) {
            // timestamp - 8 bytes
            uint64_t timestamp;
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
                size_t buffer_size = sizeof(timestamp) + sizeof(key_size) + sizeof(value_size) + key_size + value_size; 
                std::vector<char> buffer(buffer_size);
                build_data_buffer(
                    buffer.data(),
                    timestamp,
                    key_size,
                    value_size,
                    key,
                    value
                );

                // check if new-{j} can append new data (doesn't exceed the memory limit)
                uint64_t file_size = fs::file_size(new_datafile_path);
                uint64_t new_file_size = file_size + static_cast<uint64_t>(sizeof(crc)) + buffer_size;
                if(new_file_size > MAX_FILE_SIZE) {
                    cur_timestamp = std::to_string(get_timestamp());
                    new_datafile_path = "newdatafiles/" + cur_timestamp;
                    new_datafile_hint_path = "newdatafiles/" + cur_timestamp + ".hint"; 

                    fout_new_datafile.close();
                    fout_new_datafile.clear();
                    fout_new_datafile.open(new_datafile_path, std::ios::binary);

                    new_datafile_file_id = _datafiles.size();
                    _datafiles[_datafiles.size()] = new_datafile_path;
                    hint_datafiles.push_back(new_datafile_hint_path);
                }
                
                fout_new_datafile.write(reinterpret_cast<char*>(&crc), sizeof(crc));
                fout_new_datafile.write(buffer.data(), buffer_size);
                fout_new_datafile.flush();

                // i think the hint file can just have key and file_id
                // i dont see the point of having the rest (for now atleast)
                fout_new_hint.write(reinterpret_cast<char*>(&key_size), sizeof(key_size));
                fout_new_hint.write(key.data(), key_size);
                fout_new_hint.write(reinterpret_cast<char*>(&new_datafile_file_id), sizeof(new_datafile_file_id));
                fout_new_hint.flush();
            }
            
            // add remaining value_size bytes to position
            cur_value_pos += value_size;
        }
    }

    // first step is to map out hintfiles (inside "hint_datafiles")
    // go through keydir
    for(std::string hint_datafile : hint_datafiles) {
        std::ifstream fin(hint_datafile, std::ios::binary);
        
        uint64_t key_size;
        while(fin.read(reinterpret_cast<char*>(&key_size), sizeof(key_size))) {
            std::vector<char> key_bytes(key_size);
            fin.read(key_bytes.data(), key_size);
            std::string key(key_bytes.data(), key_size);
            
            uint16_t file_id;
            fin.read(reinterpret_cast<char*>(&file_id), sizeof(file_id));

            // keydir update
            // this eventually needs to be mutex locked
            _keydir[key].file_id = file_id;
        }
    }
    // then you go through every single record in the hint file
    // and update keydir (will end up just changing the file_id)
    // thing is updating keydir is going to need to be mutex locked, so we're going to have to figure that one out
    // you can let many threads in a keydir for a raw_read
    // compaction and raw_write will need to be locked though
    // so i think thats how it would work
}