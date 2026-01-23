#pragma once 

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "crc.hpp"
#include "../datastructures/SafeMap.hpp"
#include "utils.hpp"

namespace fs = std::filesystem;

const uint64_t MAX_FILE_SIZE = 8 * 1024 * 1024;
const uint64_t CARE_ENOUGH = 10 * 1024 * 1024; // couldn't think of a variable name
const double COMPACTION_THRESHOLD = 1.5;
 
struct KeyDirEntry {
    uint64_t file_id;
    uint64_t value_size;
    uint64_t value_pos;
    uint64_t timestamp;

    bool operator==(const KeyDirEntry& other) {
        return file_id == other.file_id && 
               value_size == other.value_size && 
               value_pos == other.value_pos && 
               timestamp == other.timestamp; 
    }
};

uint64_t num_compactions = 0;

class Rocask {
    public:
    Rocask();
    ~Rocask();

    template<typename K, typename V>
    void write(const K& key, const V& value);

    // write, read and compaction/merge
    template<typename K, typename V>
    V read(const K& key);
    void compaction();

    private:
    // KeyDir datastructures
    SafeMap<std::string, KeyDirEntry> _keydir;
    SafeMap<uint64_t, std::string> _datafiles;
    
    // compaction
    std::thread _compaction_thread;
    std::mutex _compaction_mutex;
    std::condition_variable _compaction_cv;
    bool _compact = false; 
    bool _shutdown = false;

    // file_id
    std::atomic<uint64_t> file_index{0};
    std::atomic<uint64_t> active_file_id{0};

    // file I/O RW lock
    std::shared_mutex _file_mutex;

    // memory size 
    uint64_t total_disk_used = 0;
    uint64_t actual_data_size = 0;

    //helper
    void process_datafile(const std::string &path, const uint64_t& file_id);
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

    // compaction helper
    bool compaction_conditions();
    void trigger_compaction(); 
    void compaction_worker();
};

Rocask::Rocask() {
    std::string _active_path = "datafiles/" + std::to_string(active_file_id.load());

    // create file, add to _files and close.
    std::ofstream tmp(_active_path);
    tmp.close();

    // any writes to _datafiles probably will use this
    _datafiles.put(active_file_id.load(), _active_path);

    _compaction_thread = std::thread(&Rocask::compaction_worker, this);
}

Rocask::~Rocask() {
    std::unique_lock<std::mutex> lock(_compaction_mutex);
    _shutdown = true;
    lock.unlock();

    _compaction_cv.notify_one();
    _compaction_thread.join();
}

void Rocask::process_datafile(const std::string& path, const uint64_t& file_id) {
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
        _keydir.put(key, entry);
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

    std::string _active_path = "datafiles/" + std::to_string(active_file_id.load());
    uint64_t file_size = fs::file_size(_active_path);
    uint64_t new_file_size = file_size + static_cast<uint64_t>(sizeof(crc)) + buffer_size;
    if(new_file_size > MAX_FILE_SIZE) {
        _datafiles.put(active_file_id.load(), _active_path);

        file_index.fetch_add(1);
        active_file_id.store(file_index.load());
        _active_path = "datafiles/" + std::to_string(active_file_id.load());

        _datafiles.put(active_file_id.load(), _active_path);

        trigger_compaction();
    }

    // binary, at end, append
    std::ofstream file(_active_path, std::ios::binary | std::ios::ate | std::ios::app);
    uint64_t value_pos = static_cast<uint64_t>(file.tellp()) + sizeof(crc) + before_value_size;
    file.write(crc_ptr, sizeof(crc));
    file.write(buffer.data(), buffer_size);
    file.flush();

    // update in memory hashmap (keydir)
    KeyDirEntry entry = {
        active_file_id.load(),
        value_size,
        value_pos,
        timestamp
    };

    std::optional<KeyDirEntry> previous_entry = _keydir.put(key, entry);
    uint64_t memory_used = sizeof(crc) + buffer_size;
    total_disk_used += memory_used;

    if(previous_entry.has_value()) {
        actual_data_size += (value_size - previous_entry->value_size);
    } else {
        actual_data_size += memory_used;
    }
}

std::string Rocask::raw_read(std::string key) {
    if(!_keydir.contains(key)) {
        throw std::out_of_range("KeyError: " + key + " not found in map.");
    }
    KeyDirEntry entry = _keydir.get(key);
    
    std::shared_lock lock(_file_mutex);
    
    std::string output;
    read_at_offset(
        _datafiles.get(entry.file_id),
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

    file_index.fetch_add(1);
    uint64_t new_datafile_file_id = file_index.load();

    std::string new_datafile_path = "datafiles/" + std::to_string(new_datafile_file_id);
    std::string new_datafile_hint_path = "hintfiles/" + std::to_string(new_datafile_file_id) + ".hint";

    std::vector<std::pair<uint64_t, std::string>> datafiles_in_dir = _datafiles.items();
    
    _datafiles.put(new_datafile_file_id, new_datafile_path);

    std::vector<std::string> hint_datafiles{new_datafile_hint_path};

    std::ofstream fout_new_datafile(new_datafile_path, std::ios::binary);
    std::ofstream fout_new_hint(new_datafile_hint_path, std::ios::binary);

    uint64_t new_cur_value_pos = 0;

    uint64_t during_compact_active_id = active_file_id.load();

    for(size_t i = 0; i < datafiles_in_dir.size(); i++) {
        uint64_t datafile_id = datafiles_in_dir[i].first;
        std::string datafile_path = datafiles_in_dir[i].second;
        
        // skip active path, compact/merge only old files
        if(datafile_id == during_compact_active_id) {
            continue;
        }

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

            KeyDirEntry old_entry = _keydir.get(key);
            KeyDirEntry datafile_entry = {
                datafile_id, 
                value_size, 
                cur_value_pos, 
                timestamp
            };

            if(old_entry == datafile_entry) {
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
                    
                    file_index.fetch_add(1);
                    new_datafile_file_id = file_index.load();
                    
                    new_datafile_path = "datafiles/" + std::to_string(new_datafile_file_id);
                    new_datafile_hint_path = "hintfiles/" + std::to_string(new_datafile_file_id);
                    hint_datafiles.push_back(new_datafile_hint_path);

                    _datafiles.put(new_datafile_file_id, new_datafile_path);

                    fout_new_datafile.close();
                    fout_new_datafile.clear();
                    fout_new_datafile.open(new_datafile_path, std::ios::binary);

                    // fout_new_hint.close();
                    // fout_new_hint.clear();
                    // fout_new_hint.open(new_datafile_hint_path, std::ios::binary);

                    new_cur_value_pos = 0;
                }

                fout_new_datafile.write(reinterpret_cast<char*>(&crc), sizeof(crc));
                fout_new_datafile.write(buffer.data(), buffer_size);
                fout_new_datafile.flush();
                
                new_cur_value_pos += sizeof(crc) + sizeof(timestamp) + sizeof(key_size) + sizeof(value_size) + key_size;

                // locked CAS update of _keydir
                _keydir.update(key, old_entry, [&](KeyDirEntry& cur_entry) {
                    cur_entry.file_id = new_datafile_file_id;
                    cur_entry.value_pos = new_cur_value_pos;
                });
                
                // fout_new_hint.write(reinterpret_cast<char*>(&key_size), sizeof(key_size));
                // fout_new_hint.write(key.data(), key_size);
                // fout_new_hint.write(reinterpret_cast<char*>(&new_datafile_file_id), sizeof(new_datafile_file_id));
                // fout_new_hint.write(reinterpret_cast<char*>(&value_size), sizeof(value_size));
                // fout_new_hint.write(reinterpret_cast<char*>(&new_cur_value_pos), sizeof(new_cur_value_pos));
                // fout_new_hint.write(reinterpret_cast<char*>(&timestamp), sizeof(timestamp));
                // fout_new_hint.flush();

                new_cur_value_pos += value_size;
            }
            
            // add remaining value_size bytes to position
            cur_value_pos += value_size;
        }

        fin_old.close();
    }

    std::unique_lock lock(_file_mutex);
    for(size_t i = 0; i < datafiles_in_dir.size(); i++) {
        uint64_t datafile_id = datafiles_in_dir[i].first;
        std::string datafile_path = datafiles_in_dir[i].second;

        if(datafile_id == during_compact_active_id) {
            continue;
        }

        _datafiles.remove(datafile_id);
        uint64_t file_size = fs::file_size(datafile_path);
        fs::remove(datafile_path);

        total_disk_used -= file_size;
    }
}

bool Rocask::compaction_conditions() {
    if(total_disk_used < CARE_ENOUGH) return false;
    if(actual_data_size == 0) return true;
    double ratio = (double) total_disk_used / actual_data_size;
    return ratio > COMPACTION_THRESHOLD; 
}

void Rocask::trigger_compaction() {
    if(!compaction_conditions()) {
        return;
    }

    std::unique_lock<std::mutex> lock(_compaction_mutex);
    _compact = true;
    lock.unlock();
    _compaction_cv.notify_one();
}

void Rocask::compaction_worker() {
    while(true) {
        std::unique_lock<std::mutex> lock(_compaction_mutex);

        _compaction_cv.wait(lock, [this] {
            return _compact || _shutdown;
        });

        if(_shutdown) break;

        _compact = false;
        lock.unlock();

        if(compaction_conditions()) {
            compaction();
        }
    }
}