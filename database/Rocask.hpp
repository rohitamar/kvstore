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

class Rocask {
    public:
    Rocask(int id);
    ~Rocask();

    template<typename K, typename V>
    void write(const K& key, const V& value);

    // write, read and compaction/merge
    template<typename K, typename V>
    V read(const K& key);
    void compaction();

    private:
    // datafiles folder
    int db_id;
    std::string datafiles_folder;

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

    // statistics
    uint64_t num_compactions = 0;
};

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