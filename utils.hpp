#pragma once 

#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <type_traits>
#include <vector>

namespace fs = std::filesystem;

inline uint64_t get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    auto seconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();
    return static_cast<uint64_t>(seconds);
}

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
    output.assign(output_ptr.data(), size);
    return true;
}

template<typename T>
std::string serialize(const T& data) {
    if constexpr (std::is_same_v<std::decay_t<T>, std::string>) {
        return data;
    } else {
        static_assert(std::is_trivially_copyable_v<T>, "Key and Value must be a fundamental data type or a string.");
        return std::string(
            reinterpret_cast<const char*>(&data),
            sizeof(T)
        );
    }
}

template<typename T>
T deserialize(const std::string& data) {
    if constexpr (std::is_same_v<std::decay_t<T>, std::string>) {
        return data;
    } else {
        static_assert(std::is_trivially_copyable_v<T>, "Key and Value must be a fundamental data type or a string.");
        
        if(data.size() != sizeof(T)) {
            throw std::runtime_error("Deserialization error.");
        }
        
        T result;
        std::memcpy(&result, data.data(), sizeof(T));
        return result;
    }
}

uint32_t num_data_files() {
    const fs::path datafiles_dir{"datafiles"};
    uint32_t count = 0;
    for(auto _ : fs::directory_iterator(datafiles_dir)) {
        count++;
    }
    return count;
}

std::vector<std::string> get_datafiles() {
    const fs::path datafiles_dir{"datafiles"};
    std::vector<std::string> datafiles_in_dir;
    for(const auto& datafile_path : fs::directory_iterator(datafiles_dir)) {
        datafiles_in_dir.push_back(datafile_path.path().generic_string());
    }
    
    // sort to maintain order (file_id indices are decided like this)
    std::sort(datafiles_in_dir.begin(), datafiles_in_dir.end());

    // ensure that datafiles/active is last element
    // helps with file_id indexing
    assert(datafiles_in_dir.back() == "datafiles/active");

    return datafiles_in_dir;
}