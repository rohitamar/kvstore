#pragma once 

#include <cassert>
#include <cstring>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <type_traits>
#include <vector>

namespace fs = std::filesystem;

uint64_t get_timestamp();

bool read_at_offset(
    const std::string& filename,
    uint64_t offset, 
    uint64_t size, 
    std::string& output
);

uint32_t num_data_files();

std::vector<std::string> get_datafiles();

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