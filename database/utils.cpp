#include "utils.hpp"

uint64_t get_timestamp() {
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
    return datafiles_in_dir;
}