#include <iostream>
#include <string>
#include <filesystem>

namespace fs = std::filesystem; 

int main() {
    fs::rename("datafiles/active", "datafiles/old-" + std::to_string(1));
}