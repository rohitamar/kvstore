#include <iostream>
#include <random> 
#include <string>
#include <filesystem>

namespace fs = std::filesystem; 

int main() {
    std::random_device dev;
    std::mt19937 gen(dev());
    std::uniform_int_distribution<size_t> dist(0, 0);
    std::cout << dist(gen) << "\n";
}