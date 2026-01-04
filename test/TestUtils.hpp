#include <random> 
#include <string>

const std::string letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

std::string gen_random_string(size_t length) {
    static std::random_device dev;
    static std::mt19937 rng(dev());
    static std::uniform_int_distribution<std::mt19937::result_type> dist(0, 25);

    std::string result;
    result.reserve(length);
    for(size_t i = 0; i < length; i++) {
        result += letters[dist(rng)];
    }

    return result;
}

double get_random_value() {
    static std::random_device dev;
    static std::mt19937 gen(dev());
    static std::uniform_real_distribution<double> dist(0.0, 1.0);
    return dist(gen);
}

size_t get_random_index(size_t length) {
    static std::random_device dev;
    static std::mt19937 gen(dev());
    std::uniform_int_distribution<size_t> dist(0, length);
    return dist(gen);
}