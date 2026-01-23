#include <algorithm>
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

template<typename T>
std::vector<T> sample_vector(const std::vector<T>& vec, size_t size) {
    std::vector<T> sample_vec;
    std::random_device rd;
    std::mt19937 gen(rd());

    std::sample(
        vec.begin(), vec.end(), 
        std::back_inserter(sample_vec),
        size,
        gen
    );

    return sample_vec;
}

size_t get_random_index(size_t length) {
    static std::random_device dev;
    static std::mt19937 gen(dev());
    std::uniform_int_distribution<size_t> dist(0, length);
    return dist(gen);
}