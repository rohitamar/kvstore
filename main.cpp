#include <iostream>
#include <random> 
#include <string>

#include "Rocask.hpp"

const std::string letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const int num_records = 15;

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

int main() {
    Rocask db;
    // std::ofstream fout("test.txt");
    // for(int i = 0; i < num_records; i++) {
    //     std::string key = gen_random_string(50);
    //     std::string value = gen_random_string(250);
    //     db.write<std::string, std::string>(key, value);
    //     fout << key << " " << value << "\n";
    // }
    std::string value = db.read<std::string, std::string>("UMFXVUFSPSUNDLWWVNXKMCRLAQXJWUUPTOJCOYSWULNFSHTCIZ");
    std::cout << value << "\n";
    return 0;
}