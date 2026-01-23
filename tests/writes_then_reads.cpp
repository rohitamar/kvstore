#include <iostream>
#include <map>
#include <random>
#include <set> 
#include <string>
#include <vector>

#include "../database/Rocask.hpp"
#include "../database/utils.hpp"
#include "TestUtils.hpp"

const size_t num_records = 1500;
const size_t key_size = 50;
const size_t value_size = 5000;
const double dup_rate = 1.0;

int main() {
    Rocask db;
    
    std::vector<std::string> keys;
    std::map<std::string, std::string> real_map;

    std::string testcase_filename = "./dumps/testcase-" + std::to_string(get_timestamp()) + ".txt";
    std::cout << "Testcase file: " << testcase_filename << std::endl;
    std::ofstream fout(testcase_filename);

    for(size_t i = 0; i < num_records; i++) {
        std::string key = gen_random_string(key_size);
        std::string value = gen_random_string(value_size);
        db.write<std::string, std::string>(key, value);

        fout << key << " " << value << "\n";
        keys.push_back(key);
        real_map[key] = value;
        
        double val = get_random_value();
        if(val <= dup_rate) {
            size_t ind = get_random_index(keys.size() - 1);
            std::string old_key = keys[ind];
            std::string new_value = gen_random_string(value_size);
            db.write<std::string, std::string>(old_key, new_value);

            fout << old_key << " " << new_value << "\n";
            real_map[old_key] = new_value; 
        }
    }

    for(std::string key : keys) {
        std::string db_value = db.read<std::string, std::string>(key);
        std::string actual_value = real_map[key];
        if(db_value != actual_value) {
            std::cerr << "Key: " << key << "\n";
            std::cerr << "DB Value: " << db_value.substr(0, 5) << "\n";
            std::cerr << "Actual Value: " << actual_value.substr(0, 5) << "\n";
            exit(1);
        }
    }
    
    return 0;
}