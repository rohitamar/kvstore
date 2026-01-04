#include <iostream>
#include <random> 
#include <string>
#include <vector>

#include "Rocask.hpp"
#include "test/TestUtils.hpp"

const size_t num_records = 20000;

int main() {
    Rocask db;
    std::ofstream fout("test.txt");
    std::vector<std::string> keys;
    for(size_t i = 0; i < num_records; i++) {
        std::string key = gen_random_string(1025);
        std::string value = gen_random_string(1000);
        db.write<std::string, std::string>(key, value);
        fout << key << " " << value << "\n";
        keys.push_back(key);
        
        // double val = get_random_value();
        // if(val >= 0.6) {
        //     size_t ind = get_random_index(keys.size() - 1);
        //     std::string old_key = keys[ind];
        //     std::string new_value = gen_random_string(250);
        //     db.write<std::string, std::string>(old_key, new_value);
        //     fout << old_key << " " << new_value << "\n"; 
        // }
    }

    db.compaction();
    
    return 0;
}