/*
Compaction ended.
Key: HKUQA
DB Value: XXQFD
Actual Value: PAQZR
make: *** [Makefile:5: run] Error 1
*/
#include <fstream>
#include <iostream>
#include <map>
#include <random>
#include <set> 
#include <sstream>
#include <string>
#include <vector>

#include "Rocask.hpp"
#include "test/TestUtils.hpp"

int main() {
    Rocask db;
    
    std::vector<std::string> keys;
    std::map<std::string, std::string> real_map;

    std::ifstream fin("./dumps/failed1.txt");
    std::string line;
    while(std::getline(fin, line)) {
        std::stringstream ss(line);
        std::string key, value;
        ss >> key >> value;
        db.write<std::string, std::string>(key, value);
        real_map[key] = value;
        keys.push_back(key);
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
}