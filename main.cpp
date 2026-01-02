#include <iostream>

#include "Rocask.hpp"

int main() {
    Rocask db;
    db.write<std::string, std::string>("Rohit", "123456");
    db.write<std::string, int>("Amarnath", 789123);

    std::cout << db.read<std::string, int>("Amarnath") << "\n";
    return 0;
}