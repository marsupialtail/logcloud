#include "plist.h"
#include "cassert"
#include <iostream>
#include <fstream>
#include <sstream>

int main(int argc, char *argv[]) {
    
    std::vector<std::vector<plist_size_t>> data = {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}};  
    // make a copy of data
    auto data_copy = data;
    PListChunk plist(std::move(data));
    std::string serialized = plist.serialize();
    PListChunk plist2(serialized);
    std::string serialized2 = plist2.serialize();
    assert(serialized == serialized2);

    auto result = plist2.data();
    // check that result is the same as data_copy
    assert(result == data_copy);
    assert(plist2.lookup(1) == data_copy[1]);


    // read in data from a file. Each line has numbers separated by spaces
    std::ifstream infile(argv[1]);
    if (!infile.is_open()) {
        std::cerr << "Error opening input file: " << argv[1] << "\n";
        return 1;
    }
    std::string line;
    std::vector<std::vector<plist_size_t>> data2;
    while (std::getline(infile, line)) {
        std::istringstream iss(line);
        std::vector<plist_size_t> numbers;
        plist_size_t number;
        while (iss >> number) {
            number = number / 10000;
            if(numbers.size() > 0 && numbers.back() == number) {
                continue;
            }
            numbers.push_back(number);
        }
        data2.push_back(numbers);
    }

    PListChunk plist3(std::move(data2));
    std::string serialized3 = plist3.serialize();
    std::cout << "serialized size " << serialized3.size() << "\n";

    return 0;
}