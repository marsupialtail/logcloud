#include <vector>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <sstream>
#include  "math.h"
#include "../compressor.h"

int main(int argc, char *argv[]) {
    
    Compressor compressor(CompressionAlgorithm::ZSTD);

    std::vector<int64_t> numbers = {0};
    //read the numbers from argv[1], one number each line

    std::ifstream infile(argv[1]);
    if (!infile.is_open()) {
        std::cerr << "Error opening input file: " << argv[1] << "\n";
        return 1;
    }

    std::string line;
    size_t max_diff = -1;
    while (std::getline(infile, line)) {
        int number = std::stoul(line) - numbers.back();
        if (max_diff == -1 || abs(number) > max_diff) {
            max_diff = abs(number);
        }
        numbers.push_back(std::stoul(line));
    }

    std::cout << "max diff: " << max_diff << "\n";

    std::string compressed_level_offsets = compressor.compress((char *)numbers.data(), numbers.size() * sizeof(int64_t));
    std::cout << "compressed length: " << compressed_level_offsets.size() << "\n";
    
}
