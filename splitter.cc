#include <iostream>
#include <fstream>
#include <string>
#include <unordered_map>

int main(int argc, char * argv[]) {
    std::ifstream infile(argv[1]);  // Input file named "strings.txt"
    if (!infile) {
        std::cerr << "Failed to open the input file." << std::endl;
        return 1;
    }

    // Using an unordered_map to store file streams for different lengths.
    std::unordered_map<size_t, std::ofstream> outfiles;

    std::string line;
    while (std::getline(infile, line)) {
        size_t length = line.size();
        
        // If an ofstream for this length doesn't already exist, create it.
        if (outfiles.find(length) == outfiles.end()) {
            std::string filename = "length_" + std::to_string(length) + ".txt";
            outfiles[length].open(filename, std::ios::out);

            if (!outfiles[length]) {
                std::cerr << "Failed to open the output file: " << filename << std::endl;
                return 1;
            }
        }

        outfiles[length] << line << '\n';
    }

    // Close all the opened ofstream objects.
    for (auto& pair : outfiles) {
        pair.second.close();
    }

    return 0;
}

