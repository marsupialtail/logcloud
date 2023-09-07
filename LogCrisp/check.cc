#include <iostream>
#include <fstream>
#include <string>

int main(int argc, char *argv[]) {
    // Check for correct number of arguments
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <filename>\n";
        return 1;
    }

    // Open the file for reading
    std::ifstream inFile(argv[1]);
    if (!inFile.is_open()) {
        std::cerr << "Error opening file: " << argv[1] << "\n";
        return 2;
    }

    std::string line;
    while (std::getline(inFile, line)) {
        // Check if the line doesn't start with "201"
        if (line.substr(0, 3) != "201") {
            std::cout << line << "\n";
        }
    }

    inFile.close();
    return 0;
}
