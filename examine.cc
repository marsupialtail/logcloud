#include <iostream>
#include <fstream>
#include <bitset>

int main(int argc, char * argv[]) {
    std::ifstream binaryFile(argv[1], std::ios::binary);
    std::ofstream textFile(argv[2]);

    if (!binaryFile.is_open() || !textFile.is_open()) {
        std::cerr << "Error opening files!" << std::endl;
        return 1;
    }

    char byte;
    while (binaryFile.read(&byte, 1)) {
        std::bitset<8> bits(byte);
        for (int i = 7; i >= 0; --i) {
            textFile << bits[i];
        }
        textFile << std::endl; // optional, adds a newline after every byte
    }

    binaryFile.close();
    textFile.close();

    std::cout << "Processing complete!" << std::endl;

    return 0;
}

