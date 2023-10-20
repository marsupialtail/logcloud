#include <fstream>
#include <iostream>

bool is_file_ascii(const std::string& filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file) {
        std::cerr << "Error: Could not open file\n";
        return false;
    }

    char c;
    while (file.get(c)) {
        if (static_cast<unsigned char>(c) > 127) {
            return false;
        }
    }

    return true;
}

int main(int argc, char * argv[]) {
    std::string filename = argv[1];
    if (is_file_ascii(filename)) {
        std::cout << "The file is ASCII compliant\n";
    } else {
        std::cout << "The file is not ASCII compliant\n";
    }
    return 0;
}
