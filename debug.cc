#include <iostream>
#include <string>

int main() {
    std::string hex_string;
    std::cout << "Enter a hexadecimal number: ";
    std::cin >> hex_string;

    try {
        unsigned long value = std::stoul(hex_string, nullptr, 16);
        std::cout << "The decimal value is: " << value << std::endl;
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << std::endl;
    }

    return 0;
}

