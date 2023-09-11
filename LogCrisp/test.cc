#include <iostream>
#include <vector>

int main() {
    int inputNumber;
    std::cout << "Enter a number from 1 to 63: ";
    std::cin >> inputNumber;

    if (inputNumber < 1 || inputNumber > 63) {
        std::cerr << "Invalid input. Please enter a number from 1 to 63." << std::endl;
        return 1;
    }

    std::vector<int> matchingNumbers;

    for (int i = 1; i <= 63; ++i) {
        if ((i & inputNumber) == inputNumber) {
            matchingNumbers.push_back(i);
        }
    }

    std::cout << "Numbers sharing the same 1-bits as " << inputNumber << ": ";
    for (int num : matchingNumbers) {
        std::cout << num << " ";
    }
    std::cout << std::endl;

    return 0;
}

