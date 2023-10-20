#include <iostream>
#include <fstream>
#include <unordered_map>
#include <cmath>

double calculateEntropy(const std::string& text) {
    std::unordered_map<char, int> charFrequency;
    int totalChars = 0;

    // Count the frequency of each character in the text
    for (char ch : text) {
        charFrequency[ch]++;
        totalChars++;
    }

    double entropy = 0.0;

    // Calculate the entropy using the frequency of each character
    for (const auto& pair : charFrequency) {
        double probability = static_cast<double>(pair.second) / totalChars;
        entropy -= probability * log2(probability);
    }

    return entropy;
}

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <filename>\n";
        return 1;
    }

    const std::string filename = argv[1];
    std::ifstream file(filename);

    if (!file) {
        std::cerr << "Error opening file: " << filename << "\n";
        return 1;
    }

    std::string text;
    char ch;
    while (file.get(ch)) {
        text.push_back(ch);
    }

    file.close();

    double entropy = calculateEntropy(text);

    std::cout << "Empirical entropy of characters in the file: " << entropy << std::endl;

    return 0;
}

