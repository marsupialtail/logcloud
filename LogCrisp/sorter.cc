#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <unordered_map>
#include <map>

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <file1> <file2>" << std::endl;
        return 1;
    }

    std::ifstream file1(argv[1]);
    std::ifstream file2(argv[2]);

    if (!file1 || !file2) {
        std::cerr << "Error opening files." << std::endl;
        return 1;
    }

    std::vector<size_t> lines1;
    std::vector<std::string> lines2;

    std::string line;
    while (std::getline(file1, line)) {
        lines1.push_back(std::stoul(line));
    }

    while (std::getline(file2, line)) {
        lines2.push_back(line);
    }

    if (lines1.size() != lines2.size()) {
        std::cerr << "Files have different number of lines. Exiting." << std::endl;
        return 1;
    }

    // Create an ordered copy of lines1 for reference
    std::vector<size_t> lines1Copy = lines1;
    std::sort(lines1Copy.begin(), lines1Copy.end());

    // Build a multimap index based on the sorted version
    std::multimap<size_t, int> lineIndices;
    for (size_t i = 0; i < lines1Copy.size(); ++i) {
        lineIndices.insert({lines1Copy[i], i});
    }

    std::vector<std::string> sortedLines2(lines2.size());
    for (size_t i = 0; i < lines2.size(); ++i) {
        auto it = lineIndices.find(lines1[i]);
        if (it != lineIndices.end()) {
            sortedLines2[it->second] = lines2[i];
            lineIndices.erase(it); // Erase once used to handle duplicates
        }
    }

    std::ofstream outfile2("sorted2.txt");

    for (const auto& l : sortedLines2) {
        outfile2 << l << "\n";
    }

    std::ofstream outfile1("sorted1.txt");

    for (const auto& l : lines1Copy) {
        outfile1 << l << "\n";
    }

    return 0;
}

