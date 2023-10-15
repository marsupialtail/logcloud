#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <set>
#include <unordered_map>
#include <algorithm>
#include <string>
#include <iomanip>
#include <cassert>
#include <filesystem>

int main()
{
	std::ostringstream oss;
        oss << "compressed_0/chunk0000.eid";
        std::string chunk_filename = oss.str();
        std::cout << "processing chunk file: " << chunk_filename << std::endl;

        std::ifstream eid_file(chunk_filename);
        if (!eid_file.is_open()) {
            std::cerr << "Failed to open file: " << chunk_filename << std::endl;
            return -1;
        }
	std::string line;
        std::vector<int> chunk_eids;
        while (std::getline(eid_file, line)) {
            std::cout << line << std::endl;
            chunk_eids.push_back(std::stoi(line));
        }
        eid_file.close();
}
