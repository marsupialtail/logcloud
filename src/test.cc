#include <iostream>
#include <fstream>
#include <string>

void splitFile(const std::string& inputFilename, const std::string& output1, const std::string& output2, size_t N) {
    std::ifstream infile(inputFilename);
    std::ofstream outfile1(output1);
    std::ofstream outfile2(output2);

    if (!infile.is_open() || !outfile1.is_open() || !outfile2.is_open()) {
        std::cerr << "Error opening files!" << std::endl;
        return;
    }

    std::string line;
    while (std::getline(infile, line)) {
        if (line.length() <= N) {
            outfile1 << line << std::endl;
            outfile2 << std::endl;
        } else {
            outfile1 << line.substr(0, N) << std::endl;
            outfile2 << line.substr(N) << std::endl;
        }
    }

    infile.close();
    outfile1.close();
    outfile2.close();
}

int main(int argc, char * argv[]) {
    const std::string inputFilename = argv[1];
    const std::string output1 = "first_N.txt";
    const std::string output2 = "remaining.txt";
    const size_t N = std::stoul(argv[2]); // Modify this value to choose how many characters you want to split on

    splitFile(inputFilename, output1, output2, N);

    std::cout << "Files processed successfully!" << std::endl;
    return 0;
}

