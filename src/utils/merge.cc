#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>

using namespace std;

// Function to merge multiple sorted files into an output file
void mergeFiles(const vector<string>& inputFiles, const string& outputFile) {
    vector<ifstream> inputStreams;

    // Open all input files and initialize input streams
    for (const string& fileName : inputFiles) {
        ifstream inFile(fileName);
        if (!inFile.is_open()) {
            cerr << "Error: Failed to open input file " << fileName << endl;
            return;
        }
        inputStreams.push_back(move(inFile));
    }

    // Open the output file
    ofstream outFile(outputFile);
    if (!outFile.is_open()) {
        cerr << "Error: Failed to open output file " << outputFile << endl;
        return;
    }

    vector<string> currentLines(inputFiles.size());
    bool anyFileOpen = true;

    // Read the first line from each input file
    for (size_t i = 0; i < inputFiles.size(); ++i) {
        if (getline(inputStreams[i], currentLines[i])) {
            anyFileOpen = true;
        } else {
            anyFileOpen = false; // Input file is empty
        }
    }

    // Merge the sorted chunks
    while (anyFileOpen) {
        // Find the minimum line among all current lines
        string minLine = *min_element(currentLines.begin(), currentLines.end());

        // Write the minimum line to the output file
        outFile << minLine << endl;

        // Update the current line for the file that had the minimum line
        for (size_t i = 0; i < inputFiles.size(); ++i) {
            if (currentLines[i] == minLine) {
                if (getline(inputStreams[i], currentLines[i])) {
                    anyFileOpen = true;
                } else {
                    anyFileOpen = false; // Input file is exhausted
                }
                break; // We found the file with the minimum line, no need to check others
            }
        }
    }

    // Close all input and output files
    for (ifstream& inFile : inputStreams) {
        inFile.close();
    }
    outFile.close();
}

int main() {
    vector<string> inputFiles = {"input1.txt", "input2.txt", "input3.txt"};
    string outputFile = "output.txt";

    // Merge the input files into a single output file
    mergeFiles(inputFiles, outputFile);

    cout << "Files merged and saved to " << outputFile << endl;

    return 0;
}

