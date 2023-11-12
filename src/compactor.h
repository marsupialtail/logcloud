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
#include "compressor.h"
#include <cerrno>
#include <cstring>
#include <glog/logging.h>

const int ROW_GROUP_SIZE = 100000;
const double DICT_RATIO_THRESHOLD = 0.5; // if something appears in more than 50% of row groups, then it is a dictionary

using namespace std;
namespace fs = std::filesystem;

void mergeFiles(const vector<string>& inputFilenames, const vector<string>& inputFilenamesLinenumbers,
               const string& outputFilename, const string& outputFilenameLinenumbers, size_t num_row_groups) {
    
    vector<ifstream> inputFiles;
    vector<ifstream> inputFilesLinenumbers;

    // Open input files and input files for line numbers
    for (const string& filename : inputFilenames) {
        inputFiles.emplace_back(filename);
    }

    for (const string& filename : inputFilenamesLinenumbers) {
        inputFilesLinenumbers.emplace_back(filename);
    }

    vector<string> currentLines(inputFiles.size());
    vector<vector<int>> currentLinenumbers(inputFilesLinenumbers.size());
    
    ofstream outputFile(outputFilename);
    ofstream outputFileLinenumbers(outputFilenameLinenumbers);
    ofstream dictFile("compressed/compacted_type_0", std::ios::app);

    // Read the first line from each file
    for (size_t i = 0; i < inputFiles.size(); ++i) {
        getline(inputFiles[i], currentLines[i]);
    }

    for (size_t i = 0; i < inputFilesLinenumbers.size(); ++i) {
        string line;
        getline(inputFilesLinenumbers[i], line);
        istringstream iss(line);
        int num;
        while (iss >> num) {
            currentLinenumbers[i].push_back(num);
        }
    }

    while (any_of(currentLines.begin(), currentLines.end(), [](const string& s) { return !s.empty(); })) {

        // set it to the smallest string is not empty
        // filter currentLines for non empty strings
        vector<string> filteredCurrentLines = currentLines;
        filteredCurrentLines.erase(remove_if(filteredCurrentLines.begin(), filteredCurrentLines.end(), [](const string& s) { return s.empty(); }), filteredCurrentLines.end());
        string it = *min_element(filteredCurrentLines.begin(),filteredCurrentLines.end());

        if (it.empty()) {
            // print out all of currentLines
            for (const string& line : currentLines) {
                LOG(WARNING) << "problem";
                LOG(WARNING) << line << '\n';
            }
        }

        set<int> itLinenumbers;
        for (size_t i = 0; i < currentLines.size(); ++i) {
            if (currentLines[i] == it) {
                for (int num : currentLinenumbers[i]) {
                    itLinenumbers.insert(num);
                }
            }
        }

        if (itLinenumbers.size() > num_row_groups * DICT_RATIO_THRESHOLD) {
            // Write it and itLinenumbers to dict file
            dictFile << it << '\n';
        } else {
            // Write it and itLinenumbers to output file
            outputFile << it << '\n';
            for (int num : itLinenumbers) {
                outputFileLinenumbers << num << ' ';
            }
            outputFileLinenumbers << '\n';
        }

        for (size_t i = 0; i < currentLines.size(); ++i) {
            if (currentLines[i] == it) {
                if (!getline(inputFiles[i], currentLines[i])) {
                    currentLines[i].clear();
                    inputFiles[i].close();
                    inputFilesLinenumbers[i].close();
                }
                else {
                    string line;
                    getline(inputFilesLinenumbers[i], line);
                    istringstream iss(line);
                    currentLinenumbers[i].clear();
                    int num;
                    while (iss >> num) {
                        currentLinenumbers[i].push_back(num);
                    }
                }
            }
        }
    }

    dictFile.close();
    outputFile.close();
    outputFileLinenumbers.close();
}

int compact(int num_groups) {
    
    // first read the total number of lines

    string filename = "compressed/" + to_string(num_groups - 1) + "/current_line_number";
    ifstream file(filename);
    string line;
    getline(file, line);
    size_t total_lines = stoul(line);
    size_t num_row_groups = total_lines / ROW_GROUP_SIZE + 1;

    // first handle the outliers
    vector<string> inputFilenames;
    vector<string> inputFilenamesLinenumbers;
    for (int i = 0; i < num_groups; ++i) {
        if (! fs::exists("compressed/" + to_string(i) + "/outlier")) { continue; }
        inputFilenames.push_back("compressed/" + to_string(i) + "/outlier");
        inputFilenamesLinenumbers.push_back("compressed/" + to_string(i) + "/outlier_lineno");
    }
    if (! inputFilenames.empty()) {
        mergeFiles(inputFilenames, inputFilenamesLinenumbers, "compressed/outlier", "compressed/outlier_lineno", num_row_groups);
    }
    
    for(int type = 1; type <= 63; ++type) {
        vector<string> inputFilenames;
        vector<string> inputFilenamesLinenumbers;

        for (int i = 0; i < num_groups; ++i) {

            // check if the file exists first
            string filename = "compressed/" + to_string(i) + "/compacted_type_" + to_string(type);
            if (! fs::exists(filename)) {
                continue;
            }

            inputFilenames.push_back("compressed/" + to_string(i) + "/compacted_type_" + to_string(type));
            inputFilenamesLinenumbers.push_back("compressed/" + to_string(i) + "/compacted_type_" + to_string(type) + "_lineno");
        }

        if (inputFilenames.empty()) {
            continue;
        }

        string outputFilename = "compressed/compacted_type_" + to_string(type);
        string outputFilenameLinenumbers = "compressed/compacted_type_" + to_string(type) + "_lineno";

        mergeFiles(inputFilenames, inputFilenamesLinenumbers, outputFilename, outputFilenameLinenumbers, num_row_groups);
    }

    LOG(INFO) << "Files merged " << endl;

    return 0;
}
