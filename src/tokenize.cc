#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cassert>
#include <cstring>
#include <iomanip>
#include <filesystem>
#include <random>
#include <algorithm>

namespace fs = std::filesystem;

#define symbol_TY 32
const int CharTable[128] = {32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,32,1,1,1,1,1,1,1,1,1,1,32,32,32,32,32,32,32,2,2,2,2,2,2,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,8,32,32,32,32,32,32,4,4,4,4,4,4,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,16,32,32,32,32,32};

int get_type(const char* query){
    int len = strlen(query);
    int Type = 0;
    for(int i = 0; i < len; i++){
        int c = (int)query[i];
        Type |= (c < 0 || c >= 128) ? symbol_TY : CharTable[c];
    }
    return Type;
}

int main(int argc, char** argv) {


    if (argc < 6) {
        std::cerr << "Usage: " << argv[0] << " <input_filenames> <output_template_base> <group_number> <timestamp_bytes> <timestamp_format>\n";
        return -1;
    }

    // get the number of input files
    size_t num_input_files = argc - 5;

    // files = sorted(os.sys.argv[1:-1])
    auto files = std::vector<std::string>(argv + 1, argv + argc - 4);
    std::sort(files.begin(), files.end());

    // Open the input file for reading
    std::vector<std::ifstream *> infiles (num_input_files);
    for (size_t i = 0; i < num_input_files; ++i) {
        infiles[i] = new std::ifstream(files[i]);
        if (!infiles[i]->is_open()) {
            std::cerr << "Error opening input file: " << files[i] << "\n";
            return -1;
        }
    }

    size_t timestamp_bytes = std::stoi(argv[argc - 2]);
    std::string timestamp_prefix = argv[argc - 1];
    size_t timestamp_prefix_size = timestamp_prefix.size();

    size_t group_number = std::stoi(argv[argc - 3]);

    std::string template_prefix = "compressed/" + std::string(argv[argc - 4]) + "_" + std::to_string(group_number);

    std::vector<std::string> buffer;
    std::string line;

    size_t chunk_file_counter = 0;

    size_t global_line_count = 0;
    std::string current_chunk = "";
    std::random_device rd;
    std::mt19937 generator(rd());
    std::vector<std::string> samples = {};
    std::vector<std::string> chunks = {};

    fs::create_directory("compressed/" + std::to_string(group_number));
    std::ofstream timestamp_file("compressed/" + std::to_string(group_number) + "/timestamp");
    std::ofstream log_file("compressed/" + std::to_string(group_number) + "/log");

    // check if timestamp_file and log_file have been successfully opened
    if (!timestamp_file.is_open() || !log_file.is_open()) {
        std::cerr << "Error opening timestamp or log file\n";
        return -1;
    }

    for (size_t f = 0; f < num_input_files; ++f) {

        std::cout << "Processing file " << files[f] << "\n";
        
        // use a do while loop here else you are going to miss the last line. ChatGPT is still not smart enough.
        do {
            std::getline(*infiles[f], line);

            // Remove the newline character (if it exists)
            if (!line.empty() && line.back() == '\n') {
                line.pop_back();
            }

            if (!(*infiles[f]) || line.substr(0, timestamp_prefix_size) == timestamp_prefix) {
                if (!buffer.empty()) {
                    
                    timestamp_file << buffer[0].substr(0, timestamp_bytes) << "\n";

                    std::string log_str;

                    for (size_t i = 0; i < buffer.size(); ++i) {
                        if (i == 0){
                            log_str += buffer[i].substr(timestamp_bytes);
                        } else {
                            log_str += buffer[i];
                        }
                    }
                    
                    current_chunk += log_str + "\n";
                    log_file << log_str << "\n";
                    
                }
                buffer.clear();
                // if we are actually at the end do not push back the line which is empty! This will mess up processing the next file
                if (*infiles[f]) buffer.push_back(line);
            } else {
                buffer.push_back(line);
            }
        } while (*infiles[f]);
        infiles[f]->close();
    
    }

    timestamp_file.close();
    log_file.close();

    return 0;
}