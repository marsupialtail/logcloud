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

#define DEBUG false

// feed the LogCrisp compressor chunks of 64MB. Since this is the only size it's been comprehensively tested at!
#define CHUNK_SIZE 67108864
#define TOTAL_SAMPLE_LINES 3000000

namespace fs = std::filesystem;

extern "C" int trainer_wrapper(std::string sample_str, std::string output_path);
extern "C" int compressor_wrapper(std::string& chunk, std::string output_path, std::string template_path, int prefix);

void compress_chunk(int chunk_file_counter, std::string& current_chunk, std::string template_name, size_t group_number) {
    std::string dirName = "variable_" + std::to_string(chunk_file_counter);
    std::string tagName = "variable_tag_" + std::to_string(chunk_file_counter) + ".txt";

    // Check if the directory exists and remove it
    if (std::filesystem::exists(dirName)) {
        std::filesystem::remove_all(dirName); 
    }

    // Create the directory
    std::filesystem::create_directories(dirName);

    // Check if the file exists and remove it
    if (std::filesystem::exists(tagName)) {
        std::filesystem::remove(tagName); 
    }

    std::ostringstream oss;
    oss << "compressed/" + std::to_string(group_number) + "/chunk" << std::setw(4) << std::setfill('0') << chunk_file_counter;
    std::string chunk_filename = oss.str();
    compressor_wrapper(current_chunk, chunk_filename, template_name, chunk_file_counter);

    std::filesystem::rename("variable_" + std::to_string(chunk_file_counter), "compressed/" + std::to_string(group_number) + "/variable_" + std::to_string(chunk_file_counter));
    std::filesystem::rename("variable_" + std::to_string(chunk_file_counter) + "_tag.txt", "compressed/" + std::to_string(group_number) + "/variable_" + std::to_string(chunk_file_counter) + "_tag.txt");
}

int main(int argc, char** argv) {


    if (argc < 6) {
        std::cerr << "Usage: " << argv[0] << " <input_filenames> <output_template_base> <group_number> <timestamp_bytes> <timestamp_prefix>\n";
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

    std::string template_prefix = std::string(argv[argc - 4]) + "_" + std::to_string(group_number);

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

                    if (samples.size() < TOTAL_SAMPLE_LINES) {
                        samples.push_back(log_str);
                    } else {
                        std::uniform_int_distribution<size_t> distribution(0, global_line_count ++);
                        size_t j = distribution(generator);
                        if (j < TOTAL_SAMPLE_LINES) {
                            samples[j] = log_str;
                        }
                    }
                    
                    current_chunk += log_str + "\n";
                    log_file << log_str << "\n";

                    if (current_chunk.size() >= CHUNK_SIZE) {

                        if (chunk_file_counter == 9998) {
                            std::cerr << "Too many chunks. Exiting\n";
                            return -1;
                        }

                        chunks.push_back(std::move(current_chunk));
                        current_chunk = "";
                    }
                    
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

    // write the last chunk
    chunks.push_back(std::move(current_chunk));
    current_chunk = "";

    std::string samples_str = "";
    for (size_t i = 0; i < samples.size(); ++i) {
        samples_str += samples[i] + "\n";
    }
    samples.clear();
    trainer_wrapper(samples_str, template_prefix);
    
    for (size_t i = 0; i < chunks.size(); ++i) {
        compress_chunk(i, chunks[i], template_prefix, group_number);
    }

    return 0;
}