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

#include <arrow/api.h>
#include <arrow/status.h>
#include <arrow/memory_pool.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#define DEBUG false

#define CHUNK_SIZE 67108864
#define ROW_GROUP_SIZE 100000
#define ROW_GROUPS_PER_FILE 10
#define TOTAL_SAMPLE_LINES 100000
#define SAMPLE_FILE_LIMIT 1

namespace fs = std::filesystem;

extern "C" int trainer_wrapper(std::string sample_str, std::string output_path);
extern "C" int compressor_wrapper(std::string& chunk, std::string output_path, std::string template_path, int prefix);

void compress_chunk(int chunk_file_counter, std::string& current_chunk, std::string template_name) {
    std::string dirName = "variable_" + std::to_string(chunk_file_counter);
    std::string tagName = "variable_tag_" + std::to_string(chunk_file_counter) + ".txt";

    // Check if the directory exists and remove it
    if (std::filesystem::exists(dirName)) {
        std::filesystem::remove_all(dirName); // equivalent to shutil.rmtree in Python
    }

    // Create the directory
    std::filesystem::create_directories(dirName);

    // Check if the file exists and remove it
    if (std::filesystem::exists(tagName)) {
        std::filesystem::remove(tagName); // Remove the file
    }

    std::ostringstream oss;
    oss << "compressed/chunk" << std::setw(4) << std::setfill('0') << chunk_file_counter;
    std::string chunk_filename = oss.str();
    compressor_wrapper(current_chunk, chunk_filename, template_name, chunk_file_counter);

    std::filesystem::rename("variable_" + std::to_string(chunk_file_counter), "compressed/variable_" + std::to_string(chunk_file_counter));
    std::filesystem::rename("variable_" + std::to_string(chunk_file_counter) + "_tag.txt", "compressed/variable_" + std::to_string(chunk_file_counter) + "_tag.txt");
}

arrow::Status RunMain(int argc, char** argv) {

    bool trained = false;

    if (argc < 6) {
        std::cerr << "Usage: " << argv[0] << " <input_filenames> <output_chunk_base> <output_parquet_base> <timestamp_bytes> <timestamp_prefix>\n";
        return arrow::Status::Invalid("Invalid command line arguments");
    }

    if (fs::exists("chunks")) {
        fs::remove_all("chunks");
    }
    fs::create_directory("chunks");

    if (fs::exists("parquets")) {
        fs::remove_all("parquets");
    }
    fs::create_directory("parquets");

    if (fs::exists("compressed")) {
        fs::remove_all("compressed");
    }
    fs::create_directory("compressed");

#if DEBUG
    if (fs::exists("__sample__")) {
        fs::remove("__sample__");
    }
#endif

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
            return arrow::Status::Invalid("Invalid command line arguments");
        }
    }

    size_t timestamp_bytes = std::stoi(argv[argc - 2]);
    std::string timestamp_prefix = argv[argc - 1];
    size_t timestamp_prefix_size = timestamp_prefix.size();

    std::string parquet_files_prefix = "parquets/" + std::string(argv[argc - 3]);
    std::string chunk_files_prefix = "chunks/" + std::string(argv[argc - 4]);

    std::vector<std::string> buffer;
    std::string line;

    std::shared_ptr<arrow::Schema> schema = arrow::schema({
        arrow::field("timestamp", arrow::utf8()),
        arrow::field("log", arrow::utf8())
    });

    std::shared_ptr<arrow::Array> timestamp_array, log_array;
    arrow::StringBuilder timestamp_builder, log_builder;
    std::shared_ptr<arrow::Table> table = nullptr;
    auto writer_properties = parquet::WriterProperties::Builder().compression(parquet::Compression::ZSTD)->build();
    size_t parquet_file_counter = 0;
    size_t chunk_file_counter = 0;

    size_t global_line_count = 0;
    std::string current_chunk = "";
    std::random_device rd;
    std::mt19937 generator(rd());
    std::vector<std::string> samples = {};

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
                    
                    ARROW_RETURN_NOT_OK(timestamp_builder.Append(buffer[0].substr(0, timestamp_bytes)));

                    std::string log_str;

                    for (size_t i = 0; i < buffer.size(); ++i) {
                        if (i == 0){
                            log_str += buffer[i].substr(timestamp_bytes);
                        } else {
                            log_str += buffer[i];
                        }
                    }

                    if (!trained) {
                        if (samples.size() < TOTAL_SAMPLE_LINES) {
                            samples.push_back(log_str);
                        } else {
                            std::uniform_int_distribution<size_t> distribution(0, global_line_count ++);
                            size_t j = distribution(generator);
                            if (j < TOTAL_SAMPLE_LINES) {
                                samples[j] = log_str;
                            }
                        }
                    }

                    current_chunk += log_str + "\n";
                    if (current_chunk.size() >= CHUNK_SIZE) {

                        if (chunk_file_counter == 9998) {
                            std::cerr << "Too many chunks. Exiting\n";
                            return arrow::Status::Invalid("Too many chunks");
                        }

                        if (! trained) {
                            std::ostringstream oss;
                            oss << chunk_files_prefix << std::setw(4) << std::setfill('0') << chunk_file_counter++;
                            std::string chunk_filename = oss.str();
                            std::ofstream chunk_file(chunk_filename);
                            chunk_file << current_chunk;
                            chunk_file.close();
                        } else {
                            compress_chunk(chunk_file_counter++, current_chunk, "hadoop");
                        }
                        current_chunk = "";
                    }

                    ARROW_RETURN_NOT_OK(log_builder.Append(log_str));
                }
                buffer.clear();
                // if we are actually at the end do not push back the line which is empty! This will mess up processing the next file
                if (*infiles[f]) buffer.push_back(line);
            } else {
                buffer.push_back(line);
            }
        } while (*infiles[f]);
        

        ARROW_RETURN_NOT_OK(timestamp_builder.Finish(&timestamp_array));
        ARROW_RETURN_NOT_OK(log_builder.Finish(&log_array));
        std::shared_ptr<arrow::Table> this_table = arrow::Table::Make(schema, {timestamp_array, log_array});

        if (table == nullptr) {
            table = this_table;
        } else {
            ARROW_ASSIGN_OR_RAISE(table, arrow::ConcatenateTables({table, this_table}));
        }

        while (table->num_rows() >= ROW_GROUP_SIZE * ROW_GROUPS_PER_FILE) {
            // Write the table to a parquet file
            size_t write_lines = ROW_GROUP_SIZE * ROW_GROUPS_PER_FILE;
            std::string parquet_filename = parquet_files_prefix + std::to_string(parquet_file_counter++) + ".parquet";
            std::shared_ptr<arrow::io::FileOutputStream> outfile;
            ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(parquet_filename));
            ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*(table->Slice(0, write_lines)), arrow::default_memory_pool(), outfile, ROW_GROUP_SIZE, writer_properties));
            table = table->Slice(write_lines);
        }
        
        infiles[f]->close();

        if (!trained && (f >= SAMPLE_FILE_LIMIT -1 || f == num_input_files - 1)) {

#if DEBUG
            std::cout << "Writing samples to __sample__, sample size: " <<  samples.size() << "\n";
            std::ofstream sample_file("__sample__");
            for (size_t i = 0; i < samples.size(); ++i) {
                sample_file << samples[i] << "\n";
            }
            sample_file.close();
#endif
            // join the samples together into a single string delimited by newline character
            std::string samples_str = "";
            for (size_t i = 0; i < samples.size(); ++i) {
                samples_str += samples[i] + "\n";
            }
            trainer_wrapper(samples_str, "hadoop");
            trained = true;
            samples.clear();

            // now you need to go process all the flushed chunks and delete them after
            for (size_t i = 0; i < chunk_file_counter; ++i) {

                std::string flushed_chunk = "";
                std::ostringstream oss;
                oss << chunk_files_prefix << std::setw(4) << std::setfill('0') << i;
                std::ifstream chunk_file(oss.str());
                std::string line;
                while (std::getline(chunk_file, line)) {
                    flushed_chunk += line + "\n";
                }
                compress_chunk(i, flushed_chunk, "hadoop");
            }

            // remove the chunk folder
            if (fs::exists("chunks")) {
                fs::remove_all("chunks");
            }
        }

        arrow::MemoryPool* pool = arrow::default_memory_pool();
        std::cout << "Current Arrow memory usage" << pool->bytes_allocated() << std::endl;
    
    }

    // write the last chunk
    if (!trained){
        std::ostringstream oss;
        oss << chunk_files_prefix << std::setw(4) << std::setfill('0') << chunk_file_counter++;
        std::string chunk_filename = oss.str();
        std::ofstream chunk_file(chunk_filename);
        chunk_file << current_chunk;
        chunk_file.close();
        current_chunk = "";
    } else {
        compress_chunk(chunk_file_counter++, current_chunk, "hadoop");
    }
    
    // write the last parquet file
    std::string parquet_filename = parquet_files_prefix + std::to_string(parquet_file_counter++) + ".parquet";
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(parquet_filename));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, ROW_GROUP_SIZE, writer_properties));
    return arrow::Status::OK();
}

int main(int argc, char *argv[]) {
    
    arrow::Status s = RunMain(argc, argv);
    if (!s.ok()) {
        std::cout << s.ToString() << "\n";
    }
    return 0;
}

