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
#include <time.h>

#include <arrow/api.h>
#include <arrow/status.h>
#include <arrow/memory_pool.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/properties.h>

#define DEBUG false

// feed the LogCrisp compressor chunks of 64MB. Since this is the only size it's been comprehensively tested at!
#define CHUNK_SIZE 67108864
#define TOTAL_SAMPLE_LINES 3000000
const int ROW_GROUP_SIZE = 100000;
const int ROW_GROUPS_PER_FILE = 10;

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

inline bool isValidTimestamp(size_t timestamp) {
    // Define minimum and maximum valid epoch times
    size_t minValidTimestamp = 946684800; // January 1, 2000, 00:00:00 UTC
    size_t maxValidTimestamp = 2524608000; // January 1, 2050, 00:00:00 UTC

    return (timestamp >= minValidTimestamp && timestamp < maxValidTimestamp);
}

arrow::Status write_parquet_file(std::string parquet_files_prefix, std::shared_ptr<arrow::Table> & table) {

    // what we are going to do here is figure out first the last parquet file written, which could be None
    // then we are going to do a copy on write strategy to make sure that the all the parquet files except the last one are full

    // first figure out the last file in parquets

    auto writer_properties = parquet::WriterProperties::Builder().compression(parquet::Compression::ZSTD)->build();

    size_t parquet_file_counter = 0;
    while (fs::exists(parquet_files_prefix + std::to_string(parquet_file_counter) + ".parquet")) {
        parquet_file_counter++;
    }
    if (parquet_file_counter > 0) {
        parquet_file_counter--;
        std::string filename = parquet_files_prefix + std::to_string(parquet_file_counter) + ".parquet";
        //read in the last parquet file
        std::shared_ptr<arrow::io::RandomAccessFile> input;
        ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(filename));
        std::shared_ptr<arrow::Table> last_table;
        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &arrow_reader));
        ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&last_table));

        // concatenate the last table with the current table
        ARROW_ASSIGN_OR_RAISE(table, arrow::ConcatenateTables({last_table, table}));
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

    // write the rest
    if (table->num_rows() > 0) {
        std::string parquet_filename = parquet_files_prefix + std::to_string(parquet_file_counter) + ".parquet";
        std::shared_ptr<arrow::io::FileOutputStream> outfile;
        ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(parquet_filename));
        ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, ROW_GROUP_SIZE, writer_properties));
    }

    return arrow::Status::OK();
}

arrow::Status RunMain(int argc, char *argv[]) {

    // get the number of input files
    size_t num_input_files = argc - 5;
    auto files = std::vector<std::string>(argv + 1, argv + argc - 4);
    std::sort(files.begin(), files.end());

    std::string index_name = std::string(argv[argc - 4]);
    size_t group_number = std::stoi(argv[argc - 3]);
    size_t timestamp_bytes = std::stoi(argv[argc - 2]);
    std::string timestamp_format = argv[argc - 1];

    std::string template_prefix = "compressed/" + index_name + "_" + std::to_string(group_number);

    std::string parquet_files_prefix = "parquets/" + index_name;
    // assert the parquet directory exists
    if (!fs::exists("parquets/")) {
        std::cerr << "Parquet directory does not exist\n";
        return arrow::Status::ExecutionError("Parquet directory does not exist");
    }

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

    struct tm tm;
    memset(&tm, 0, sizeof(tm));

    std::shared_ptr<arrow::Schema> schema = arrow::schema({
        arrow::field("timestamp", arrow::uint64()),
        arrow::field("log", arrow::utf8())
    });
    std::shared_ptr<arrow::Array> timestamp_array, log_array;
    arrow::UInt64Builder timestamp_builder;
    arrow::StringBuilder log_builder;

    for (size_t f = 0; f < num_input_files; ++f) {

        std::ifstream infile(files[f]);
        if (!infile.is_open()) {
            std::cerr << "Error opening input file: " << files[f] << "\n";
            return arrow::Status::ExecutionError("Error opening input file");
        }

        std::cout << "Processing file " << files[f] << "\n";
        
        // use a do while loop here else you are going to miss the last line. ChatGPT is still not smart enough.

        size_t last_timestamp = 0;
        while (std::getline(infile, line)) {

            // Remove the newline character (if it exists)
            if (!line.empty() && line.back() == '\n') {
                line.pop_back();
            }
            if (line.empty()) {continue; }
                    
            std::string extract_timestamp_from_this_line = line.substr(0, timestamp_bytes);
            strptime(extract_timestamp_from_this_line.c_str(), timestamp_format.c_str(), &tm);
            // convert tm into epoch time in seconds
            size_t epoch_ts = mktime(&tm);

            if (!isValidTimestamp(epoch_ts)) {
                if (last_timestamp == 0) {
                    std::cout << "unable to backfill timestamp for a log line, most likely because the start of a file does not contain valid timestamp" << std::endl;
                    std::cout << "this will lead to wrong extracted timestamps" << std::endl;
                    std::cout << "attempted to parse " << extract_timestamp_from_this_line <<  " with " << timestamp_format << std::endl;
                }
                epoch_ts = last_timestamp;
            }

            if (samples.size() < TOTAL_SAMPLE_LINES) {
                samples.push_back(line);
            } else {
                std::uniform_int_distribution<size_t> distribution(0, global_line_count ++);
                size_t j = distribution(generator);
                if (j < TOTAL_SAMPLE_LINES) {
                    samples[j] = line;
                }
            }
            
            current_chunk += line + "\n";

            ARROW_RETURN_NOT_OK(timestamp_builder.Append(epoch_ts));
            ARROW_RETURN_NOT_OK(log_builder.Append(line));

            if (current_chunk.size() >= CHUNK_SIZE) {

                if (chunk_file_counter == 9998) {
                    std::cerr << "Too many chunks. Exiting\n";
                    return arrow::Status::ExecutionError("Too many chunks. Exiting");
                }

                chunks.push_back(std::move(current_chunk));
                current_chunk = "";
            }
            
        }

        infile.close();
        
    } 

    ARROW_RETURN_NOT_OK(timestamp_builder.Finish(&timestamp_array));
    ARROW_RETURN_NOT_OK(log_builder.Finish(&log_array));
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {timestamp_array, log_array});

    arrow::Status s = write_parquet_file(parquet_files_prefix, table);
    if (!s.ok()) { return s;}

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

    return arrow::Status::OK();
}

int main(int argc, char *argv[]) {
    if (argc < 6) {
        std::cerr << "Usage: " << argv[0] << " <input_filenames> <output_template_base> <group_number> <timestamp_bytes> <timestamp_prefix>\n";
        return -1;
    }
    arrow::Status s = RunMain(argc, argv);
    if (!s.ok()) {
        std::cout << s.ToString() << "\n";
    }
    return 0;
}