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
#include <cerrno>
#include <set>

#include <arrow/api.h>
#include <arrow/status.h>
#include <arrow/memory_pool.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/properties.h>

#include "type_util.h"
#include <cctype>
#include <glog/logging.h>

#define DEBUG false

// feed the LogCrisp compressor chunks of 64MB. Since this is the only size it's been comprehensively tested at!
const int CHUNK_SIZE = 67108864;
const int TOTAL_SAMPLE_LINES = 3000000;
const int ROW_GROUP_SIZE = 100000;
const int ROW_GROUPS_PER_FILE = 10;
const int OUTLIER_THRESHOLD = 1000;
const std::string delimiters = ": \t";
const bool write_ts_in_parquet = false;

namespace fs = std::filesystem;

std::vector<std::string> tokenize(std::string str) {
    std::vector<std::string> tokens;
    char* cstr = new char[str.size() + 1];
    std::strcpy(cstr, str.c_str());

    char* token = std::strtok(cstr, delimiters.c_str());
    while (token != nullptr) {
        tokens.push_back(token);
        token = std::strtok(nullptr, delimiters.c_str());
    }

    delete[] cstr;
    return tokens;
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

arrow::Status compress_logs(std::vector<std::string> & files, std::string & index_name, 
    size_t group_number, size_t timestamp_bytes, std::string & timestamp_format, std::string & parquet_files_prefix) {
    std::vector<std::string> buffer;
    std::string line;

    size_t chunk_file_counter = 0;

    size_t global_line_count = 0;
    std::string current_chunk = "";
    std::vector<std::string> chunks = {};

    fs::create_directory("compressed/" + std::to_string(group_number));

    struct tm tm;
    memset(&tm, 0, sizeof(tm));

    std::shared_ptr<arrow::Schema> schema = arrow::schema({
        arrow::field("timestamp", arrow::uint64()),
        arrow::field("log", arrow::utf8())
    });

    std::vector<size_t> epoch_ts_vector = {};
    std::vector<std::string> log_vector = {};

    size_t current_line_number;
    if (group_number == 0){
       current_line_number = 0;
    } else {
        // read in the current_line_number from the previous group
        std::ifstream current_line_number_file("compressed/" + std::to_string(group_number - 1) + "/current_line_number");
        if (!current_line_number_file.is_open()) { return arrow::Status::ExecutionError("Failed to open current_line_number file"); }
        std::string line;
        std::getline(current_line_number_file, line);
        current_line_number = std::stoul(line);
    }

    std::map<int, std::map<std::string, std::vector<size_t>>> inverted_index = {};

    for (size_t f = 0; f < files.size() ; ++f) {

        std::ifstream infile(files[f]);
        if (!infile.is_open()) {
            std::cerr << "Error opening input file: " << files[f] << "\n";
            return arrow::Status::ExecutionError("Error opening input file");
        }

        LOG(INFO) << "Processing file " << files[f] << "\n";
        
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
                    LOG(WARNING) << "unable to backfill timestamp for a log line, most likely because the start of a file does not contain valid timestamp" << std::endl;
                    LOG(WARNING) << "this will lead to wrong extracted timestamps" << std::endl;
                    LOG(WARNING) << "attempted to parse " << extract_timestamp_from_this_line <<  " with " << timestamp_format << std::endl;
                }
                epoch_ts = last_timestamp;
            } else {
                last_timestamp = epoch_ts;
            }
            
            current_chunk += line + "\n";

            std::vector<std::string> tokens = tokenize(line);

            for (const std::string & token : tokens) {
                int type_of_token = get_type(token.c_str());
                if (inverted_index.find(type_of_token) == inverted_index.end()) {
                    inverted_index[type_of_token] = {};
                }
                if (inverted_index[type_of_token][token].size() == 0 || inverted_index[type_of_token][token].back() != current_line_number / ROW_GROUP_SIZE) {
                    inverted_index[type_of_token][token].push_back(current_line_number / ROW_GROUP_SIZE);
                }
            }


            epoch_ts_vector.push_back(epoch_ts);
            log_vector.push_back(line);
            current_line_number += 1;

            if (current_chunk.size() >= CHUNK_SIZE) {

                std::shared_ptr<arrow::Array> timestamp_array, log_array;
                arrow::UInt64Builder timestamp_builder;
                arrow::StringBuilder log_builder;

                if (chunk_file_counter == 9998) {
                    std::cerr << "Too many chunks. Exiting\n";
                    return arrow::Status::ExecutionError("Too many chunks. Exiting");
                }

                current_chunk = "";

                ARROW_RETURN_NOT_OK(timestamp_builder.AppendValues(epoch_ts_vector));
                ARROW_RETURN_NOT_OK(log_builder.AppendValues(log_vector));
                ARROW_RETURN_NOT_OK(timestamp_builder.Finish(&timestamp_array));
                ARROW_RETURN_NOT_OK(log_builder.Finish(&log_array));
                std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {timestamp_array, log_array});

                if (! write_ts_in_parquet) {
                    table = table->SelectColumns({1}).ValueOrDie();
                }

                arrow::Status s = write_parquet_file(parquet_files_prefix, table );
                if (!s.ok()) { return s;}

                epoch_ts_vector.clear();
                log_vector.clear();

            }
            
        }

        infile.close();
        
    } 

    std::shared_ptr<arrow::Array> timestamp_array, log_array;
    arrow::UInt64Builder timestamp_builder;
    arrow::StringBuilder log_builder;
    ARROW_RETURN_NOT_OK(timestamp_builder.AppendValues(epoch_ts_vector));
    ARROW_RETURN_NOT_OK(log_builder.AppendValues(log_vector));

    ARROW_RETURN_NOT_OK(timestamp_builder.Finish(&timestamp_array));
    ARROW_RETURN_NOT_OK(log_builder.Finish(&log_array));
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {timestamp_array, log_array});

    if (! write_ts_in_parquet) {
        table = table->SelectColumns({1}).ValueOrDie();
    }

    arrow::Status s = write_parquet_file(parquet_files_prefix, table );
    if (!s.ok()) { return s;}

    // write the current_line_number to a file
    std::ofstream current_line_number_file("compressed/" + std::to_string(group_number) + "/current_line_number");
    current_line_number_file << current_line_number;
    current_line_number_file.close();

    std::ofstream outlier_file("compressed/" + std::to_string(group_number) + "/outlier");
    std::ofstream outlier_lineno_file("compressed/" + std::to_string(group_number) + "/outlier_lineno");
    std::map<std::string, std::vector<size_t>> outlier_items;

    // iterate through the inverted index and write out the files
    for (auto const& [t, items] : inverted_index) {
        if (items.size() > OUTLIER_THRESHOLD) {
            std::ofstream compacted_type_file("compressed/" + std::to_string(group_number) + "/compacted_type_" + std::to_string(t));
            std::ofstream compacted_lineno_file("compressed/" + std::to_string(group_number) + "/compacted_type_" + std::to_string(t) + "_lineno");
            // iterate over the items
            for (auto const& [item, lineno] : items) {
                compacted_type_file << item << "\n";
                for (size_t num : lineno) {
                    compacted_lineno_file << num << " ";
                }
                compacted_lineno_file << "\n";
            }

            compacted_type_file.close();
            compacted_lineno_file.close();
        } else {
            // push all of items to outlier_items
            for (auto const& [item, lineno] : items) {
                outlier_items[item] = lineno;
            }
        }
    }

    for (auto const& [item, lineno] : outlier_items) {
        outlier_file << item << "\n";
        for (size_t num : lineno) {
            outlier_lineno_file << num << " ";
        }
        outlier_lineno_file << "\n";
    }

    outlier_file.close();
    outlier_lineno_file.close();

    return arrow::Status::OK();
}

extern "C" {
// expects index_prefix in the format bucket/index_name/split_id/index_name
void rex_python(size_t number_of_files,  const char ** filenames, const char * index_name, size_t group_number, size_t timestamp_bytes, 
    const char * timestamp_format, const char * parquet_files_prefix) {

    google::InitGoogleLogging("rottnest");

    std::vector<std::string> files;
    for (size_t i = 0; i < number_of_files; ++i) {
        files.push_back(std::string(filenames[i]));
    }

    std::string indexNameStr(index_name);
    std::string timestampFormatStr(timestamp_format);
    std::string parquetFilesPrefixStr(parquet_files_prefix);

    arrow::Status s = compress_logs(files, indexNameStr, group_number, timestamp_bytes, timestampFormatStr, parquetFilesPrefixStr);
    if (!s.ok()) {
        LOG(ERROR) << s.ToString() << "\n";
    }

    google::ShutdownGoogleLogging();
}

}

int main(int argc, char *argv[]) {
    if (argc < 6) {
        std::cerr << "Usage: " << argv[0] << " <input_filenames> <output_template_base> <group_number> <timestamp_bytes> <timestamp_prefix>\n";
        return -1;
    }

    google::InitGoogleLogging("rottnest-rexer");

    // get the number of input files
    auto files = std::vector<std::string>(argv + 1, argv + argc - 5);
    std::sort(files.begin(), files.end());

    std::string index_name = std::string(argv[argc - 5]);
    size_t group_number = std::stoi(argv[argc - 4]);
    size_t timestamp_bytes = std::stoi(argv[argc - 3]);
    std::string timestamp_format = argv[argc - 2];
    std::string parquet_files_prefix = argv[argc - 1];

    arrow::Status s = compress_logs(files, index_name, group_number, timestamp_bytes, timestamp_format, parquet_files_prefix);
    if (!s.ok()) {
        LOG(ERROR) << s.ToString() << "\n";
    }
    return 0;
}