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

#define DEBUG false

// feed the LogCrisp compressor chunks of 64MB. Since this is the only size it's been comprehensively tested at!
const int CHUNK_SIZE = 67108864;
const int TOTAL_SAMPLE_LINES = 3000000;
const int ROW_GROUP_SIZE = 100000;
const int ROW_GROUPS_PER_FILE = 10;
const int OUTLIER_THRESHOLD = 1000;

const bool write_ts_in_parquet = false;

namespace fs = std::filesystem;

typedef std::pair<int, int> variable_t;
std::tuple<std::map<int, std::set<variable_t>>, std::map<int, std::vector<variable_t>>> get_variable_info(int total_chunks, size_t group_number) {
    
    int a, b;
    char underscore, V;
    
    std::map<variable_t, int> variable_to_type;
    std::map<int, std::set<variable_t>> chunk_variables;
    for (int chunk = 0; chunk < total_chunks; ++chunk) {
        std::string variable_tag_file = "compressed/" + std::to_string(group_number) + "/variable_" + std::to_string(chunk) + "_tag.txt";
        std::ifstream file(variable_tag_file);
        std::string line;
        while (std::getline(file, line)) {
            std::istringstream iss(line);
            std::string variable_str;
            int tag;
            iss >> variable_str >> tag;
            std::istringstream iss2(variable_str);
            iss2 >> V >> a >> underscore >> V >> b;
            variable_t variable = {a,b};
            variable_to_type[variable] = tag;
            chunk_variables[chunk].insert(variable);
        }
        file.close();
    }

    std::map<int, std::vector<variable_t>> eid_to_variables;
    for (const auto &key : variable_to_type) {
        variable_t variable = key.first;
        int eid = variable.first;
        eid_to_variables[eid].push_back(variable);
    }
    return {chunk_variables, eid_to_variables};
}

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
    size_t num_input_files = argc - 6;
    auto files = std::vector<std::string>(argv + 1, argv + argc - 5);
    std::sort(files.begin(), files.end());

    std::string index_name = std::string(argv[argc - 5]);
    size_t group_number = std::stoi(argv[argc - 4]);
    size_t timestamp_bytes = std::stoi(argv[argc - 3]);
    std::string timestamp_format = argv[argc - 2];

    std::string template_prefix = "compressed/" + index_name + "_" + std::to_string(group_number);
    std::string parquet_files_prefix = argv[argc - 1];

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

    std::vector<size_t> epoch_ts_vector = {};
    std::vector<std::string> log_vector = {};

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
            } else {
                last_timestamp = epoch_ts;
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

            
            epoch_ts_vector.push_back(epoch_ts);
            log_vector.push_back(line);

            if (current_chunk.size() >= CHUNK_SIZE) {

                std::shared_ptr<arrow::Array> timestamp_array, log_array;
                arrow::UInt64Builder timestamp_builder;
                arrow::StringBuilder log_builder;

                if (chunk_file_counter == 9998) {
                    std::cerr << "Too many chunks. Exiting\n";
                    return arrow::Status::ExecutionError("Too many chunks. Exiting");
                }

                chunks.push_back(std::move(current_chunk));
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

    // write the last chunk
    chunks.push_back(std::move(current_chunk));
    current_chunk = "";

    std::string samples_str = "";
    for (size_t i = 0; i < samples.size(); ++i) {
        samples_str += samples[i] + "\n";
    }
    samples.clear();
    trainer_wrapper(samples_str, template_prefix);
    
    for (size_t chunk = 0; chunk < chunks.size(); ++chunk) {
        compress_chunk(chunk, chunks[chunk], template_prefix, group_number);
    }


    /*
    
    Now go process the compressed chunks, extract using LogCrisp and produce variables
    
    */

    size_t total_chunks = chunks.size();
    auto [chunk_variables, eid_to_variables] = get_variable_info(total_chunks, group_number);
    std::set<int> touched_types = {};
    
    
    std::map<int, std::vector<std::string>> expanded_items;
    std::map<int, std::vector<size_t>> expanded_lineno;

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
         

    for (int chunk = 0; chunk < total_chunks; ++chunk) {

        std::map<variable_t, std::ifstream*> variable_files = {};
        for (const auto & variable: chunk_variables[chunk]) {
            variable_files[variable] = new std::ifstream("compressed/" + std::to_string(group_number) + "/variable_" + std::to_string(chunk) + "/E" 
                                    + std::to_string(variable.first) + "_V" + std::to_string(variable.second));
        }

        std::ostringstream oss;
        oss << "compressed/" + std::to_string(group_number) + "/chunk" << std::setw(4) << std::setfill('0') << chunk << ".eid";
        std::string chunk_filename = oss.str();
        std::cout << "processing chunk file: " << chunk_filename << std::endl;

        std::ifstream eid_file(chunk_filename);
        if (!eid_file.is_open()) {             
            std::cerr << "Error: " << std::strerror(errno);
            return arrow::Status::ExecutionError("Failed to open " + chunk_filename); }

        std::string line;
        std::vector<int> chunk_eids;
        while (std::getline(eid_file, line)) {
            chunk_eids.push_back(std::stoi(line));
        }
        eid_file.close();

        for (int eid : chunk_eids) {

            // maui shitz
            // if ( (current_line_number + 1) % ROW_GROUP_SIZE == 0) {
            //     std::cout << "current_line_number: " << current_line_number << std::endl;
            //     // compress and write the variable_buffer to disk
            //     Compressor compressor(CompressionAlgorithm::ZSTD);
            //     std::string compressed_str = compressor.compress(variable_buffer.c_str(), variable_buffer.size());
            //     fwrite(compressed_str.c_str(), sizeof(char), compressed_str.size(), fp);
            //     byte_offsets.push_back(ftell(fp));
            //     variable_buffer = "";
            // }

            if ((eid < 0) || (eid_to_variables.find(eid) == eid_to_variables.end()) ) {
                // this is an outlier or if this templates does not have variables, skip it
                current_line_number++;
                continue;
            } 
            
            else {
                auto this_variables = eid_to_variables[eid];
                std::map<int, std::vector<std::string>> type_vars;

                for (const auto &variable : this_variables) {
                    std::string item;
                    std::getline(*variable_files[variable], item);

                    // IMPORTANT: we will use the actual type of the object here instead of the type of the LogGrep variable
                    // this is to reduce dependency on the correctness of LogGrep and speed up exact match
                    // since with exact match you can now search just one type instead of all possible super-types
                    int t = get_type(item.c_str());
                    if (t == 0) {
                        std::cerr << "WARNING, null variable detected in LogCrisp." << chunk << " " << variable.first << " " << variable.second << "This variable is not indexed." << std::endl;
                        continue;
                    }
                    touched_types.insert(t);
                    // variable_buffer += item + " ";

                    type_vars[t].push_back(item);
                }
                // variable_buffer += "\n";

                for (const auto &entry : type_vars) {
                    int t = entry.first;
                    if (expanded_items.find(t) == expanded_items.end()) {
                        expanded_items[t] = {};
                        expanded_lineno[t] = {};
                    }
                    expanded_items[t].insert(expanded_items[t].end(), entry.second.begin(), entry.second.end());
                    expanded_lineno[t].resize(expanded_lineno[t].size() + entry.second.size(), current_line_number / ROW_GROUP_SIZE);
                }
                current_line_number++;
            }
        }
        // go close all the variable files
        for (const auto &kv : variable_files) {
            kv.second->close();
        }
    }

    // write the current_line_number to a file
    std::ofstream current_line_number_file("compressed/" + std::to_string(group_number) + "/current_line_number");
    current_line_number_file << current_line_number;
    current_line_number_file.close();

    std::map<int, std::ofstream*> compacted_type_files;
    std::map<int, std::ofstream*> compacted_lineno_files;
    std::ofstream outlier_file("compressed/" + std::to_string(group_number) + "/outlier");
    std::ofstream outlier_lineno_file("compressed/" + std::to_string(group_number) + "/outlier_lineno");

    for (const int &t : touched_types) {
        if (expanded_items[t].empty()) {return arrow::Status::ExecutionError("Error in variable extraction. No items detected for type " + std::to_string(t));}
        // Sort expanded_items and expanded_lineno based on expanded_items
        std::vector<std::pair<std::string, size_t>> paired;
        for (size_t i = 0; i < expanded_items[t].size(); ++i) {
            paired.emplace_back(expanded_items[t][i], expanded_lineno[t][i]);
        }

        std::sort(paired.begin(), paired.end(), 
                [](const std::pair<std::string, size_t> &a, const std::pair<std::string, size_t> &b) {
                return a.first == b.first ? a.second < b.second : a.first < b.first;
                });

        for (size_t i = 0; i < paired.size(); ++i) {
            expanded_items[t][i] = paired[i].first;
            expanded_lineno[t][i] = paired[i].second;
        }

        std::vector<std::string> compacted_items;
        std::vector<std::vector<size_t>> compacted_lineno;
        std::string last_item;

        for (size_t i = 0; i < expanded_items[t].size(); ++i) {
            if (expanded_items[t][i] != last_item) {
                compacted_items.push_back(expanded_items[t][i]);
                compacted_lineno.push_back({expanded_lineno[t][i]});
                last_item = expanded_items[t][i];
            } else {
                if(expanded_lineno[t][i] != compacted_lineno.back().back()) {
                    compacted_lineno.back().push_back(expanded_lineno[t][i]);
                }
            }
        }

        assert(compacted_items.size() == compacted_lineno.size());

        if (compacted_items.size() > OUTLIER_THRESHOLD) {
            if (compacted_type_files[t] == nullptr) {
                compacted_type_files[t] = new std::ofstream("compressed/" + std::to_string(group_number) + "/compacted_type_" + std::to_string(t));
                compacted_lineno_files[t] = new std::ofstream("compressed/" + std::to_string(group_number) + "/compacted_type_" + std::to_string(t) + "_lineno");
            }
            for (size_t i = 0; i < compacted_items.size(); ++i) {
                *compacted_type_files[t] << compacted_items[i] << "\n";

                for (int num : compacted_lineno[i]) {
                    *compacted_lineno_files[t] << num << " ";
                }
                *compacted_lineno_files[t] << "\n";
                
            }
        } else {
            for (size_t i = 0; i < compacted_items.size(); ++i) {
                outlier_file << compacted_items[i] << "\n";
                for (int num : compacted_lineno[i]) {
                    outlier_lineno_file << num << " ";
                }
                outlier_lineno_file << "\n";
            }
        }

        expanded_items[t].clear();
        expanded_lineno[t].clear();
        if (compacted_type_files[t] != nullptr) {
            compacted_type_files[t]->close();
            compacted_lineno_files[t]->close();
        }
    }

    // write final maui
    // if ( variable_buffer.size() > 0 ) {
    //     std::cout << "current_line_number: " << current_line_number << std::endl;
    //     // compress and write the variable_buffer to disk
    //     Compressor compressor(CompressionAlgorithm::ZSTD);
    //     std::string compressed_str = compressor.compress(variable_buffer.c_str(), variable_buffer.size());
    //     fwrite(compressed_str.c_str(), sizeof(char), compressed_str.size(), fp);
    //     variable_buffer = "";
    // }
    // fclose(fp);

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