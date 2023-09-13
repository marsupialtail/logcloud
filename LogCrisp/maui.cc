#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <set>
#include <vector>
#include <iomanip>
#include <string>
#include <algorithm>
#include "plist.h"

#define ROW_GROUP_SIZE 100000

/*
The maui file will have three sections:
The first is things that if you match you are dead. These contain the common templates (most of them) and the dictionary items.
Posting lists are not even stored for these items. If a part of your query match this, then that part is useless in filtering. If 
entire query matches, your query is useless since it will have a lot of hits. In the future we might support some kind of time based
count binning for this as well but in the future!

The second is things that if you match you are done. These are the outlier types. This is ideal scenario, you can circumvent matching Oahu
and Hawaii. Posting lists are stored for these items. Currently you always search these, in future you can decide based on types.

The third is things that if you match, you need to record that you matched and still have to search other things. This is the outleir templates
Posting lists are stored for these. You always need to search these. 

*/

#define TEMPLATE_THRESHOLD (0.8)

int main(int argc, char* argv[]) {

    FILE *fp = fopen("compressed/hadoop.kauai", "wb");
    std::vector<size_t> byte_offsets = {};

    size_t num_chunks = std::stoul(argv[1]);

    std::set <std::string> dictionary = {};
    std::ifstream dictionary_file("compressed/compacted_type_0");
    std::string line;
    while (std::getline(dictionary_file, line)) {
        dictionary.insert(line);
    }

    // we should start these things with a newline, since when we search for exact match on item we just search for \nquery\n

    std::string dictionary_str = "\n";
    for (std::string word : dictionary) {
        dictionary_str += word;
        dictionary_str += "\n";
    }

    Compressor dictionary_compressor(CompressionAlgorithm::ZSTD);
    std::string compressed_dictionary_str = dictionary_compressor.compress(dictionary_str.data(), dictionary_str.size());
    fwrite(compressed_dictionary_str.data(), sizeof(char), compressed_dictionary_str.size(), fp);
    byte_offsets.push_back(compressed_dictionary_str.size());

    std::ifstream template_file("hadoop.templates");
    std::map<int, std::string> templates = {};
    std::string line;
    std::getline(template_file, line); // Read and discard the first line

    while (std::getline(template_file, line)) {
        line.erase(line.find_last_not_of("\n") + 1); // Remove newline characters

        std::vector<std::string> tokens;
        std::istringstream iss(line);
        for (std::string token; std::getline(iss, token, ' '); ) {
            tokens.push_back(token);
        }

        if (tokens.size() >= 3) {
            int key = std::stoi(tokens[0].substr(1)); // Extract and convert the key
            std::string value = "";
            for (size_t i = 2; i < tokens.size(); ++i) {
                value += tokens[i];
                if (i != tokens.size() - 1) {
                    value += " ";
                }
            }

            templates[key] = value;
        }
    }
    
    //print out the templates
    for (auto const& [key, val] : templates) {
        std::cout << key << ": " << val << std::endl;
    }

    size_t current_idx = 0;
    std::map<size_t, size_t> template_indices = {};
    std::vector<std::vector<plist_size_t>> template_posting_lists = {};
    std::vector<std::string> outliers = {};
    std::vector<std::vector<plist_size_t>> outlier_linenos = {};

    plist_size_t lineno = 0;
    for (size_t chunk = 0; chunk < num_chunks; ++chunk) {
        std::ostringstream oss;
        oss << "compressed/chunk" << std::setw(4) << std::setfill('0') << chunk << ".eid";
        std::string chunk_filename = oss.str();
        std::cout << "processing chunk file: " << chunk_filename << std::endl;

        oss.str("");
        oss << "compressed/chunk" << std::setw(4) << std::setfill('0') << chunk << ".outlier";
        std::string outlier_filename = oss.str();

        std::ifstream eid_file(chunk_filename);
        std::ifstream outlier_file(outlier_filename);

        if (!eid_file.is_open()) {
            std::cerr << "Error opening input file: " << chunk_filename << "\n";
            return 1;
        }

        std::string line;
        while (std::getline(eid_file, line)) {
            int eid = std::stoi(line);

            if (eid < 0) {
                std::string item;
                std::getline(outlier_file, item);
                outliers.push_back(item);
                outlier_linenos.push_back({lineno / ROW_GROUP_SIZE});
            } else {
                if (template_indices.find(eid) == template_indices.end()) {
                    template_indices[eid] = current_idx ++;
                    template_posting_lists.push_back({});
                }

                size_t idx = template_indices[eid];
                if (template_posting_lists[idx].size() == 0 || template_posting_lists[idx].back() != lineno / ROW_GROUP_SIZE) {
                    template_posting_lists[idx].push_back(lineno / ROW_GROUP_SIZE);
                }
            }
            lineno ++;
            if (lineno == -1) {
                std::cout << "overflow" << std::endl;
                return 1;
            }
            
        }
    }



    size_t total_row_groups = lineno / ROW_GROUP_SIZE;
    size_t template_threshold = TEMPLATE_THRESHOLD * total_row_groups;
    // if the length of the posting list is over TEMPLATE_THRESHOLD * total_row_groups, then it's useless.

    std::string alive_templates = "";
    std::string dead_templates = "";
    std::vector<size_t> alive_eids = {};
    std::vector<size_t> dead_eids = {};

    // most should be dead

    for (auto const& [key, val] : template_indices) {
        if (template_posting_lists[val].size() > template_threshold) {
            dead_templates += templates[key];
            dead_templates += "\n";
            dead_eids.push_back(key);
        } else {
            alive_templates += templates[key];
            alive_templates += "\n";
            alive_eids.push_back(key);
        }
    }

    // print out dead_templates and dead_eids
    std::cout << "template threshold row groups " << template_threshold << std::endl;
    std::cout << "dead eids: ";
    for (size_t eid : dead_eids) {
        std::cout << eid << " ";
    }
    std::cout << std::endl;

    // concatenate the outlier strings into one string and compress that
    std::string outlier_str = "";
    for (std::string outlier : outliers) {
        outlier_str += outlier;
        outlier_str += "\n";
    }

    Compressor compressor(CompressionAlgorithm::ZSTD);
    std::string compressed_outlier_str = compressor.compress(outlier_str.data(), outlier_str.size());
    fwrite(compressed_outlier_str.data(), sizeof(char), compressed_outlier_str.size(), fp);
    byte_offsets.push_back(compressed_outlier_str.size());

    std::cout << "compressed outlier str size: " << compressed_outlier_str.size() << std::endl;

    PListChunk plist(std::move(outlier_linenos));
    std::string serialized = plist.serialize();
    fwrite(serialized.c_str(), sizeof(char), serialized.size(), fp);
    byte_offsets.push_back(serialized.size());

    std::cout << "outlier pl size: " << serialized.size() << std::endl;

    // you should write out your templates the order they appear in the posting lists, i.e. sort template_indices by value 
    // and then write out the templates in that order

    std::vector<std::pair<size_t, size_t>> sorted_template_idx(template_indices.begin(), template_indices.end());

    std::sort(sorted_template_idx.begin(), sorted_template_idx.end(), [](const std::pair<size_t, size_t> &left, const std::pair<size_t, size_t> &right) {
        return left.second < right.second;
    });

    std::string template_str = "";
    for (auto const& [key, val] : sorted_template_idx) {
        template_str += templates[key];
        template_str += "\n";
    }

    std::string compressed_template_str = compressor.compress(template_str.data(), template_str.size());
    fwrite(compressed_template_str.data(), sizeof(char), compressed_template_str.size(), fp);
    byte_offsets.push_back(compressed_template_str.size());

    std::cout << "template str size: " << template_str.size() << std::endl;

    PListChunk plist2(std::move(template_posting_lists));
    std::string serialized2 = plist2.serialize();
    fwrite(serialized2.c_str(), sizeof(char), serialized2.size(), fp);
    byte_offsets.push_back(serialized2.size());

    std::cout << "template pl size: " << serialized2.size() << std::endl;

    std::string outlier_type_str = "";
    std::ifstream outlier_type_infile("compressed/outlier");
    std::ifstream outlier_type_lineno_infile("compressed/outlier_lineno");
    std::vector<std::vector<plist_size_t>> outlier_type_linenos = {};
    std::string outlier_type_line;
    while (std::getline(outlier_type_lineno_infile, line)) {
        std::getline(outlier_type_infile, outlier_type_line);
        outlier_type_str += outlier_type_line + "\n";
        std::istringstream iss(line);
        std::vector<plist_size_t> numbers;
        plist_size_t number;
        while (iss >> number) {
            number = number / ROW_GROUP_SIZE;
            if(numbers.size() > 0 && numbers.back() == number) {
                continue;
            }
            numbers.push_back(number);
        }
        outlier_type_linenos.push_back(numbers);
    }

    std::string compressed_outlier_type_str = compressor.compress(outlier_type_str.data(), outlier_type_str.size());
    fwrite(compressed_outlier_type_str.data(), sizeof(char), compressed_outlier_type_str.size(), fp);
    byte_offsets.push_back(compressed_outlier_type_str.size());
    std::cout << "outlier_type str size: " << compressed_outlier_type_str.size() << std::endl;

    PListChunk plist3(std::move(outlier_type_linenos));
    std::string serialized3 = plist3.serialize();
    std::cout << "outlier type lineno size " << serialized3.size() << "\n";
    fwrite(serialized3.c_str(), sizeof(char), serialized3.size(), fp);
    byte_offsets.push_back(serialized3.size());

    // write out the byte offsets
    fwrite(byte_offsets.data(), sizeof(size_t), byte_offsets.size(), fp);

    fclose(fp);
}