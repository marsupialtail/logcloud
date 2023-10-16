#include "kauai.h"

#define ROW_GROUP_SIZE 100000

/*
The format of the kauai file is:

dictionary_str |  template_str | template_pl | outlier_str | outlier_pl | outlier_type_str | outlier_type_pl

search_kauai will return:
(0, {}): you should brute force
(1, {...}): list of row groups to search, no need to search Oahu/Hawaii
(2, {...}): list of row groups to search, but also need to search Oahu/Hawaii

Here is the search logic for exhaustive match request for single token
First lookup the token in the dictionary_str
If match:
    Return, brute force
If not match:
    Lookup in template str/pl
    If many matches * (have to define what this means)
        Return brute force
    Else
        Record row groups
        Lookup in outlier str/pl, add to the row groups
        Lookup in outlier type str/pl
        If match:
            Add to the row groups
            Return
        Else:
            Have to go search Oahu/Hawaii

For inexhaustive match request, you also have a top K limit.
First lookup the token in the dictionary_str
If match:
    Return, brute force
If not match:
    Lookup in template str/pl
    If matches > K
        Return (1, top K matched row groups)
    Else
        Record row groups
        Lookup in outlier str/pl, add to the row groups
        If matches > K
            Return (1, top K matched row groups)
        Else
            Lookup in outlier type str/pl
            If any match:
                Add to the row groups
                Return (1, row groups), might be fewer than K
            Else:
                Have to go search Oahu/Hawaii
                Return (2, row groups)
    
For phrase queries, things are a bit more complicated

*/

std::pair<int, std::vector<plist_size_t>> search_kauai(VirtualFileRegion * vfr, std::string query, int mode, int k) {

    // mode = 0 means exhaustive, 1 means inexhaustive
    // k is top K, has no effect if mode == 0

    // first read in the byte_offsets, which should be the last sizeof(size_t) * 6 bytes of the file

    if (mode == 0) {assert(false);}

    Compressor compressor(CompressionAlgorithm::ZSTD);
    size_t byte_offsets[8];
    vfr->vfseek(-sizeof(size_t) * 6, SEEK_END);
    vfr->vfread(byte_offsets, sizeof(size_t) * 6);

    vfr->vfseek(0, SEEK_SET);
    std::cout << vfr->vftell() << std::endl;

    // read in the dictionary
    size_t dictionary_str_size = byte_offsets[0];
    std::string dictionary_str;
    dictionary_str.resize(dictionary_str_size);
    vfr->vfread((void *) dictionary_str.data(), dictionary_str_size);
    std::string decompressed_dictionary_str = compressor.decompress(dictionary_str);

    // read in the dead templates
    size_t template_str_size = byte_offsets[1] - byte_offsets[0];
    std::string template_str;
    template_str.resize(template_str_size);
    vfr->vfseek(byte_offsets[0], SEEK_SET);
    vfr->vfread(&template_str[0], template_str_size);
    std::string decompressed_template_str = compressor.decompress(template_str);

    // read in the dead template posting lists
    size_t template_pl_size = byte_offsets[2] - byte_offsets[1];
    std::string template_pl;
    template_pl.resize(template_pl_size);
    vfr->vfseek(byte_offsets[1], SEEK_SET);
    vfr->vfread(&template_pl[0], template_pl_size);
    PListChunk template_plist_chunk(std::move(template_pl));
    std::vector<std::vector<plist_size_t>> & template_plist = template_plist_chunk.data();

    // read in the outlier strings
    size_t outlier_str_size = byte_offsets[3] - byte_offsets[2];
    std::string outlier_str;
    outlier_str.resize(outlier_str_size);
    vfr->vfseek(byte_offsets[2], SEEK_SET);
    vfr->vfread(&outlier_str[0], outlier_str_size);
    std::string decompressed_outlier_str = compressor.decompress(outlier_str);

    // read in the outlier posting lists
    size_t outlier_pl_size = byte_offsets[4] - byte_offsets[3];
    std::string outlier_pl;
    outlier_pl.resize(outlier_pl_size);
    vfr->vfseek(byte_offsets[3], SEEK_SET);
    vfr->vfread(&outlier_pl[0], outlier_pl_size);
    PListChunk outlier_plist_chunk(std::move(outlier_pl));
    std::vector <std::vector<plist_size_t>> & outlier_plist = outlier_plist_chunk.data();

    // read in the outlier type strings
    size_t outlier_type_str_size = byte_offsets[5] - byte_offsets[4];
    std::string outlier_type_str;
    outlier_type_str.resize(outlier_type_str_size);
    vfr->vfseek(byte_offsets[4], SEEK_SET);
    vfr->vfread(&outlier_type_str[0], outlier_type_str_size);

    // read in the outlier type posting lists, first get the file size, the limit is file_size - byte_offsets[7] - 8 * sizeof(size_t)

    size_t outlier_type_pl_size = vfr->size() - byte_offsets[5] - 6 * sizeof(size_t);
    std::string outlier_type_pl;
    outlier_type_pl.resize(outlier_type_pl_size);
    vfr->vfseek(byte_offsets[5], SEEK_SET);
    vfr->vfread(&outlier_type_pl[0], outlier_type_pl_size);
    PListChunk outlier_type_plist_chunk(std::move(outlier_type_pl));
    std::vector <std::vector<plist_size_t>> & outlier_type_plist = outlier_type_plist_chunk.data();

    // first lookup the query in the dictionary

    std::string line;
    size_t line_no = 0;

    std::istringstream iss(decompressed_dictionary_str);
    line_no = 0;
    while (std::getline(iss, line)) {
        size_t template_idx = line.find(query);
        if (template_idx != std::string::npos) {
            std::cout << "query matched dictionary item, brute force " << query << std::endl;
            return std::make_pair(0, std::vector<plist_size_t>());
        }
        line_no ++;
    }

    std::vector<plist_size_t> matched_row_groups = {};

    auto search_text = [] (std::string & query, std::string & source_str, 
        std::vector <std::vector<plist_size_t>> & plists, std::vector<plist_size_t> & matched_row_groups) {
        std::istringstream iss(source_str);
        size_t line_no = 0;
        std::string line;
        while (std::getline(iss, line)) {
            size_t template_idx = line.find(query);
            if (template_idx != std::string::npos) {
                std::cout << line <<  " ";
                std::cout << line_no << std::endl;
                std::vector<plist_size_t> & posting_list = plists[line_no];
                for (plist_size_t row_group : posting_list) {
                    std::cout << row_group << " ";
                    matched_row_groups.push_back(row_group);
                }
                std::cout << std::endl;
            }
            line_no ++;
        }
    };

    // now lookup the query in templates
    // read the templates line by line, and then search for the query in each line
    search_text(query, decompressed_template_str, template_plist, matched_row_groups);

    if (matched_row_groups.size() >= k) {
        std::cout << "inexact query for top K satisfied by template " << query << std::endl;
        return std::make_pair(1, matched_row_groups);
    } 

    search_text(query, decompressed_outlier_str, outlier_plist, matched_row_groups);

    if (matched_row_groups.size() >= k) {
        std::cout << "inexact query for top K satisfied by template and outlier " << query << std::endl;
        return std::make_pair(1, matched_row_groups);
    } 

    // now you have to go lookup in outlier types
    search_text(query, outlier_type_str, outlier_type_plist, matched_row_groups);

    if (matched_row_groups.size() >= k) {
        std::cout << "inexact query for top K satisfied by template, outlier and outlier types " << query << std::endl;
        return std::make_pair(1, matched_row_groups);
    } else {
        std::cout << "inexact query for top K not satisfied by template, outlier and outlier types " << query << std::endl;
        return std::make_pair(2, matched_row_groups);
    }

}

int write_kauai(std::string filename, int num_groups) {
    FILE *fp = fopen((filename + ".kauai").c_str(), "wb");
    std::vector<size_t> byte_offsets = {};

    std::set <std::string> dictionary = {};
    std::ifstream dictionary_file("compressed/compacted_type_0");
    std::string line;
    std::string dictionary_str = "";
    while (std::getline(dictionary_file, line)) {
        dictionary_str += line;
        dictionary_str += "\n";
    }

    Compressor compressor(CompressionAlgorithm::ZSTD);
    std::string compressed_dictionary_str = compressor.compress(dictionary_str.c_str(), dictionary_str.size());
    fwrite(compressed_dictionary_str.c_str(), sizeof(char), compressed_dictionary_str.size(), fp);
    byte_offsets.push_back(ftell(fp));

    std::vector<std::string> templates = {};
    std::vector<std::vector<plist_size_t>> template_posting_lists = {};
    std::vector<std::string> outliers = {};
    std::vector<std::vector<plist_size_t>> outlier_linenos = {};

    plist_size_t lineno = 0;

    for(size_t group_number = 0; group_number < num_groups; ++group_number){

        std::map<size_t, size_t> group_template_idx = {};
        std::ifstream template_file("compressed/" + filename + "_" + std::to_string(group_number) + ".templates");
        std::getline(template_file, line); // Read and discard the first line

        while (std::getline(template_file, line)) {
            line.erase(line.find_last_not_of("\n") + 1); // Remove newline characters

            std::vector<std::string> tokens;
            std::istringstream iss(line);
            for (std::string token; std::getline(iss, token, ' '); ) {
                tokens.push_back(token);
            }

            if (tokens.size() >= 3) {
                size_t key = std::stoul(tokens[0].substr(1)); // Extract and convert the key
                std::string value = "";
                for (size_t i = 2; i < tokens.size(); ++i) {
                    value += tokens[i];
                    if (i != tokens.size() - 1) {
                        value += " ";
                    }
                }

                templates.push_back(value);
                template_posting_lists.push_back({});
                group_template_idx[key] = templates.size() - 1;
            }
        }
        
        //print out the templates
        // for (auto const& [key, val] : templates) {
        //     std::cout << key << ": " << val << std::endl;
        // }

        size_t total_chunks = 0;
        while (true) {
            std::ostringstream oss;
            oss << "compressed/" + std::to_string(group_number) + "/chunk" << std::setw(4) << std::setfill('0') << total_chunks << ".eid";
            std::string chunk_filename = oss.str();
            if (std::filesystem::exists(chunk_filename)) {
                total_chunks++;
            } else {
                break;
            }
        }

        for (size_t chunk = 0; chunk < total_chunks; ++chunk) {
            std::ostringstream oss;
            oss << "compressed/" + std::to_string(group_number) + "/chunk" << std::setw(4) << std::setfill('0') << chunk << ".eid";
            std::string chunk_filename = oss.str();
            std::cout << "processing chunk file: " << chunk_filename << std::endl;

            oss.str("");
            oss << "compressed/" + std::to_string(group_number) + "/chunk" << std::setw(4) << std::setfill('0') << chunk << ".outlier";
            std::string outlier_filename = oss.str();

            std::ifstream eid_file(chunk_filename);
            std::ifstream outlier_file(outlier_filename);

            if (!eid_file.is_open()) { std::cerr << "Error opening input file: " << chunk_filename << "\n"; return 1;}

            std::string line;
            while (std::getline(eid_file, line)) {
                size_t eid = std::stoul(line);

                // TODO: this is a hack. eid could be negative which will manifest as very large unsigned long.
                if (eid > 1000000) {
                    std::string item;
                    std::getline(outlier_file, item);
                    outliers.push_back(item);
                    outlier_linenos.push_back({lineno / ROW_GROUP_SIZE});
                } else {
                    size_t idx = group_template_idx[eid];
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
    }

    // print out templates and template_posting_lists to files
    FILE * template_fp = fopen("compressed/template", "w");
    FILE * template_lineno_fp = fopen("compressed/template_lineno", "w");
    for (size_t i = 0; i < templates.size(); ++i) {
        fprintf(template_fp, "%s\n", templates[i].c_str());
        for (size_t j = 0; j < template_posting_lists[i].size(); ++j) {
            fprintf(template_lineno_fp, "%lu ", template_posting_lists[i][j]);
        }
        fprintf(template_lineno_fp, "\n");
    }
    fclose(template_fp);
    fclose(template_lineno_fp);

    // now remove templates whose posting lists are empty
    std::vector<std::string> new_templates = {};
    std::vector<std::vector<plist_size_t>> new_template_posting_lists = {};
    for (size_t i = 0; i < templates.size(); ++i) {
        if (template_posting_lists[i].size() > 0) {
            new_templates.push_back(templates[i]);
            new_template_posting_lists.push_back(template_posting_lists[i]);
        }
    }
    templates = new_templates;
    template_posting_lists = new_template_posting_lists;

    std::string template_str = "";
    for (size_t i = 0; i < templates.size(); ++i) {
        template_str += templates[i];
        template_str += "\n";
    }

    std::string compressed_template_str = compressor.compress(template_str.c_str(), template_str.size());
    fwrite(compressed_template_str.c_str(), sizeof(char), compressed_template_str.size(), fp);
    byte_offsets.push_back(ftell(fp));

    std::cout << "template str size: " << template_str.size() << std::endl;

    PListChunk plist2(std::move(template_posting_lists));
    std::string serialized2 = plist2.serialize();
    fwrite(serialized2.c_str(), sizeof(char), serialized2.size(), fp);
    byte_offsets.push_back(ftell(fp));

    std::cout << "template pl size: " << serialized2.size() << std::endl;

    // concatenate the outlier strings into one string and compress that
    std::string outlier_str = "";
    for (std::string outlier : outliers) {
        outlier_str += outlier;
        outlier_str += "\n";
    }

    std::string compressed_outlier_str = compressor.compress(outlier_str.c_str(), outlier_str.size());
    fwrite(compressed_outlier_str.c_str(), sizeof(char), compressed_outlier_str.size(), fp);
    byte_offsets.push_back(ftell(fp));

    std::cout << "compressed outlier str size: " << compressed_outlier_str.size() << std::endl;

    PListChunk plist(std::move(outlier_linenos));
    std::string serialized = plist.serialize();
    fwrite(serialized.c_str(), sizeof(char), serialized.size(), fp);
    byte_offsets.push_back(ftell(fp));

    std::cout << "outlier pl size: " << serialized.size() << std::endl;

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

    std::string compressed_outlier_type_str = compressor.compress(outlier_type_str.c_str(), outlier_type_str.size());
    fwrite(compressed_outlier_type_str.c_str(), sizeof(char), compressed_outlier_type_str.size(), fp);
    byte_offsets.push_back(ftell(fp));
    std::cout << "outlier_type str size: " << compressed_outlier_type_str.size() << std::endl;

    PListChunk plist3(std::move(outlier_type_linenos));
    std::string serialized3 = plist3.serialize();
    std::cout << "outlier type lineno size " << serialized3.size() << "\n";
    fwrite(serialized3.c_str(), sizeof(char), serialized3.size(), fp);

    // write out the byte offsets
    for (size_t byte_offset : byte_offsets) {
        std::cout << byte_offset << " ";
    }
    fwrite(byte_offsets.data(), sizeof(size_t), byte_offsets.size(), fp);

    //print out byte_offsets
    std::cout << "byte offsets: ";
    for (size_t byte_offset : byte_offsets) {
        std::cout << byte_offset << " ";
    }
    std::cout << std::endl;

    fclose(fp);
    return 0;
}