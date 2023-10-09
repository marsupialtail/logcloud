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
#include "../vfr.h"

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

The format of this file is thus

dictionary_str | dead_template_str | dead_template_pl | alive_template_str | alive_template_pl | outlier_str | outlier_pl | outlier_type_str | outlier_type_pl

*/

#define TEMPLATE_THRESHOLD (0.8)

void search_kauai(VirtualFileRegion * vfr, std::string query) {
    // first read in the byte_offsets, which should be the last sizeof(size_t) * 8 bytes of the file

    Compressor compressor(CompressionAlgorithm::ZSTD);
    size_t byte_offsets[8];
    vfr->vfseek(-sizeof(size_t) * 8, SEEK_END);
    vfr->vfread(byte_offsets, sizeof(size_t) * 8);

    vfr->vfseek(0, SEEK_SET);
    std::cout << vfr->vftell() << std::endl;

    // read in the dictionary
    size_t dictionary_str_size = byte_offsets[0];
    std::string dictionary_str;
    dictionary_str.resize(dictionary_str_size);
    vfr->vfread((void *) dictionary_str.data(), dictionary_str_size);
    std::string decompressed_dictionary_str = compressor.decompress(dictionary_str);

    // read in the dead templates
    size_t dead_template_str_size = byte_offsets[1] - byte_offsets[0];
    std::string dead_template_str;
    dead_template_str.resize(dead_template_str_size);
    vfr->vfseek(byte_offsets[0], SEEK_SET);
    vfr->vfread(&dead_template_str[0], dead_template_str_size);
    std::string decompressed_dead_template_str = compressor.decompress(dead_template_str);

    // read in the dead template posting lists
    size_t dead_template_pl_size = byte_offsets[2] - byte_offsets[1];
    std::string dead_template_pl;
    dead_template_pl.resize(dead_template_pl_size);
    vfr->vfseek(byte_offsets[1], SEEK_SET);
    vfr->vfread(&dead_template_pl[0], dead_template_pl_size);
    PListChunk dead_template_plist_chunk(std::move(dead_template_pl));
    std::vector<std::vector<plist_size_t>> & dead_template_plist = dead_template_plist_chunk.data();

    // read in the alive templates
    size_t alive_template_str_size = byte_offsets[3] - byte_offsets[2];
    std::string alive_template_str;
    alive_template_str.resize(alive_template_str_size);
    vfr->vfseek(byte_offsets[2], SEEK_SET);
    vfr->vfread(&alive_template_str[0], alive_template_str_size);
    std::string decompressed_alive_template_str = compressor.decompress(alive_template_str);

    // read in the alive template posting lists
    size_t alive_template_pl_size = byte_offsets[4] - byte_offsets[3];
    std::string alive_template_pl;
    alive_template_pl.resize(alive_template_pl_size);
    vfr->vfseek(byte_offsets[3], SEEK_SET);
    vfr->vfread(&alive_template_pl[0], alive_template_pl_size);
    PListChunk alive_template_plist_chunk(std::move(alive_template_pl));
    std::vector<std::vector<plist_size_t>> & alive_template_plist = alive_template_plist_chunk.data();

    // read in the outlier strings
    size_t outlier_str_size = byte_offsets[5] - byte_offsets[4];
    std::string outlier_str;
    outlier_str.resize(outlier_str_size);
    vfr->vfseek(byte_offsets[4], SEEK_SET);
    vfr->vfread(&outlier_str[0], outlier_str_size);
    std::string decompressed_outlier_str = compressor.decompress(outlier_str);

    // read in the outlier posting lists
    size_t outlier_pl_size = byte_offsets[6] - byte_offsets[5];
    std::string outlier_pl;
    outlier_pl.resize(outlier_pl_size);
    vfr->vfseek(byte_offsets[5], SEEK_SET);
    vfr->vfread(&outlier_pl[0], outlier_pl_size);
    PListChunk outlier_plist_chunk(std::move(outlier_pl));
    std::vector <std::vector<plist_size_t>> & outlier_plist = outlier_plist_chunk.data();

    // read in the outlier type strings
    size_t outlier_type_str_size = byte_offsets[7] - byte_offsets[6];
    std::string outlier_type_str;
    outlier_type_str.resize(outlier_type_str_size);
    vfr->vfseek(byte_offsets[6], SEEK_SET);
    vfr->vfread(&outlier_type_str[0], outlier_type_str_size);

    // read in the outlier type posting lists, first get the file size, the limit is file_size - byte_offsets[7] - 8 * sizeof(size_t)

    size_t outlier_type_pl_size = vfr->size() - byte_offsets[7] - 8 * sizeof(size_t);
    std::string outlier_type_pl;
    outlier_type_pl.resize(outlier_type_pl_size);
    vfr->vfseek(byte_offsets[7], SEEK_SET);
    vfr->vfread(&outlier_type_pl[0], outlier_type_pl_size);
    PListChunk outlier_type_plist_chunk(std::move(outlier_type_pl));
    std::vector <std::vector<plist_size_t>> & outlier_type_plist = outlier_type_plist_chunk.data();

    // first lookup the query in the dictionary
    std::string query_with_newlines = "\n" + query + "\n";
    size_t dictionary_idx = decompressed_dictionary_str.find(query_with_newlines);
    if (dictionary_idx != std::string::npos) {
        std::cout << "query matched dictionary item, give up " << query << std::endl;
        return;
    }

    // now lookup the query in dead templates
    size_t dead_template_idx = decompressed_dead_template_str.find(query);
    if (dead_template_idx != std::string::npos) {
        std::cout << "query matched dead template, give up " << query << std::endl;
        return;
    }

    // now lookup the query in alive templates
    size_t alive_template_idx = decompressed_alive_template_str.find(query);
    // you need to find which line your query matched, and then search the posting list for that line
    if (alive_template_idx != std::string::npos) {
        std::cout << "query matched alive template, search posting list " << query << std::endl;
        size_t line_no = 0;
        for (size_t i = 0; i < alive_template_idx; ++i) {
            if (decompressed_alive_template_str[i] == '\n') {
                line_no ++;
            }
        }

        std::cout << "line no " << line_no << std::endl;
        std::vector<plist_size_t> & posting_list = alive_template_plist[line_no];
        for (plist_size_t lineno : posting_list) {
            std::cout << lineno << " ";
        }
        std::cout << std::endl;
    }

}

int write_kauai(std::string filename, int num_chunks) {
    FILE *fp = fopen(("compressed/" + filename + ".kauai").c_str(), "wb");
    std::vector<size_t> byte_offsets = {};

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

    Compressor compressor(CompressionAlgorithm::ZSTD);
    std::string compressed_dictionary_str = compressor.compress(dictionary_str.c_str(), dictionary_str.size());
    fwrite(compressed_dictionary_str.c_str(), sizeof(char), compressed_dictionary_str.size(), fp);
    byte_offsets.push_back(ftell(fp));


    std::ifstream template_file(filename + ".templates");
    std::map<size_t, std::string> templates = {};
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
            size_t eid = std::stoul(line);

            // TODO: this is a hack. eid could be negative which will manifest as very large unsigned long.
            if (eid > 1000000) {
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

    std::vector<std::pair<size_t, size_t>> sorted_template_idx(template_indices.begin(), template_indices.end());
    std::sort(sorted_template_idx.begin(), sorted_template_idx.end(), [](const std::pair<size_t, size_t> &left, const std::pair<size_t, size_t> &right) {
        return left.second < right.second;
    });
    for (auto const& [key, val] : sorted_template_idx) {
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
    std::cout << "alive eids: ";
    for (size_t eid : alive_eids) {
        std::cout << eid << " ";
    }
    std::cout << std::endl;

    // you should write out your templates the order they appear in the posting lists, i.e. sort template_indices by value 
    // and then write out the templates in that order    

    std::string template_str = "\n";
    for (size_t dead_eid : dead_eids) {
        template_str += templates[dead_eid];
        template_str += "\n";
    }

    std::string compressed_template_str = compressor.compress(template_str.c_str(), template_str.size());
    fwrite(compressed_template_str.c_str(), sizeof(char), compressed_template_str.size(), fp);
    byte_offsets.push_back(ftell(fp));

    std::cout << "dead template str size: " << template_str.size() << std::endl;

    std::vector<std::vector<plist_size_t>> dead_template_posting_lists = {};
    for (size_t dead_eid : dead_eids) {
        dead_template_posting_lists.push_back(template_posting_lists[template_indices[dead_eid]]);
    }

    PListChunk plist2(std::move(dead_template_posting_lists));
    std::string serialized2 = plist2.serialize();
    fwrite(serialized2.c_str(), sizeof(char), serialized2.size(), fp);
    byte_offsets.push_back(ftell(fp));

    std::cout << "dead template pl size: " << serialized2.size() << std::endl;

    std::string alive_template_str = "\n";
    for (size_t alive_eid : alive_eids) {
        alive_template_str += templates[alive_eid];
        alive_template_str += "\n";
    }

    std::string compressed_alive_template_str = compressor.compress(alive_template_str.c_str(), alive_template_str.size());
    fwrite(compressed_alive_template_str.c_str(), sizeof(char), compressed_alive_template_str.size(), fp);
    byte_offsets.push_back(ftell(fp));

    std::cout << "alive template str size: " << alive_template_str.size() << std::endl;

    std::vector<std::vector<plist_size_t>> alive_template_posting_lists = {};
    for (size_t alive_eid : alive_eids) {
        alive_template_posting_lists.push_back(template_posting_lists[template_indices[alive_eid]]);
    }

    PListChunk plist4(std::move(alive_template_posting_lists));
    std::string serialized4 = plist4.serialize();
    fwrite(serialized4.c_str(), sizeof(char), serialized4.size(), fp);
    byte_offsets.push_back(ftell(fp));

    std::cout << "alive template pl size: " << serialized4.size() << std::endl;

    // concatenate the outlier strings into one string and compress that
    std::string outlier_str = "\n";
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

    std::string outlier_type_str = "\n";
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

int main(int argc, char* argv[]) {

    // first argument will be mode, which is either 'index' or 'search'. The second argument will be the number of chunks to index
    // or the string to search

    if (argc != 4) {
        std::cout << "Usage: " << argv[0] << " index <name> <num_chunks> or query <name> <query>" << std::endl;
        return 1;
    }

    std::string mode = argv[1];

    Aws::SDKOptions options;
    Aws::InitAPI(options);

    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = "us-west-2";
    clientConfig.connectTimeoutMs = 10000; // 10 seconds
    clientConfig.requestTimeoutMs = 10000; // 10 seconds
    Aws::S3::S3Client s3_client = Aws::S3::S3Client(clientConfig);

    if (mode == "index") {
        size_t num_chunks = std::stoul(argv[3]);
        write_kauai(argv[2], num_chunks);
    } else if (mode == "search") {
        std::string query = argv[3];
        // VirtualFileRegion * vfr = new DiskVirtualFileRegion("compressed/hadoop.kauai");
        VirtualFileRegion * vfr = new S3VirtualFileRegion(s3_client, "cluster-dump", std::string(argv[2]) + ".kauai", "us-west-2");
        search_kauai(vfr, query);
    } else {
        std::cout << "Usage: " << argv[0] << " <mode> <num_chunks>" << std::endl;
        return 1;
    }

    Aws::ShutdownAPI(options);    
}