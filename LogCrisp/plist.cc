#include "plist.h"
#include "../fts.h"
#include "../vfr.h"
#include "cassert"
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <filesystem>
#include <numeric>
#include <map>


/*
This step is going to first discover all the compressed/compacted_type* files
For each of the compacted type, we are going to read in the data and write the plists
each compacted type will have its own blocking parameter, such that the each block is within X bytes compressed
we need to record all the blocking parameters for each compacted type
Then we are going to write the compressed blocks for each type into a single file, we need to remember the offset for each block

The layout of each compressed block
8 bytes indicating length of compressed strings | compressed strings | compressed posting list. This is not compressed again

The layout of the entire file
type_A_block_0 | type_A_block_1 | ... | type_B_block_0 | .... | compressed metadata page | 8 bytes indicating the length of the compressed metadata page

The metadata page will have the following data structures:
- 8 bytes indicating the number of types, N
- 8 bytes indicating the total number of blocks
- 8 bytes for each type in a list of length N, indicating the order of the types in the blocks, e.g. 1, 21, 53, 63
- 8 bytes for each type in a list of length N, indicating the offset into the block-byte-offset array for each type
- 8 bytes for each block, denoting block offset
*/

#define BLOCK_BYTE_LIMIT 1000000
#define ROW_GROUP_SIZE 100000
#define symbol_TY 32
using namespace std;
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

// figure out all the possible types from 1 to 63 that contains all the bits that type has
std::vector<int> get_all_types(int type) {
    std::vector<int> types = {};
    for (int i = 1; i <= 63; ++i) {
        if ((type & i) == type) {
            types.push_back(i);
        }
    }
    return types;
}

std::vector<plist_size_t> search_oahu(VirtualFileRegion * vfr, int query_type, std::vector<size_t> chunks, std::string query_str) {
    // read the metadata page

    // read the last 8 bytes to figure out the length of the metadata page
    vfr->vfseek(-sizeof(size_t), SEEK_END);
    size_t metadata_page_length;
    vfr->vfread(&metadata_page_length, sizeof(size_t));

    // read the metadata page
    vfr->vfseek(-sizeof(size_t) - metadata_page_length, SEEK_END);
    std::string metadata_page;
    metadata_page.resize(metadata_page_length);
    vfr->vfread(&metadata_page[0], metadata_page_length);

    // decompress the metadata page
    Compressor compressor(CompressionAlgorithm::ZSTD);
    std::string decompressed_metadata_page = compressor.decompress(metadata_page);

    // read the number of types
    size_t num_types = *reinterpret_cast<const size_t*>(decompressed_metadata_page.data());
    std::cout << "num types: " << num_types << "\n";

    // read the number of blocks
    size_t num_blocks = *reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + sizeof(size_t));
    std::cout << "num blocks: " << num_blocks << "\n";

    // read the type order
    std::vector<int> type_order;
    for (size_t i = 0; i < num_types; ++i) {
        type_order.push_back(*reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + 2 * sizeof(size_t) + i * sizeof(size_t)));
    }

    // print out what are the types
    for (int type : type_order) {
        std::cout << type << "\n";
    }

    // read the type offsets
    std::vector<size_t> type_offsets;
    for (size_t i = 0; i < num_types + 1; ++i) {
        type_offsets.push_back(*reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + 2 * sizeof(size_t) + num_types * sizeof(size_t) + i * sizeof(size_t)));
    }

    // read the block offsets
    std::vector<size_t> block_offsets;
    for (size_t i = 0; i < num_blocks + 1; ++i) {
        block_offsets.push_back(*reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + 2 * sizeof(size_t) + 2 * num_types * sizeof(size_t) + sizeof(size_t) + i * sizeof(size_t)));
    }

    // figure out where in the type_order is query_type
    auto it = std::find(type_order.begin(), type_order.end(), query_type);
    // if not found return empty vector
    if (it == type_order.end()) {
        return {};
    }

    size_t type_index = std::distance(type_order.begin(), it);
    std::cout << "type index: " << type_index << "\n";

    size_t type_offset = type_offsets[type_index];  
    size_t num_chunks = type_offsets.at(type_index + 1) - type_offset;

    std::cout << "num chunks: " << num_chunks << "\n";
    //print out chunks
    for (size_t i = 0; i < chunks.size(); ++i) {
        std::cout << chunks[i] << "\n";
    }

    // go through the blocks
    std::vector<plist_size_t> row_groups = {};

    #pragma omp parallel for
    for (size_t i = 0; i < std::min(chunks.size(), num_chunks); ++i) {
        size_t block_offset = block_offsets[type_offset + chunks[i]];
        size_t next_block_offset = block_offsets[type_offset + chunks[i] + 1];
        size_t block_size = next_block_offset - block_offset;
        // std::cout << "block size: " << block_size << "\n";
        // read the block

        VirtualFileRegion * local_vfr = vfr->slice(block_offset, block_size );
        // vfr->vfseek(block_offset, SEEK_SET);
        std::string block;
        block.resize(block_size);
        local_vfr->vfread(&block[0], block_size);

        // read the length of the compressed strings
        size_t compressed_strings_length = *reinterpret_cast<const size_t*>(block.data());
        // std::cout << "compressed strings length: " << compressed_strings_length << "\n";

        // read the compressed strings
        std::string compressed_strings = block.substr(sizeof(size_t), compressed_strings_length);
        std::string decompressed_strings = compressor.decompress(compressed_strings);

        // read the compressed posting list
        std::string compressed_plist = block.substr(sizeof(size_t) + compressed_strings_length);
        PListChunk plist(compressed_plist);

        // decompressed strings will be delimited by \n, figure out which lines actually contain the query_str
        std::istringstream iss(decompressed_strings);
        std::string line;
        size_t line_number = 0;
        while (std::getline(iss, line)) {
            // check if line contains query_str anywhere in the line
            if (("\n" + line + "\n").find(query_str) != std::string::npos) {
                // lookup the query string
                std::vector<plist_size_t> result = plist.lookup(line_number);
                #pragma omp critical
                {
                    row_groups.insert(row_groups.end(), result.begin(), result.end());
                }
            }
            ++line_number;
        }
    }

    return row_groups;
}

std::pair<std::map<int, size_t>, std::map<int, size_t>> write_oahu(std::string output_name) {

    // first figure out the number of types by listing all compressed/compacted_type* files
    std::vector<int> types;
    for (const auto & entry : std::filesystem::directory_iterator("compressed")) {
        std::string path = entry.path();
        if (path.find("compacted_type") != std::string::npos && path.find("lineno") == std::string::npos) {
            int type = stoi(path.substr(path.find("compacted_type") + 15));
            // 0 is for dictionary items. We don't need to process them
            if (type != 0) {
                types.push_back(type);
            }
        }
    }

    auto compressor = Compressor(CompressionAlgorithm::ZSTD);

    FILE *fp = fopen(("compressed/" + output_name + ".oahu").c_str(), "wb");
    std::vector<size_t> byte_offsets = {0};
    std::vector<size_t> type_offsets = {0};

    // print out types
    for (int type : types) {
        std::cout << type << "\n";
    }

    std::map<int, size_t> type_uncompressed_lines_in_block = {};
    std::map<int ,size_t> type_chunks;

    // go through the types
    for (int type : types) {
        std::string string_file_path = "compressed/compacted_type_" + std::to_string(type);
        std::string lineno_file_path = "compressed/compacted_type_" + std::to_string(type) + "_lineno";
        std::ifstream string_file(string_file_path);
        std::ifstream lineno_file(lineno_file_path);
        
        // we are just going to see how many strings can fit under BLOCK_BYTE_LIMIT bytes compressed. The posting list will be tiny compressed.
        std::string buffer = "";
        std::vector<std::vector<plist_size_t>> lineno_buffer = {};

        std::string str_line;
        std::string lineno_line;
        size_t uncompressed_lines_in_block = 0;
        size_t blocks_written = 0;
        size_t lines_in_buffer = 0;

        while (std::getline(string_file, str_line)) {

            buffer += str_line + "\n";
            lines_in_buffer += 1;

            std::getline(lineno_file, lineno_line);

            std::istringstream iss(lineno_line);
            std::vector<plist_size_t> numbers;
            plist_size_t number;
            while (iss >> number) {
                number = number / ROW_GROUP_SIZE;
                if(numbers.size() > 0 && numbers.back() == number) {
                    continue;
                }
                numbers.push_back(number);
            }
            lineno_buffer.push_back(numbers);

            if (uncompressed_lines_in_block == 0 && buffer.size() > BLOCK_BYTE_LIMIT / 2) {
                // compress the buffer
                std::string compressed_buffer = compressor.compress(buffer.c_str(), buffer.size());
                // figure out how many lines you need such that the compressed thing will have size BLOCK_BYTE_LIMIT
                uncompressed_lines_in_block = ((float) BLOCK_BYTE_LIMIT / (float)compressed_buffer.size()) * lines_in_buffer;
            }

            if (uncompressed_lines_in_block > 0 && lines_in_buffer == uncompressed_lines_in_block) {
                // we have a block
                // compress the buffer
                std::string compressed_buffer = compressor.compress(buffer.c_str(), buffer.size());
                PListChunk plist(std::move(lineno_buffer));
                std::string serialized3 = plist.serialize();
                // reset the buffer
                buffer = "";
                lines_in_buffer = 0;
                lineno_buffer = {};
                size_t compressed_buffer_size = compressed_buffer.size();
                fwrite(&compressed_buffer_size, sizeof(size_t), 1, fp);
                fwrite(compressed_buffer.c_str(), sizeof(char), compressed_buffer.size(), fp);
                fwrite(serialized3.c_str(), sizeof(char), serialized3.size(), fp);

                byte_offsets.push_back(byte_offsets.back() + compressed_buffer.size() + serialized3.size() + sizeof(size_t));
                blocks_written += 1;

            }
            
        }

        std::string compressed_buffer = compressor.compress(buffer.c_str(), buffer.size());
        PListChunk plist(std::move(lineno_buffer));
        std::string serialized3 = plist.serialize();
        // reset the buffer
        buffer = "";
        lineno_buffer = {};
        size_t compressed_buffer_size = compressed_buffer.size();
        fwrite(&compressed_buffer_size, sizeof(size_t), 1, fp);
        fwrite(compressed_buffer.c_str(), sizeof(char), compressed_buffer.size(), fp);
        fwrite(serialized3.c_str(), sizeof(char), serialized3.size(), fp);
        blocks_written += 1;

        std::cout << "type: " << type << " blocks written: " << blocks_written << "\n";
        type_chunks[type] = blocks_written;

        byte_offsets.push_back(byte_offsets.back() + compressed_buffer.size() + serialized3.size() + sizeof(size_t));

        type_offsets.push_back(byte_offsets.size() - 1);
        type_uncompressed_lines_in_block[type] = uncompressed_lines_in_block;
        std::cout << "type: " << type << " uncompressed lines in block: " << uncompressed_lines_in_block << "\n";
    }

    // write the metadata page
    // The metadata page will have the following data structures:
    // - 8 bytes indicating the number of types, N
    // - 8 bytes indicating the total number of blocks
    // - 8 bytes for each type in a list of length N, indicating the order of the types in the blocks, e.g. 1, 21, 53, 63
    // - 8 bytes for each type in a list of length N, indicating the offset into the block-byte-offset array for each type
    // - 8 bytes for each block, denoting block offset
    
    std::string metadata_page = "";

    // write the number of types
    size_t num_types = types.size();
    metadata_page += std::string((char *)&num_types, sizeof(size_t));

    // write the number of blocks
    size_t num_blocks = byte_offsets.size() - 1;
    metadata_page += std::string((char *)&num_blocks, sizeof(size_t));

    // write the type order
    for (int type : types) {
        metadata_page += std::string((char *)&type, sizeof(size_t));
    }

    // write the type offsets
    for (size_t type_offset : type_offsets) {
        metadata_page += std::string((char *)&type_offset, sizeof(size_t));
    }

    // write the block offsets
    for (size_t byte_offset : byte_offsets) {
        metadata_page += std::string((char *)&byte_offset, sizeof(size_t));
    }

    // compress the metadata page
    std::string compressed_metadata_page = compressor.compress(metadata_page.c_str(), metadata_page.size());
    size_t compressed_metadata_page_size = compressed_metadata_page.size();

    // write the compressed metadata page
    
    fwrite(compressed_metadata_page.c_str(), sizeof(char), compressed_metadata_page.size(), fp);
    fwrite(&compressed_metadata_page_size, sizeof(size_t), 1, fp);

    fclose(fp);

    return std::make_pair(type_chunks, type_uncompressed_lines_in_block);
}

/*
The layout for this file:
type_A_group_0_wavelet | type_A_group_0_logidx | type_A_group_1_wavelet | ... | type_B_group_0_wavelet | ... | compressed metadata page | 8 bytes indicating the length of the compressed metadata page
It is not expected to have too many groups, since one group is 100MB of the uncompressed term dictionary. 80 GB of original data produces maybe up to 2 GB term dictionaries of each type
The layout of the compressed metadata page
- 8 bytes indicating the number of types, N
- 8 bytes indicating the total number of groups
- 8 bytes for each type in a list of length N, indicating the order of the types in the blocks, e.g. 1, 21, 53, 63
- 8 bytes for each type in a list of length N, indicating the offset into the block-byte-offset array for each type
- 2 x 8 bytes for each group, denoting group offset for wavelet and logidx

Remember how the wavelet groups work. Each compacted_type variable is divided into chunks of approximately the same size. 
Each chunk has a whole number of lines. Some of these chunks could be grouped into a group. A group is 100M, configurable
*/

#define GROUP_BYTE_LIMIT 500000000
#define BRUTE_THRESHOLD 5

void write_hawaii(std::string filename, std::map<int, size_t> type_chunks, std::map<int, size_t> type_uncompressed_lines_in_block) {
    // FILE * fp = fopen(filename.c_str(), "wb");

    std::vector<size_t> byte_offsets = {};
    std::vector<size_t> type_offsets = {0};
    std::map<int, size_t> groups_per_type;
    std::map<int, size_t> chunks_in_group_per_type;

    FILE * fp = fopen((filename + ".hawaii").c_str(), "wb");

    for (auto item: type_uncompressed_lines_in_block) {
        int type = item.first;
        size_t chunks_written = type_chunks[type];
        size_t uncompressed_lines_in_block = item.second;

        if (type_chunks[type] < BRUTE_THRESHOLD) {
            continue;
        }

        std::string string_file_path = "compressed/compacted_type_" + std::to_string(type);
        std::ifstream string_file(string_file_path);
        std::string buffer = "\n";

        std::string str_line;
        size_t lines_in_buffer = 0;
        size_t chunks_in_buffer = 0;

        // figure out the size of the compacted_type_* file
        std::ifstream infile(string_file_path, std::ifstream::ate | std::ifstream::binary);
        size_t uncompressed_file_size = infile.tellg();
        infile.close();

        // figure out how many chunks will be 100MB based on total number of chunks (chunks_written) and total file size

        size_t chunks_in_group;

        if(uncompressed_lines_in_block > 0 && chunks_written > 1 && uncompressed_file_size > GROUP_BYTE_LIMIT * 2) {
            chunks_in_group = (chunks_written / (uncompressed_file_size / GROUP_BYTE_LIMIT)) + 1;
        } else {
            chunks_in_group = -1;
        }

        chunks_in_group_per_type[type] = chunks_in_group;

        size_t total_groups = 1;

        std::vector<std::string> buffers = {};

        while (std::getline(string_file, str_line)) {
            buffer += str_line + "\n";
            lines_in_buffer += 1;

            if (chunks_in_group != -1 && lines_in_buffer % uncompressed_lines_in_block == 0) {
                chunks_in_buffer += 1;
                if (chunks_in_group != -1 && chunks_in_buffer == chunks_in_group) {

                    auto [wavelet_tree, log_idx, C] = bwt_and_build_wavelet(buffer.data(), uncompressed_lines_in_block);

                    byte_offsets.push_back(ftell(fp));
                    write_wavelet_tree_to_disk(wavelet_tree, C, buffer.size(), fp);
                    byte_offsets.push_back(ftell(fp));
                    write_log_idx_to_disk(log_idx, fp);
                    
                    buffer = "";
                    lines_in_buffer = 0;
                    chunks_in_buffer = 0;
                    total_groups += 1;
                }
            }
        }

        auto [wavelet_tree, log_idx, C] = bwt_and_build_wavelet(buffer.data(), uncompressed_lines_in_block);
        byte_offsets.push_back(ftell(fp));
        write_wavelet_tree_to_disk(wavelet_tree, C, buffer.size(), fp);
        byte_offsets.push_back(ftell(fp));
        write_log_idx_to_disk(log_idx, fp);

        type_offsets.push_back(byte_offsets.size());
        groups_per_type[type] = total_groups;

    }
    // print out groups per type
    for (auto item: groups_per_type) {
        std::cout << item.first << " " << item.second << "\n";
    }

/*
Reminder:
The layout of the compressed metadata page
- 8 bytes indicating the number of types, N
- 8 bytes indicating the total number of groups
- 8 bytes for each type in a list of length N, indicating the order of the types in the blocks, e.g. 1, 21, 53, 63
- 8 bytes for each type in a list of length N, indicating the number of chunks in a group for type N
- 8 bytes for each type in a list of length N, indicating the offset into the block-byte-offset array for each type
- 2 x 8 bytes for each group, denoting group offset for wavelet and logidx
*/

    std::string metadata_page = "";

    size_t num_types = groups_per_type.size();
    metadata_page += std::string((char *)&num_types, sizeof(size_t));

    size_t num_groups = byte_offsets.size() / 2;
    metadata_page += std::string((char *)&num_groups, sizeof(size_t));

    for (auto item: groups_per_type) {
        metadata_page += std::string((char *)&item.first, sizeof(size_t));
    }

    for (auto item: chunks_in_group_per_type) {
        metadata_page += std::string((char *)&item.second, sizeof(size_t));
    }

    for (size_t type_offset : type_offsets) {
        metadata_page += std::string((char *)&type_offset, sizeof(size_t));
    }

    for (size_t byte_offset : byte_offsets) {
        metadata_page += std::string((char *)&byte_offset, sizeof(size_t));
    }

    std::string compressed_metadata_page = Compressor(CompressionAlgorithm::ZSTD).compress(metadata_page.c_str(), metadata_page.size());
    size_t compressed_metadata_page_size = compressed_metadata_page.size();

    fwrite(compressed_metadata_page.c_str(), sizeof(char), compressed_metadata_page.size(), fp);
    fwrite(&compressed_metadata_page_size, sizeof(size_t), 1, fp);
    
    fclose(fp);
}

std::set<size_t> search_hawaii(VirtualFileRegion * vfr, int type, std::string query) {
    // read in the metadata page size and then the metadata page and then decompress everything
    
    // DiskVirtualFileRegion vfr(filename.c_str());
    vfr->vfseek(-sizeof(size_t), SEEK_END);
    size_t metadata_page_length;
    vfr->vfread(&metadata_page_length, sizeof(size_t));

    // read the metadata page
    vfr->vfseek(-sizeof(size_t) - metadata_page_length, SEEK_END);
    std::string metadata_page;
    metadata_page.resize(metadata_page_length);
    vfr->vfread(&metadata_page[0], metadata_page_length);

    // decompress the metadata page
    Compressor compressor(CompressionAlgorithm::ZSTD);
    std::string decompressed_metadata_page = compressor.decompress(metadata_page);

    // read the number of types
    size_t num_types = *reinterpret_cast<const size_t*>(decompressed_metadata_page.data());
    std::cout << "num types: " << num_types << "\n";

    // read the number of groups
    size_t num_groups = *reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + sizeof(size_t));
    std::cout << "num groups: " << num_groups << "\n";

    // read the type order
    std::vector<int> type_order;
    for (size_t i = 0; i < num_types; ++i) {
        type_order.push_back(*reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + 2 * sizeof(size_t) + i * sizeof(size_t)));
        std::cout << "type order: " << type_order[i] << "\n";
    }

    auto it = std::find(type_order.begin(), type_order.end(), type);
    size_t type_index = std::distance(type_order.begin(), it);

    // if not found return empty
    if (type_index == num_types) {
        return {(size_t)-1};
    }

    std::vector<int> chunks_in_group;
    for (size_t i = 0; i < num_types; ++i) {
        chunks_in_group.push_back(*reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + 2 * sizeof(size_t) + num_types * sizeof(size_t) + i * sizeof(size_t)));
        std::cout << "chunks in group: " << chunks_in_group[i] << "\n";
    }

    size_t chunks_in_group_for_type = chunks_in_group[type_index];

    // read the type offsets
    std::vector<size_t> type_offsets;
    for (size_t i = 0; i < num_types + 1; ++i) {
        type_offsets.push_back(*reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + 2 * sizeof(size_t) + 2 * num_types * sizeof(size_t) + i * sizeof(size_t)));
        std::cout << "type offsets: " << type_offsets[i] << "\n";
    }

    // read the group offsets
    std::vector<size_t> group_offsets;
    for (size_t i = 0; i < num_groups * 2; ++i) {
        group_offsets.push_back(*reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + 2 * sizeof(size_t) + 3 * num_types * sizeof(size_t) + sizeof(size_t) + i * sizeof(size_t)));
        std::cout << "group offsets: " << group_offsets[i] << "\n";
    }    

    size_t type_offset = type_offsets[type_index];
    size_t num_iters = type_offsets[type_index + 1] - type_offsets[type_index];

    // go through the groups
    std::set<size_t> chunks = {};
    #pragma omp parallel for
    for (size_t i = type_offset; i < num_iters; i += 2) {

        // delay this thread by a random number of seconds under 1 second
        std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 1000));

        size_t group_id = i / 2;
        size_t group_chunk_offset = group_id * chunks_in_group_for_type;

        size_t wavelet_offset = group_offsets[i];
        size_t logidx_offset = group_offsets[i + 1];
        size_t next_wavelet_offset = group_offsets[i + 2];
        size_t wavelet_size = logidx_offset - wavelet_offset;
        size_t logidx_size = next_wavelet_offset - logidx_offset;

        // note that each threads operates on a slice, which is a copy with own cursor. 
        VirtualFileRegion * wavelet_vfr  = vfr->slice(wavelet_offset, wavelet_size);
        VirtualFileRegion * logidx_vfr = vfr->slice(logidx_offset, logidx_size);

        auto matched_pos = search_disk(wavelet_vfr, logidx_vfr, query);

        //print out matched_pos
        #pragma omp critical
        {
            for (auto pos : matched_pos) {
                std::cout << "pos " << pos << "\n";
                chunks.insert(group_chunk_offset + pos);
            }
        }
    }
    return chunks;
}

std::vector<size_t> search(std::string query) {

    std::string processed_query = "";
    for (int i = 0; i < query.size(); i++) {
        if (query[i] != '\n') {
            processed_query += query[i];
        }
    }

    std::cout << "query: " << processed_query << "\n";
    
    int query_type = get_type(processed_query.c_str());
    std::cout << "deduced type: " << query_type << "\n";
    std::vector<int> types_to_search = get_all_types(query_type);

    Aws::SDKOptions options;
    Aws::InitAPI(options);

    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = "us-west-2";
    clientConfig.connectTimeoutMs = 10000; // 10 seconds
    clientConfig.requestTimeoutMs = 10000; // 10 seconds
    Aws::S3::S3Client s3_client = Aws::S3::S3Client(clientConfig);
    
    // print out types to search
    for (int type: types_to_search) {
        std::cout << "type to search: " << type << "\n";
    }

    std::vector<size_t> results;

    for (int type: types_to_search) {

        std::cout << "searching type " << type << "\n";

        // std::set<size_t> result =  search_hawaii(new DiskVirtualFileRegion("hadoop.hawaii"), type, query);
        std::set<size_t> result =  search_hawaii(new S3VirtualFileRegion(s3_client, "cluster-dump", "hadoop_large.hawaii", "us-west-2"), type, query);
        for (size_t r : result) {
            std::cout << r << "\n";
        }

        if (result == std::set<size_t>{(size_t)-1}) {
            std::cout << "type not found, brute forcing Oahu\n";
            std::vector<size_t> chunks_to_search(BRUTE_THRESHOLD);
            std::iota(chunks_to_search.begin(), chunks_to_search.end(), 0);
            VirtualFileRegion * vfr_oahu = new S3VirtualFileRegion(s3_client, "cluster-dump", "hadoop.oahu", "us-west-2");
            // VirtualFileRegion * vfr_oahu = new DiskVirtualFileRegion("compressed/hadoop.oahu");
            std::vector<plist_size_t> found = search_oahu(vfr_oahu, type , chunks_to_search, query);
            for (plist_size_t r : found) {
                std::cout << "result: " << r << "\n";
            }
        } else {
            std::vector<size_t> result_vec(result.begin(), result.end());
            VirtualFileRegion * vfr_oahu = new S3VirtualFileRegion(s3_client, "cluster-dump", "hadoop.oahu", "us-west-2");
            // VirtualFileRegion * vfr_oahu = new DiskVirtualFileRegion("compressed/hadoop.oahu");
            std::vector<plist_size_t> found = search_oahu(vfr_oahu, type , result_vec, query);
            for (plist_size_t r : found) {
                std::cout << "result: " << r << "\n";
                results.push_back(r);
            }
        }
    }

    Aws::ShutdownAPI(options);
    return results;
}

int main(int argc, char *argv[]) {

    std::string mode = argv[1];

    if (mode == "index") {
        auto [type_chunks, type_uncompressed_lines_in_block] = write_oahu("hadoop");    
        write_hawaii("hadoop", type_chunks, type_uncompressed_lines_in_block);
    } else if (mode == "search") {
        std::string query = argv[2];
        std::vector<size_t> results = search(query);
    } else {
        std::cout << "Usage: " << argv[0] << " <mode> <optional:query>" << std::endl;
        return 1;
    }

}