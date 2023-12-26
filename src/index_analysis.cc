#include "cassert"
#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <filesystem>
#include <numeric>
#include <map>

#include "plist.h"
#include "vfr.h"
#include "compressor.h"
#include "type_util.h"
#include "roaring.hh" // the amalgamated roaring.hh includes roaring64map.hh
#include "roaring.c"


void analyze_oahu(std::string filename) {

    VirtualFileRegion * vfr = new DiskVirtualFileRegion(filename);

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
    // read the number of blocks
    size_t num_blocks = *reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + sizeof(size_t));

    // read the type order
    std::vector<int> type_order;
    for (size_t i = 0; i < num_types; ++i) {
        type_order.push_back(*reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + 2 * sizeof(size_t) + i * sizeof(size_t)));
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

    // print out the block offsets
    for (size_t i = 0; i < num_blocks + 1; ++i) {
        std::cout << "Block " << i << " offset: " << block_offsets[i] << std::endl;
    }

    size_t total_compressed_strings_length = 0;
    size_t total_compressed_plist_length = 0;
    size_t total_roaring_length = 0;
    size_t total_csr_length = 0;

    for (size_t i = 0; i < num_blocks; ++i) {
        size_t block_offset = block_offsets[i];
        size_t next_block_offset = block_offsets[i + 1];
        size_t block_size = next_block_offset - block_offset;
        // LOG(INFO) << "block size: " << block_size << "\n";
        // read the block

        VirtualFileRegion * local_vfr = vfr->slice(block_offset, block_size );
        // vfr->vfseek(block_offset, SEEK_SET);
        std::string block;
        block.resize(block_size);
        local_vfr->vfread(&block[0], block_size);

        // read the length of the compressed strings
        size_t compressed_strings_length = *reinterpret_cast<const size_t*>(block.data());
        // LOG(INFO) << "compressed strings length: " << compressed_strings_length << "\n";

        // read the compressed strings
        std::string compressed_strings = block.substr(sizeof(size_t), compressed_strings_length);

        // read the compressed posting list
        std::string compressed_plist = block.substr(sizeof(size_t) + compressed_strings_length);
        PListChunk plist(compressed_plist);
        std::vector<std::vector<plist_size_t>> &plist_data = plist.data();

        CompressionAlgorithm compression_algorithm = CompressionAlgorithm::ZSTD;
        Compressor compressor(compression_algorithm);

        std::vector<size_t> csr_offsets = {0};
        std::vector<size_t> values = {};
        
        size_t block_roaring_length = 0;
        std::string totalserializedString = "";
        std::vector<size_t> roaring_offsets = {0};

        for (std::vector<plist_size_t> &plist : plist_data) {

            roaring::Roaring64Map r;
            
            csr_offsets.push_back(csr_offsets.back() + plist.size());

            for (plist_size_t i : plist) {
                r.add((uint64_t)i);
                values.push_back(i);
            }
            uint32_t expectedSize = r.getSizeInBytes();
            char* serializedBitmap = new char[expectedSize];
            r.write(serializedBitmap);
            // Create a string from the serialized data
            std::string serializedString(serializedBitmap, expectedSize);
            delete[] serializedBitmap;
            totalserializedString += serializedString;
            roaring_offsets.push_back(roaring_offsets.back() + serializedString.size());

        }

        std::string compressedString = compressor.compress(totalserializedString.c_str(), totalserializedString.size());
        std::string roaring_offsets_string((char*)roaring_offsets.data(), roaring_offsets.size() * sizeof(size_t));
        std::string compressed_roaring_offsets = compressor.compress(roaring_offsets_string.c_str(), roaring_offsets_string.size());
        block_roaring_length = compressedString.size() + compressed_roaring_offsets.size();

        // now compress csr_offsets and values back to back and see the size
        std::string csr_offsets_string((char*)csr_offsets.data(), csr_offsets.size() * sizeof(size_t));
        std::string compressed_csr_offsets = compressor.compress(csr_offsets_string.c_str(), csr_offsets_string.size());
        std::string values_string((char*)values.data(), values.size() * sizeof(size_t));
        std::string compressed_values = compressor.compress(values_string.c_str(), values_string.size());

        std::cout << "Block " << i << " compressed csr offsets length: " << compressed_csr_offsets.size() << std::endl;
        std::cout << "Block " << i << " compressed values length: " << compressed_values.size() << std::endl;

        std::cout << "Block " << i << " compressed strings length: " << compressed_strings_length << std::endl;
        std::cout << "Block " << i << " compressed plist length: " << compressed_plist.size() << std::endl;
        std::cout << "Block " << i << " compressed roaring length: " << block_roaring_length << std::endl;

        total_compressed_strings_length += compressed_strings_length;
        total_compressed_plist_length += compressed_plist.size();
        total_csr_length += compressed_csr_offsets.size() + compressed_values.size();
        total_roaring_length += block_roaring_length;
    }

    std::cout << "Total compressed strings length: " << total_compressed_strings_length << std::endl;
    std::cout << "Total compressed plist length: " << total_compressed_plist_length << std::endl;
    std::cout << "Total compressed roaring length: " << total_roaring_length << std::endl;
    std::cout << "Total compressed csr length: " << total_csr_length << std::endl;

    
}

void analyze_hawaii(std::string filename) {
    
    VirtualFileRegion * vfr = new DiskVirtualFileRegion(filename);
    vfr->vfseek(-sizeof(size_t), SEEK_END);
    size_t metadata_page_length;
    vfr->vfread(&metadata_page_length, sizeof(size_t));

    // read the metadata page
    vfr->vfseek(-sizeof(size_t) - metadata_page_length, SEEK_END);
    std::string metadata_page;
    metadata_page.resize(metadata_page_length);
    vfr->vfread(&metadata_page[0], metadata_page_length);
    Compressor compressor(CompressionAlgorithm::ZSTD);
    std::string decompressed_metadata_page = compressor.decompress(metadata_page);

    // read the number of types
    size_t num_types = *reinterpret_cast<const size_t*>(decompressed_metadata_page.data());

    // read the number of groups
    size_t num_groups = *reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + sizeof(size_t));

    // read the type order
    std::vector<int> type_order;
    for (size_t i = 0; i < num_types; ++i) {
        type_order.push_back(*reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + 2 * sizeof(size_t) + i * sizeof(size_t)));
    }

    std::vector<int> chunks_in_group;
    for (size_t i = 0; i < num_types; ++i) {
        chunks_in_group.push_back(*reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + 2 * sizeof(size_t) + num_types * sizeof(size_t) + i * sizeof(size_t)));
    }

    // read the type offsets
    std::vector<size_t> type_offsets;
    for (size_t i = 0; i < num_types + 1; ++i) {
        type_offsets.push_back(*reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + 2 * sizeof(size_t) + 2 * num_types * sizeof(size_t) + i * sizeof(size_t)));
    }

    // read the group offsets
    std::vector<size_t> group_offsets;
    for (size_t i = 0; i < num_groups * 2 + 1; ++i) {
        group_offsets.push_back(*reinterpret_cast<const size_t*>(decompressed_metadata_page.data() + 2 * sizeof(size_t) + 3 * num_types * sizeof(size_t) + sizeof(size_t) + i * sizeof(size_t)));
        std::cout << "Group " << i << " offset: " << group_offsets[i] << std::endl;
    }


    std::map<int, std::set<size_t>> type_chunks = {};

    size_t total_wavelet_tree_size = 0;
    size_t total_logidx_size = 0;

    for (size_t i = 0; i < num_groups * 2; i += 2) {

        // delay this thread by a random number of seconds under 1 second

        size_t group_id = i / 2;

        size_t wavelet_offset = group_offsets[i];
        size_t logidx_offset = group_offsets[i + 1];
        size_t next_wavelet_offset = group_offsets[i + 2];
        size_t wavelet_size = logidx_offset - wavelet_offset;
        size_t logidx_size = next_wavelet_offset - logidx_offset;

        std::cout << "Group " << group_id << " wavelet size " << wavelet_size << std::endl;
        std::cout << "Group " << group_id << " logidx size " << logidx_size << std::endl;

        total_wavelet_tree_size += wavelet_size;
        total_logidx_size += logidx_size;
    }

    std::cout << "Total wavelet tree size: " << total_wavelet_tree_size << std::endl;
    std::cout << "Total logidx size: " << total_logidx_size << std::endl;
    
}

int main (int argc, char** argv) {
    if (argc != 2) {
        std::cout << "Usage: " << argv[0] << " <oahu file>" << std::endl;
        return 1;
    }

    std::string filename = argv[1];

    if (filename.find("oahu") != std::string::npos) {
        analyze_oahu(filename);
    } else if (filename.find("hawaii") != std::string::npos) {
        analyze_hawaii(filename);
    } else {
        std::cout << "Unknown file type" << std::endl;
    }

    return 0;
}