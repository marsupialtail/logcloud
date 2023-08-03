#pragma once
#include "wavelet_tree_common.h"

void packBits(std::vector<unsigned char>& packed, const std::vector<bool>& bits) {
    unsigned char currByte = 0;
    int currBit = 0;
    for (bool bit : bits) {
        currByte |= (bit << (7 - currBit));
        currBit++;
        if (currBit == 8) {
            packed.push_back(currByte);
            currByte = 0;
            currBit = 0;
        }
    }
    if (currBit != 0) {
        packed.push_back(currByte);
    }
}

void write_wavelet_tree_to_disk(const wavelet_tree_t& tree, const std::vector<size_t>& C, size_t n, std::string filename) {
    FILE *fp = fopen(filename.c_str(), "wb");

    /*
       The wavelet tree consists of a vector of bitvectors. Each bitvector is a std::vector<bool> in memory for now.
       The bitvectors could be very long, i.e. billions of bits! Especially the one at the root. The general strategy is we 
       are going to write out the bitvectors one by one. Each bitvector is divided into a number of chunks. Each chunk
       contains B bits, and at the beginning of the chunk we are going to write out the rank(1) and rank(0) up to this chunk.
       Similar idea to Jacobsen's Rank.

       The layout of the file will be a list of compressed chunks of variable lengths. This will be followed by a metadata page
       The metadata page will contain the following information:
       - Chunk offsets: the byte offsets of each chunk
       - Alphabet offsets: the chunk offset of each character. 
       Thus to compute rank(c, i), you will first find alphabet_offset[c] to get the chunk offset of the first chunk that contains c.
       Then you will find the chunk offset of the chunk that contains i. Then you will then read and decompress that chunk
       - C vector: the C vector for the FM index
       - Last eight bytes of the file will be how long the metadata page is.
    */
    size_t total_length = 0;
   // iterate through the bitvectors
    std::vector<size_t> offsets = {0};
    std::vector<size_t> level_offsets = {0};

    Compressor compressor(CompressionAlgorithm::ZSTD);
    for (size_t i = 0; i < tree.size(); i++) {
        const bitvector_t & bitvector = tree[i];
        if (bitvector.size() == 0) {
            level_offsets.push_back(level_offsets.back());
            continue;
        }
        size_t rank_0 = 0;
        size_t rank_1 = 0;
        std::vector<std::string> compressed_chunks;
        // now iterate through the chunks
        for (size_t j = 0; j < bitvector.size(); j += CHUNK_BITS) {
            // get the chunk
            bitvector_t chunk(bitvector.begin() + j, std::min(bitvector.begin() + j + CHUNK_BITS, bitvector.end()));
            // compress the chunk
            std::vector<uint8_t> packed_chunks = {};
            // now put rank_0 and rank_1's bytes into the front of packed_chunks

            for(int i = 0; i < 8; i++) {
                packed_chunks.push_back((rank_0 >> (i * 8)) & 0xFF);
            }
            for(int i = 0; i < 8; i++) {
                packed_chunks.push_back((rank_1 >> (i * 8)) & 0xFF);
            }

            // now pack chunk's bytes on there too
            packBits(packed_chunks, chunk);
            if (ftell(fp) == 10770510) {std::cout << "here" << rank_0 << " " << rank_1 << std::endl;}
            rank_0 += bitvector_rank(chunk, 0, chunk.size());
            rank_1 += bitvector_rank(chunk, 1, chunk.size());

            std::string compressed_chunk = compressor.compress((char*)packed_chunks.data(), packed_chunks.size());
            // auto compressed_chunk = packed_chunks;
            
            fwrite(compressed_chunk.data(), 1, compressed_chunk.size(), fp);
            
            offsets.push_back(offsets.back() + compressed_chunk.size());
            total_length += compressed_chunk.size();
        }   
        // important: it is offsets.size() - 1, NOT offsets.size()!
        level_offsets.push_back(offsets.size() - 1);
    }
    std::cout << total_length << std::endl;
    std::cout << "number of chunks" << offsets.size() << std::endl;
    // compress the offsets too
    std::string compressed_offsets = compressor.compress((char *)offsets.data(), offsets.size() * sizeof(size_t));
    size_t compressed_offsets_byte_offset = ftell(fp);
    fwrite(compressed_offsets.data(), 1, compressed_offsets.size(), fp);
    std::cout << "compressed offsets size " << compressed_offsets.size() << std::endl;

    std::string compressed_level_offsets = compressor.compress((char *)level_offsets.data(), level_offsets.size() * sizeof(size_t));
    size_t compressed_level_offsets_byte_offset = ftell(fp);
    fwrite(compressed_level_offsets.data(), 1, compressed_level_offsets.size(), fp);

    // now write out the C vector
    std::string compressed_C = compressor.compress((char *)C.data(), C.size() * sizeof(size_t));
    size_t compressed_C_byte_offset = ftell(fp);
    fwrite(compressed_C.data(), 1, compressed_C.size(), fp);

    // now write out the byte_offsets as three 8-byte numbers
    fwrite(&compressed_offsets_byte_offset, 1, sizeof(size_t), fp);
    fwrite(&compressed_level_offsets_byte_offset, 1, sizeof(size_t), fp);
    fwrite(&compressed_C_byte_offset, 1, sizeof(size_t), fp);
    fwrite(&n, 1, sizeof(size_t), fp);
    fclose(fp);
}

int wavelet_tree_rank(const wavelet_tree_t & tree, char c, size_t pos) {
    // iterate through the bits of c, most significant first
    size_t counter = 0;
    size_t curr_pos = pos;
    for (int i = 0; i < LOG_ALPHABET; i++) {
        bool bit = (c >> ( (LOG_ALPHABET - 1) - i)) & 1;
        const bitvector_t & bitvector = tree[counter];
        // look for the rank of bit in bitvector at curr_pos
        curr_pos = bitvector_rank(bitvector, bit, curr_pos);
        counter *= 2;
        counter += bit;
        counter += 1;
    }
    return curr_pos;
}

std::tuple<size_t, size_t> search_wavelet_tree(const wavelet_tree_t & tree, std::vector<size_t>& C, const char *P, size_t Psize, size_t n) {
    size_t start = 0;
    size_t end = n + 1;
    // use the FM index to search for the probe
    for(int i = Psize - 1; i >= 0; i --) {
        char c  = P[i];
        std::cout << "c: " << c << std::endl;
        
        start = C[c] + wavelet_tree_rank(tree, c, start);
        end = C[c] + wavelet_tree_rank(tree, c, end);
        if (start >= end) {
            std::cout << "not found" << std::endl;
            return std::make_tuple(-1, -1);
        }
    }
    std::cout << "start: " << start << std::endl;
    std::cout << "end: " << end << std::endl;
    return std::make_tuple(start, end);
}

std::tuple<size_t, std::vector<size_t>, std::vector<size_t>, std::vector<size_t>> read_metadata_from_file(const std::string filename) {
    FILE *fp = fopen(filename.c_str(), "rb");
    fseek(fp, 0, SEEK_END);
    size_t file_size = ftell(fp);
    fseek(fp, -32, SEEK_END);
    // get size of file
    size_t compressed_offsets_byte_offset;
    size_t compressed_level_offsets_byte_offset;
    size_t compressed_C_byte_offset;
    size_t n;
    fread(&compressed_offsets_byte_offset, 1, sizeof(size_t), fp);
    fread(&compressed_level_offsets_byte_offset, 1, sizeof(size_t), fp);
    fread(&compressed_C_byte_offset, 1, sizeof(size_t), fp);
    fread(&n, 1, sizeof(size_t), fp);

    std::cout << "compressed_offsets_byte_offset: " << compressed_offsets_byte_offset << std::endl;
    std::cout << "compressed_level_offsets_byte_offset: " << compressed_level_offsets_byte_offset << std::endl;
    std::cout << "compressed_C_byte_offset: " << compressed_C_byte_offset << std::endl;
    std::cout << "file size: " << file_size << std::endl;
    // now read in C array
    fseek(fp, compressed_C_byte_offset, SEEK_SET);
    Compressor compressor(CompressionAlgorithm::ZSTD);
    std::string compressed_C;
    compressed_C.resize(file_size - 32 - compressed_C_byte_offset);
    fread((void *)compressed_C.data(), 1, compressed_C.size(), fp);
    std::string decompressed_C = compressor.decompress(compressed_C);
    std::vector<size_t> C(decompressed_C.size() / sizeof(size_t));
    memcpy(C.data(), decompressed_C.data(), decompressed_C.size());
    // now read in alphabet offsets
    fseek(fp, compressed_level_offsets_byte_offset, SEEK_SET);
    std::string compressed_level_offsets;
    compressed_level_offsets.resize( compressed_C_byte_offset - compressed_level_offsets_byte_offset);
    fread((void *)compressed_level_offsets.data(), 1, compressed_level_offsets.size(), fp);
    std::string decompressed_level_offsets = compressor.decompress(compressed_level_offsets);
    std::vector<size_t> level_offsets(decompressed_level_offsets.size() / sizeof(size_t));
    memcpy(level_offsets.data(), decompressed_level_offsets.data(), decompressed_level_offsets.size());
    // now read in offsets
    fseek(fp, compressed_offsets_byte_offset, SEEK_SET);
    std::string compressed_offsets;
    compressed_offsets.resize(compressed_level_offsets_byte_offset - compressed_offsets_byte_offset);
    fread((void *)compressed_offsets.data(), 1, compressed_offsets.size(), fp);
    std::string decompressed_offsets = compressor.decompress(compressed_offsets);
    std::vector<size_t> offsets(decompressed_offsets.size() / sizeof(size_t));
    memcpy(offsets.data(), decompressed_offsets.data(), decompressed_offsets.size());

    fclose(fp);
    return std::make_tuple(n, C, level_offsets, offsets);
}

std::tuple<size_t, size_t, bitvector_t> read_chunk_from_file(const std::string filename, size_t start_byte, size_t end_byte) {
    FILE *fp = fopen(filename.c_str(), "rb");
    fseek(fp, start_byte, SEEK_SET);
    std::string compressed_chunk;
    compressed_chunk.resize(end_byte - start_byte);
    fread((void *)compressed_chunk.data(), 1, compressed_chunk.size(), fp);
    Compressor compressor(CompressionAlgorithm::ZSTD);
    std::string decompressed_chunk = compressor.decompress(compressed_chunk);
    // auto decompressed_chunk = compressed_chunk;
    // now read in the rank_0 and rank_1
    size_t rank_0 = 0;
    size_t rank_1 = 0;
   
    for(int i = 0; i < 8; i++) {
        rank_0 |= ((size_t)(unsigned char)decompressed_chunk[i] << (i * 8));
    }
    for(int i = 0; i < 8; i++) {
        rank_1 |= ((size_t)(unsigned char)decompressed_chunk[i + 8] << (i * 8));
    }
    // print out the first 128 bits of decompressed_chunk in hex
    
    // now read in the bitvector
    bitvector_t chunk;
    chunk.resize(CHUNK_BITS);
    for (int i = 0; i < CHUNK_BITS; i++) {
        chunk[i] = (decompressed_chunk[i / 8 + 16] >> (7 - (i % 8))) & 1;
    }
    fclose(fp);
    return std::make_tuple(rank_0, rank_1, chunk);
}


int wavelet_tree_rank_from_file(const std::string filename, const std::vector<size_t>& level_offsets, const std::vector<size_t> & offsets, char c, size_t pos) {
    // iterate through the bits of c, most significant first
    size_t curr_pos = pos;
    size_t counter = 0;
    for (int i = 0; i < LOG_ALPHABET; i++) {
        bool bit = (c >> ( (LOG_ALPHABET - 1) - i)) & 1;
        
        // look for the rank of bit in bitvector at curr_pos
        int chunk_id = level_offsets[counter] + curr_pos / CHUNK_BITS;
        int chunk_start = offsets.at(chunk_id);
        int chunk_end = offsets.at(chunk_id + 1);
        auto [rank_0, rank_1, chunk] = read_chunk_from_file(filename, chunk_start, chunk_end);
        curr_pos = bitvector_rank(chunk, bit, curr_pos % CHUNK_BITS) + (bit ? rank_1 : rank_0);
        counter *= 2;
        counter += bit;
        counter += 1;
    }
    return curr_pos;
}

std::tuple<size_t, size_t> search_wavelet_tree_file(const std::string filename, const char *P, size_t Psize) {

    auto [n, C, level_offsets, offsets] = read_metadata_from_file(filename);
    size_t start = 0;
    size_t end = n + 1;

    // use the FM index to search for the probe
    for(int i = Psize - 1; i >= 0; i --) {
        char c  = P[i];
        std::cout << "c: " << c << std::endl;

        start = C[c] + wavelet_tree_rank_from_file(filename, level_offsets, offsets, c, start);
        end = C[c] + wavelet_tree_rank_from_file(filename, level_offsets, offsets, c, end);
        std::cout << "start: " << start << std::endl;
        std::cout << "end: " << end << std::endl;
        if (start >= end) {
            std::cout << "not found" << std::endl;
            return std::make_tuple(-1, -1);
        }
    }

    return std::make_tuple(start, end);
}

wavelet_tree_t construct_wavelet_tree(const char *P, size_t Psize) {

    wavelet_tree_t to_hit(ALPHABET);
    for(int idx = 0; idx < Psize; idx++) {
        char c = P[idx];
        // initialize each element in to_hit to be empty vector
        size_t counter = 0;
        // push back the most significant bit of c
        to_hit[counter].push_back((c >> (LOG_ALPHABET - 1)) & 1);
        // iterate over the bits of c, most significant first, don't have to do the last bit
        for (int i = 0; i <  (LOG_ALPHABET - 1); i++) {
            bool bit = (c >> ( (LOG_ALPHABET - 1) - i)) & 1;
            counter *= 2;
            counter += bit;
            counter += 1;
            // push_back the next bit 
            to_hit[counter].push_back((c >> ( (LOG_ALPHABET - 2) - i)) & 1);
        }
    }
    return to_hit;

}

