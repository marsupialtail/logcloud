#pragma once
#include "wavelet_tree_common.h"
#include "glog/logging.h"

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

void write_wavelet_tree_to_disk(const wavelet_tree_t& tree, const std::vector<size_t>& C, size_t n, FILE * fp) {
    

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
    size_t base_offset = ftell(fp);

    for (size_t i = 0; i < tree.size(); i++) {
        const bitvector_t & bitvector = tree[i];
        if (bitvector.size() == 0) {
            level_offsets.push_back(level_offsets.back());
            continue;
        }
        size_t rank_0 = 0;
        size_t rank_1 = 0;
        std::vector<std::string> compressed_chunks;

        // compute the code for this character
        char c = i;
        LOG(INFO) << i << " ";
        // print out each bit in c
        // for (int j = 0; j < LOG_ALPHABET; j++) {
        //     LOG(INFO) << ((c >> (LOG_ALPHABET - 1 - j)) & 1);
        // }
        LOG(INFO) << std::endl;

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

            rank_0 += bitvector_rank(chunk, 0, chunk.size());
            rank_1 += bitvector_rank(chunk, 1, chunk.size());

            std::string compressed_chunk = compressor.compress((char*)packed_chunks.data(), packed_chunks.size());            
            fwrite(compressed_chunk.data(), 1, compressed_chunk.size(), fp);
            
            offsets.push_back(offsets.back() + compressed_chunk.size());
            total_length += compressed_chunk.size();
        }   
        // important: it is offsets.size() - 1, NOT offsets.size()!
        level_offsets.push_back(offsets.size() - 1);
    }
    LOG(INFO) << total_length << std::endl;
    LOG(INFO) << "number of chunks" << offsets.size() << std::endl;
    // compress the offsets too
    std::string compressed_offsets = compressor.compress((char *)offsets.data(), offsets.size() * sizeof(size_t));
    size_t compressed_offsets_byte_offset = ftell(fp) - base_offset;
    fwrite(compressed_offsets.data(), 1, compressed_offsets.size(), fp);
    LOG(INFO) << "compressed offsets size " << compressed_offsets.size() << std::endl;

    std::string compressed_level_offsets = compressor.compress((char *)level_offsets.data(), level_offsets.size() * sizeof(size_t));
    size_t compressed_level_offsets_byte_offset = ftell(fp) - base_offset;
    fwrite(compressed_level_offsets.data(), 1, compressed_level_offsets.size(), fp);

    // now write out the C vector
    std::string compressed_C = compressor.compress((char *)C.data(), C.size() * sizeof(size_t));
    size_t compressed_C_byte_offset = ftell(fp) - base_offset;
    fwrite(compressed_C.data(), 1, compressed_C.size(), fp);

    // now write out the byte_offsets as three 8-byte numbers
    fwrite(&compressed_offsets_byte_offset, 1, sizeof(size_t), fp);
    fwrite(&compressed_level_offsets_byte_offset, 1, sizeof(size_t), fp);
    fwrite(&compressed_C_byte_offset, 1, sizeof(size_t), fp);
    fwrite(&n, 1, sizeof(size_t), fp);
    
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
        LOG(INFO) << "c: " << c << std::endl;
        
        start = C[c] + wavelet_tree_rank(tree, c, start);
        end = C[c] + wavelet_tree_rank(tree, c, end);
        if (start >= end) {
            LOG(INFO) << "not found" << std::endl;
            return std::make_tuple(-1, -1);
        }
        // if (end - start < 3) {
        //     LOG(INFO) << "early exit" << std::endl;
        //     return std::make_tuple(start, end);
        // }
    }
    LOG(INFO) << "start: " << start << std::endl;
    LOG(INFO) << "end: " << end << std::endl;
    LOG(INFO) << "range: " << end - start << std::endl;
    return std::make_tuple(start, end);
}

std::tuple<size_t, std::vector<size_t>, std::vector<size_t>, std::vector<size_t>> read_metadata_from_file(VirtualFileRegion * vfr) {

    size_t file_size = vfr->size();
    vfr->vfseek(file_size - 32, SEEK_SET);

    std::vector<char> buffer(32);
    vfr->vfread(buffer.data(), 32);
    const size_t* data = reinterpret_cast<const size_t*>(buffer.data());
    size_t compressed_offsets_byte_offset = data[0];
    size_t compressed_level_offsets_byte_offset = data[1];
    size_t compressed_C_byte_offset = data[2];
    size_t n = data[3];

    LOG(INFO) << "compressed_offsets_byte_offset: " << compressed_offsets_byte_offset << std::endl;
    LOG(INFO) << "compressed_level_offsets_byte_offset: " << compressed_level_offsets_byte_offset << std::endl;
    LOG(INFO) << "compressed_C_byte_offset: " << compressed_C_byte_offset << std::endl;
    LOG(INFO) << "file size: " << file_size << std::endl;
    // now read in C array
    vfr->vfseek(compressed_offsets_byte_offset, SEEK_SET);
    buffer.clear();
    buffer.resize(file_size - 32 - compressed_offsets_byte_offset);
    vfr->vfread(buffer.data(), buffer.size());

    Compressor compressor(CompressionAlgorithm::ZSTD);
    std::string compressed_offsets(buffer.begin(), buffer.begin() + compressed_level_offsets_byte_offset - compressed_offsets_byte_offset);
    std::string decompressed_offsets = compressor.decompress(compressed_offsets);
    std::vector<size_t> offsets(decompressed_offsets.size() / sizeof(size_t));
    memcpy(offsets.data(), decompressed_offsets.data(), decompressed_offsets.size());

    std::string compressed_level_offsets(buffer.begin() + compressed_level_offsets_byte_offset - compressed_offsets_byte_offset, buffer.begin() + compressed_C_byte_offset - compressed_offsets_byte_offset);
    std::string decompressed_level_offsets = compressor.decompress(compressed_level_offsets);
    std::vector<size_t> level_offsets(decompressed_level_offsets.size() / sizeof(size_t));
    memcpy(level_offsets.data(), decompressed_level_offsets.data(), decompressed_level_offsets.size());

    std::string compressed_C(buffer.begin() + compressed_C_byte_offset - compressed_offsets_byte_offset, buffer.end());
    std::string decompressed_C = compressor.decompress(compressed_C);
    std::vector<size_t> C(decompressed_C.size() / sizeof(size_t));
    memcpy(C.data(), decompressed_C.data(), decompressed_C.size());

    vfr->reset();
    return std::make_tuple(n, C, level_offsets, offsets);
}

std::tuple<size_t, size_t, bitvector_t> read_chunk_from_file(VirtualFileRegion * vfr, size_t start_byte, size_t end_byte) {

    vfr->vfseek(start_byte, SEEK_SET);
    std::string compressed_chunk;
    compressed_chunk.resize(end_byte - start_byte);
    vfr->vfread((void *)compressed_chunk.data(), compressed_chunk.size());

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

    if (decompressed_chunk.size() < 16 + CHUNK_BITS / 8) {
        decompressed_chunk.resize(16 + CHUNK_BITS / 8, 0);
    }
    
    // now read in the bitvector
    bitvector_t chunk;
    chunk.resize(CHUNK_BITS);
    for (int i = 0; i < CHUNK_BITS; i++) {
        chunk[i] = (decompressed_chunk[i / 8 + 16] >> (7 - (i % 8))) & 1;
    }
    vfr->reset();
    return std::make_tuple(rank_0, rank_1, chunk);
}


int wavelet_tree_rank_from_file(VirtualFileRegion * vfr, const std::vector<size_t>& level_offsets, const std::vector<size_t> & offsets, char c, size_t pos) {
    // iterate through the bits of c, most significant first
    size_t curr_pos = pos;
    size_t counter = 0;
    for (int i = 0; i < LOG_ALPHABET; i++) {
        bool bit = (c >> ( (LOG_ALPHABET - 1) - i)) & 1;
        
        // look for the rank of bit in bitvector at curr_pos
        int chunk_id = level_offsets[counter] + curr_pos / CHUNK_BITS;
        int chunk_start = offsets.at(chunk_id);
        int chunk_end = offsets.at(chunk_id + 1);
        auto [rank_0, rank_1, chunk] = read_chunk_from_file(vfr, chunk_start, chunk_end);
        curr_pos = bitvector_rank(chunk, bit, curr_pos % CHUNK_BITS) + (bit ? rank_1 : rank_0);
        counter *= 2;
        counter += bit;
        counter += 1;
    }
    return curr_pos;
}

std::tuple<size_t, size_t> search_wavelet_tree_file(VirtualFileRegion * vfr, const char *P, size_t Psize) {

    auto [n, C, level_offsets, offsets] = read_metadata_from_file(vfr);
    size_t start = 0;
    size_t end = n + 1;

    size_t previous_range = -1;
    // use the FM index to search for the probe
    for(int i = Psize - 1; i >= 0; i --) {
        char c  = P[i];
        LOG(INFO) << "c: " << c << std::endl;

        start = C[c] + wavelet_tree_rank_from_file(vfr, level_offsets, offsets, c, start);
        end = C[c] + wavelet_tree_rank_from_file(vfr, level_offsets, offsets, c, end);
        LOG(INFO) << "start: " << start << std::endl;
        LOG(INFO) << "end: " << end << std::endl;
        LOG(INFO) << "range: " << end - start << std::endl;
        if (start >= end) {
            LOG(INFO) << "not found" << std::endl;
            return std::make_tuple(-1, -1);
        }
        if ( end - start == previous_range) {
            LOG(INFO) << "early exit" << std::endl;
            return std::make_tuple(start, end);
        }
        previous_range = end - start;
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

