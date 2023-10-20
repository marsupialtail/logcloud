#pragma once
#include "wavelet_tree_common.h"
#include "s3.h"

std::tuple<size_t, std::vector<size_t>, std::vector<size_t>, std::vector<size_t>> read_metadata_from_s3(const Aws::S3::S3Client& s3_client, Aws::S3::Model::GetObjectRequest& object_request) {
    
    size_t file_size = get_object_size(s3_client, object_request);
    size_t compressed_offsets_byte_offset;
    size_t compressed_level_offsets_byte_offset;
    size_t compressed_C_byte_offset;
    size_t n;
    std::vector<char> buffer = read_byte_range_from_S3(s3_client, object_request, file_size - 32, file_size - 1);
    
    const size_t* data = reinterpret_cast<const size_t*>(buffer.data());
    compressed_offsets_byte_offset = data[0];
    compressed_level_offsets_byte_offset = data[1];
    compressed_C_byte_offset = data[2];
    n = data[3];
    std::cout << "compressed_offsets_byte_offset: " << compressed_offsets_byte_offset << std::endl;
    std::cout << "compressed_level_offsets_byte_offset: " << compressed_level_offsets_byte_offset << std::endl;
    std::cout << "compressed_C_byte_offset: " << compressed_C_byte_offset << std::endl;
    std::cout << "file size: " << file_size << std::endl;

    Compressor compressor(CompressionAlgorithm::ZSTD);
    buffer = read_byte_range_from_S3(s3_client, object_request, compressed_offsets_byte_offset, file_size - 33);
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

    return std::make_tuple(n, C, level_offsets, offsets);

}

std::tuple<size_t, size_t, bitvector_t> read_chunk_from_s3(const Aws::S3::S3Client& s3_client, Aws::S3::Model::GetObjectRequest& object_request, size_t start_byte, size_t end_byte) {
    
    std::vector<char> buffer  = read_byte_range_from_S3(s3_client, object_request, start_byte, end_byte - 1);
    std::string compressed_chunk(buffer.begin(), buffer.end());
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
    return std::make_tuple(rank_0, rank_1, chunk);
}

int wavelet_tree_rank_from_s3(const Aws::S3::S3Client& s3_client, Aws::S3::Model::GetObjectRequest& object_request, const std::vector<size_t>& level_offsets, const std::vector<size_t> & offsets, char c, size_t pos) {
    // iterate through the bits of c, most significant first
    size_t curr_pos = pos;
    size_t counter = 0;
    for (int i = 0; i < LOG_ALPHABET; i++) {
        bool bit = (c >> ( (LOG_ALPHABET - 1) - i)) & 1;
        
        // look for the rank of bit in bitvector at curr_pos
        int chunk_id = level_offsets[counter] + curr_pos / CHUNK_BITS;
        int chunk_start = offsets.at(chunk_id);
        int chunk_end = offsets.at(chunk_id + 1);
        auto [rank_0, rank_1, chunk] = read_chunk_from_s3(s3_client, object_request, chunk_start, chunk_end);
        curr_pos = bitvector_rank(chunk, bit, curr_pos % CHUNK_BITS) + (bit ? rank_1 : rank_0);
        counter *= 2;
        counter += bit;
        counter += 1;
    }
    return curr_pos;
}


std::tuple<size_t, size_t> search_wavelet_tree_s3(const Aws::S3::S3Client& s3_client, Aws::S3::Model::GetObjectRequest& object_request, const char *P, size_t Psize) {
    
    auto [n, C, level_offsets, offsets] = read_metadata_from_s3(s3_client, object_request);
    size_t start = 0;
    size_t end = n + 1;

    // use the FM index to search for the probe
    for(int i = Psize - 1; i >= 0; i --) {
        char c  = P[i];
        std::cout << "c: " << c << std::endl;

        start = C[c] + wavelet_tree_rank_from_s3(s3_client, object_request, level_offsets, offsets, c, start);
        end = C[c] + wavelet_tree_rank_from_s3(s3_client, object_request, level_offsets, offsets, c, end);
        std::cout << "start: " << start << std::endl;
        std::cout << "end: " << end << std::endl;
        if (start >= end) {
            std::cout << "not found" << std::endl;
            return std::make_tuple(-1, -1);
        }
    }
    std::cout << "start: " << start << std::endl;
    std::cout << "end: " << end << std::endl;
    return std::make_tuple(start, end);
}