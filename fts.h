#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <iostream>
#include <tuple>
#include <cassert>
#include <unordered_map>
#include "compressor.h"
#include <sstream>
#include <divsufsort.h>
#include "wavelet_tree_disk.h"
#include "wavelet_tree_s3.h"

#define B (1024 * 1024)

void check_wavelet_tree(const std::vector<std::vector<size_t>> & FM_index, const wavelet_tree_t & wavelet_tree)
{
    for(int i = 0; i < ALPHABET; i++) {
        if (size_t(FM_index[i].size()) > 0) {std::cout << (char) i << std::endl;}
        // iterate through the FM indices
        for (size_t j = 0; j < FM_index[i].size(); j++) {
            // get the suffix array index
            size_t idx = FM_index[i][j];
            std::cout << j << " " << idx << " " << wavelet_tree_rank(wavelet_tree, i, idx) << " " << wavelet_tree_rank(wavelet_tree, i, idx + 1) << std::endl; 
        }
    }
}

#define GIVEUP 100

std::vector<size_t> search_disk(VirtualFileRegion * wavelet_vfr, VirtualFileRegion * log_idx_vfr, std::string query) {
    size_t log_idx_size = log_idx_vfr->size();
    size_t compressed_offsets_byte_offset;
    
    log_idx_vfr->vfseek(-sizeof(size_t), SEEK_END);
    auto start_time = std::chrono::high_resolution_clock::now();
    log_idx_vfr->vfread(&compressed_offsets_byte_offset, sizeof(size_t));
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop-start_time);
    std::cout << "log_idx decompress offsets took " << duration.count() << " milliseconds, this could choke for concurrent requests!" << std::endl;

    log_idx_vfr->vfseek(compressed_offsets_byte_offset, SEEK_SET);
    Compressor compressor(CompressionAlgorithm::ZSTD);
    std::string compressed_offsets;
    compressed_offsets.resize(log_idx_size - compressed_offsets_byte_offset - 8);
    log_idx_vfr->vfread((void *)compressed_offsets.data(), compressed_offsets.size());
    std::string decompressed_offsets = compressor.decompress(compressed_offsets);
    std::vector<size_t> chunk_offsets(decompressed_offsets.size() / sizeof(size_t));
    memcpy(chunk_offsets.data(), decompressed_offsets.data(), decompressed_offsets.size());
    

    auto log_idx_lookup = [&chunk_offsets, log_idx_vfr] (size_t idx) {
        size_t chunk_offset = chunk_offsets[idx / B];
        log_idx_vfr->vfseek(chunk_offset, SEEK_SET);
        std::string compressed_chunk;
        compressed_chunk.resize(chunk_offsets[idx / B + 1] - chunk_offset);
        log_idx_vfr->vfread((void *)compressed_chunk.data(), compressed_chunk.size());
        Compressor compressor(CompressionAlgorithm::ZSTD);
        std::string decompressed_chunk = compressor.decompress(compressed_chunk);
        std::vector<size_t> chunk(decompressed_chunk.size() / sizeof(size_t));
        memcpy(chunk.data(), decompressed_chunk.data(), decompressed_chunk.size());
        return chunk[idx % B];
    };

    std::cout << "num reads: " << wavelet_vfr->num_reads << std::endl;
    std::cout << "num bytes read: " << wavelet_vfr->num_bytes_read << std::endl;
    
    auto [start, end] = search_wavelet_tree_file(wavelet_vfr, query.c_str(), query.size());

    std::cout << "num reads: " << wavelet_vfr->num_reads << std::endl;
    std::cout << "num bytes read: " << wavelet_vfr->num_bytes_read << std::endl;

    std::vector<size_t> matched_pos = {};

    if (end - start > GIVEUP) {
        std::cout << "too many matches, giving up" << std::endl;
        return {(size_t) -1};
    } else {
        for (int i = start; i < end; i++) {
            // this will make redundant freads for consecutive log_idx, but a half decent OS would cache it in RAM.
            size_t pos = log_idx_lookup(i);
            printf("log_idx %ld ", pos);
            // now print the original text from bytes log_idx[i] to log_idx[i + 1]
            matched_pos.push_back(pos);
        }
        std::cout << "num reads: " << wavelet_vfr->num_reads << std::endl;
        std::cout << "num bytes read: " << wavelet_vfr->num_bytes_read << std::endl;
        wavelet_vfr->reset();
        log_idx_vfr->reset();
    }

    return matched_pos;
}

std::tuple<wavelet_tree_t, std::vector<size_t>, std::vector<size_t>> bwt_and_build_wavelet( char* Text, size_t block_lines = -1) {

    std::vector<size_t> C(ALPHABET, 0);
    // std::vector<std::vector<size_t>> FM_index(ALPHABET, std::vector<size_t>{});

    int n = strlen(Text);
    std::cout << "n:" << n << std::endl;
    // allocate
    int *SA = (int *)malloc(n * sizeof(int));
    // sort
    divsufsort((unsigned char *)Text, SA,  strlen(Text));

    // since we are doing this for log files, we also need to keep track of *which* log every element of the suffix array points to.
    std::vector<size_t> log_idx (n + 1, 0);

    std::cout << Text[n-1] << std::endl;
    // assert(Text[n - 1] == '\n');
    //first make an auxiliary structure that records where each newline character is
    std::vector<size_t> newlines = {0};
    for (int i = 0; i < n; i++) {
        if (Text[i] == '\n') {
            newlines.push_back(i);
        }
    }

    std::cout << "detected " << newlines.size() << " logs " << std::endl;
    std::cout << "block lines " << block_lines << std::endl;
    std::cout << newlines[newlines.size() - 1] << std::endl;
    
    std::vector<size_t> total_chars(ALPHABET, 0);

    // the suffix array does not output the last character as the first character so add it here
    // FM_index[Text[n - 1]].push_back(0);
    total_chars[Text[n - 1]] ++;

    // get the second to last element of newlines, since the last element is always the length of the file assuming file ends with "\n"
    log_idx[0] = newlines[newlines.size() - 2];

    FILE *debug_fp = fopen("logidx.log", "w");
    std::vector<char> last_chars = {Text[n - 1]};

    size_t average_bytes_per_line = n / newlines.size();

    auto start_time = std::chrono::high_resolution_clock::now();
    for(int i = 0; i < n; ++i) {
        // printf("%c\n", Text[SA[i] - 1]);
        last_chars.push_back(Text[SA[i] - 1]);
        char c = Text[SA[i] - 1];
        // FM_index[c].push_back(i + 1);
        total_chars[c] ++;

        // int start = 0;
        // int end = newlines.size() - 1;
        // int mid = (start + end) / 2;
        // while (start < end) {
        //     if (newlines[mid] < SA[i] - 1) {
        //         start = mid + 1;
        //     } else {
        //         end = mid;
        //     }
        //     mid = (start + end) / 2;
        // }

        // int my_guess = (SA[i] - 1) / average_bytes_per_line;
        // // now only binary search a range around the guess
        // int start = my_guess > 10000 ? my_guess - 10000 : 0;
        // int end = std::min((int)newlines.size() - 1, my_guess + 10000);
        // auto it = std::lower_bound(newlines.begin() + start, newlines.begin() + end, SA[i] - 1);

        // // if not found
        // if (it - newlines.begin() == end) {
        //     it = std::lower_bound(newlines.begin(), newlines.end(), SA[i] - 1);
        // }

        auto it = std::lower_bound(newlines.begin(), newlines.end(), SA[i] - 1);

        auto mid = it - newlines.begin() + 1;

        if (block_lines == -1) {
            log_idx[i + 1] = newlines[mid - 1];
        } else {
            log_idx[i + 1] = (mid - 1) / block_lines;
        }

        // TODO: will this work for the first log? Somehow it works, but I don't know why
        
        // log_idx[i + 1] = (start - 1) / ROW_GROUP_SIZE;
        // std::cout << log_idx[i + 1] << std::endl;
        fprintf(debug_fp, "%ld\n", log_idx[i+1]); 
    }
    fclose(debug_fp);

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop-start_time);
    std::cout << "log_idx binary search took " << duration.count() << " milliseconds" << std::endl;

    for(int i = 0; i < ALPHABET; i++) {
        for(int j = 0; j < i; j ++)
            // C[i] += FM_index[j].size();
            C[i] += total_chars[j];
    }

    
    wavelet_tree_t wavelet_tree = construct_wavelet_tree(last_chars.data(), n + 1);

    return std::make_tuple(wavelet_tree, log_idx, C);
    
}

void write_log_idx_to_disk(std::vector<size_t> log_idx, FILE * log_idx_fp) {
    std::vector<size_t> chunk_offsets = {0};
    // iterate over chunks of log_idx_fp with size B, compress each of them, and record the offsets
    Compressor compressor(CompressionAlgorithm::ZSTD);

    size_t base_offset = ftell(log_idx_fp);

    for (int i = 0; i < log_idx.size(); i += B) {
        std::vector<size_t> chunk(log_idx.begin() + i, log_idx.begin() + std::min(i + B, int(log_idx.size())));
        std::string compressed_chunk = compressor.compress((char *)chunk.data(), chunk.size() * sizeof(size_t), 5);
        fwrite(compressed_chunk.data(), 1, compressed_chunk.size(), log_idx_fp);
        chunk_offsets.push_back(chunk_offsets.back() + compressed_chunk.size());
    }
    // now write the compressed_offsets
    size_t compressed_offsets_byte_offset = ftell(log_idx_fp) - base_offset;
    std::cout << "log_idx compressed array size: " << compressed_offsets_byte_offset << std::endl;
    std::string compressed_offsets = compressor.compress((char *)chunk_offsets.data(), chunk_offsets.size() * sizeof(size_t));
    fwrite(compressed_offsets.data(), 1, compressed_offsets.size(), log_idx_fp);
    std::cout << "log_idx compressed_offsets size: " << compressed_offsets.size() << std::endl;
    // now write 8 bytes, the byte offset
    fwrite(&compressed_offsets_byte_offset, 1, sizeof(size_t), log_idx_fp);
}
