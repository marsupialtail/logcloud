#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <iostream>
#include <tuple>
#include <divsufsort.h>
#include <cassert>
#include <unordered_map>
#include "compressor.h"
#include <sstream>
#include "wavelet_tree_disk.h"
#include "wavelet_tree_s3.h"

#define ALPHABET 256

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

int main(int argc, const char *argv[1]) {

    // usage: fts index <infile> <log_idx_file> <fm_file>
    // usage: fts query-disk <infile> <log_idx_file> <fm_file> <pattern>
    // usage: fts query-s3 <bucket> <infile> <log_idx_file> <fm_file> <pattern>

    const char * mode = argv[1];

    if (strcmp(mode, "index") == 0) {

        // assert(argc == 5);
        
        char *Text;
        // open the file handle with filename in argv[1] into Text
        FILE *fp = fopen(argv[2], "r");
        fseek(fp, 0, SEEK_END);
        size_t size = ftell(fp);
        fseek(fp, 0, SEEK_SET);

        // TODO: currently this skips the last \n by force

        Text = (char *)malloc(size);
        fread(Text, 1, size, fp);
        fclose(fp);

        std::vector<size_t> C(ALPHABET, 0);
        std::vector<std::vector<size_t>> FM_index(ALPHABET, std::vector<size_t>{});

        int n = strlen(Text);
        // allocate
        int *SA = (int *)malloc(n * sizeof(int));
        // sort
        divsufsort((unsigned char *)Text, SA,  strlen(Text));

        // since we are doing this for log files, we also need to keep track of *which* log every element of the suffix array points to.
        std::vector<size_t> log_idx (n + 1, 0);

        std::cout << Text[n-1] << std::endl;
        // assert(Text[n - 1] == '\n');
        //first make an auxiliary structure that records where each newline character is
        std::vector<size_t> newlines = {};
        for (int i = 0; i < n; i++) {
            if (Text[i] == '\n') {
                newlines.push_back(i);
            }
        }

        std::cout << "detected " << newlines.size() << " logs " << std::endl;
        std::cout << newlines[newlines.size() - 1] << std::endl;
        
        std::vector<size_t> total_chars(ALPHABET, 0);

        // the suffix array does not output the last character as the first character so add it here
        FM_index[Text[n - 1]].push_back(0);
        total_chars[Text[n - 1]] ++;
        log_idx[0] = newlines[newlines.size() - 2];

        // get the second to last element of newlines, since the last element is always the length of the file assuming file ends with "\n"

        std::vector<char> last_chars = {Text[n - 1]};
        for(int i = 0; i < n; ++i) {
            // printf("%c\n", Text[SA[i] - 1]);
            last_chars.push_back(Text[SA[i] - 1]);
            char c = Text[SA[i] - 1];
            // FM_index[c].push_back(i + 1);
            total_chars[c] ++;

            int start = 0;
            int end = newlines.size() - 1;
            int mid = (start + end) / 2;
            while (start < end) {
                if (newlines[mid] < SA[i] - 1) {
                    start = mid + 1;
                } else {
                    end = mid;
                }
                mid = (start + end) / 2;
            }
            log_idx[i + 1] = newlines[start - 1];
        }

        for(int i = 0; i < ALPHABET; i++) {
            for(int j = 0; j < i; j ++)
                // C[i] += FM_index[j].size();
                C[i] += total_chars[j];
        }

        wavelet_tree_t wavelet_tree = construct_wavelet_tree(last_chars.data(), size + 1);
        
        // search_wavelet_tree(wavelet_tree, C, query, strlen(query), size);
        write_wavelet_tree_to_disk(wavelet_tree, C, n, argv[3]);
        FILE *log_idx_fp = fopen(argv[4], "wb");
        std::vector<size_t> chunk_offsets = {0};
        // iterate over chunks of log_idx_fp with size B, compress each of them, and record the offsets
        Compressor compressor(CompressionAlgorithm::ZSTD);
        for (int i = 0; i < log_idx.size(); i += B) {
            std::vector<size_t> chunk(log_idx.begin() + i, log_idx.begin() + std::min(i + B, int(log_idx.size())));
            std::string compressed_chunk = compressor.compress((char *)chunk.data(), chunk.size() * sizeof(size_t), 5);
            fwrite(compressed_chunk.data(), 1, compressed_chunk.size(), log_idx_fp);
            chunk_offsets.push_back(chunk_offsets.back() + compressed_chunk.size());
        }
        // now write the compressed_offsets
        size_t compressed_offsets_byte_offset = ftell(log_idx_fp);
        std::cout << "log_idx compressed array size: " << compressed_offsets_byte_offset << std::endl;
        std::string compressed_offsets = compressor.compress((char *)chunk_offsets.data(), chunk_offsets.size() * sizeof(size_t));
        fwrite(compressed_offsets.data(), 1, compressed_offsets.size(), log_idx_fp);
        std::cout << "log_idx compressed_offsets size: " << compressed_offsets.size() << std::endl;
        // now write 8 bytes, the byte offset
        fwrite(&compressed_offsets_byte_offset, 1, sizeof(size_t), log_idx_fp);
        fclose(log_idx_fp);
        
    } else if (strcmp(mode, "query-disk") == 0)  
    {
        FILE *fp = fopen(argv[2], "r");
        fseek(fp, 0, SEEK_END);
        size_t size = ftell(fp);
        fseek(fp, 0, SEEK_SET);
        char * Text = (char *)malloc(size);
        fread(Text, 1, size, fp);
        fclose(fp);

        const char * filename = argv[3];

        FILE *log_idx_fp = fopen(argv[4], "rb");
        fseek(log_idx_fp, 0, SEEK_END);
        size_t log_idx_size = ftell(log_idx_fp);
        fseek(log_idx_fp, -sizeof(size_t), SEEK_END);
        size_t compressed_offsets_byte_offset;
        fread(&compressed_offsets_byte_offset, sizeof(size_t), 1, log_idx_fp);
        fseek(log_idx_fp, compressed_offsets_byte_offset, SEEK_SET);
        Compressor compressor(CompressionAlgorithm::ZSTD);
        std::string compressed_offsets;
        compressed_offsets.resize(log_idx_size - compressed_offsets_byte_offset - 8);
        fread((void *)compressed_offsets.data(), 1, compressed_offsets.size(), log_idx_fp);
        std::string decompressed_offsets = compressor.decompress(compressed_offsets);
        std::vector<size_t> chunk_offsets(decompressed_offsets.size() / sizeof(size_t));
        memcpy(chunk_offsets.data(), decompressed_offsets.data(), decompressed_offsets.size());

        auto log_idx_lookup = [&chunk_offsets, log_idx_fp] (size_t idx) {
            size_t chunk_offset = chunk_offsets[idx / B];
            fseek(log_idx_fp, chunk_offset, SEEK_SET);
            std::string compressed_chunk;
            compressed_chunk.resize(chunk_offsets[idx / B + 1] - chunk_offset);
            fread((void *)compressed_chunk.data(), 1, compressed_chunk.size(), log_idx_fp);
            Compressor compressor(CompressionAlgorithm::ZSTD);
            std::string decompressed_chunk = compressor.decompress(compressed_chunk);
            std::vector<size_t> chunk(decompressed_chunk.size() / sizeof(size_t));
            memcpy(chunk.data(), decompressed_chunk.data(), decompressed_chunk.size());
            return chunk[idx % B];
        };

        const char * query = argv[5];
        auto [start, end] = search_wavelet_tree_file(filename, query, strlen(query));

        for (int i = start; i < end; i++) {
            // this will make redundant freads for consecutive log_idx, but a half decent OS would cache it in RAM.
            size_t pos = log_idx_lookup(i);
            printf("log_idx %ld ", pos);
            // now print the original text from bytes log_idx[i] to log_idx[i + 1]
            for (int j = pos + 1; j < pos + 1000; j++) {
                printf("%c", Text[j]);
                if(Text[j] == '\n') {
                    break;
                }
            }
            printf("\n");
        }
    } else if (strcmp(mode, "query-s3") == 0)  
    {
        
        const char * bucket_name = argv[2];
        const char * wavelet_idx_name = argv[3];
        const char * log_idx_name = argv[4];
        const char * query = argv[5];

        Aws::SDKOptions options;
        Aws::InitAPI(options);

        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = "us-west-2";
        clientConfig.connectTimeoutMs = 30000; // 30 seconds
        clientConfig.requestTimeoutMs = 30000; // 30 seconds
        Aws::S3::S3Client s3_client(clientConfig);
        Aws::S3::Model::GetObjectRequest object_request_log_idx;

        object_request_log_idx.WithBucket(bucket_name).WithKey(log_idx_name);
        size_t log_idx_size = get_object_size(s3_client, object_request_log_idx);
        std::vector<char> buffer = read_byte_range_from_S3(s3_client, object_request_log_idx, log_idx_size - 8, log_idx_size - 1);
        size_t compressed_offsets_byte_offset = *(size_t *)buffer.data();
        
        Compressor compressor(CompressionAlgorithm::ZSTD);
        buffer = read_byte_range_from_S3(s3_client, object_request_log_idx, compressed_offsets_byte_offset, log_idx_size - 9);
        std::string compressed_offsets(buffer.data(), buffer.size());
        std::string decompressed_offsets = compressor.decompress(compressed_offsets);
        std::vector<size_t> chunk_offsets(decompressed_offsets.size() / sizeof(size_t));
        memcpy(chunk_offsets.data(), decompressed_offsets.data(), decompressed_offsets.size());

        auto log_idx_lookup_range = [&chunk_offsets, & s3_client, &object_request_log_idx] (size_t start_idx, size_t end_idx) {
            size_t start_chunk_offset = chunk_offsets[start_idx / B];
            size_t end_chunk_offset = chunk_offsets[end_idx / B + 1];
            size_t total_chunks = end_idx / B - start_idx / B + 1;
            std::vector<char> buffer = read_byte_range_from_S3(s3_client, object_request_log_idx, start_chunk_offset, end_chunk_offset);
            std::string compressed_chunks(buffer.data(), buffer.size());
            // this contains possibly multiple chunks! We need to decode them separately
            std::vector<size_t> results = {};
            for (int i = 0; i < total_chunks; i++) {
                size_t chunk_offset = chunk_offsets[start_idx / B + i];
                std::string compressed_chunk = compressed_chunks.substr(chunk_offset - start_chunk_offset, chunk_offsets[start_idx / B + i + 1] - chunk_offset);
                compressed_chunk.resize(chunk_offsets[start_idx / B + i + 1] - chunk_offset);
                Compressor compressor(CompressionAlgorithm::ZSTD);
                std::string decompressed_chunk = compressor.decompress(compressed_chunk);
                std::vector<size_t> log_idx(decompressed_chunk.size() / sizeof(size_t));
                memcpy(log_idx.data(), decompressed_chunk.data(), decompressed_chunk.size());
                // if this is the first chunk, skip to start_idx, if last chunk, stop and end_idx, otherwise add the entire chunk to the results
                if (i == 0) {
                    results.insert(results.end(), log_idx.begin() + start_idx % B, std::min(log_idx.end(), end_idx % B + log_idx.begin()));
                } else if (i == total_chunks - 1) {
                    results.insert(results.end(), log_idx.begin(), log_idx.begin() + end_idx % B);
                } else {
                    results.insert(results.end(), log_idx.begin(), log_idx.end());
                }
            }
            return results;
        };

        Aws::S3::Model::GetObjectRequest object_request_wavelet;
        object_request_wavelet.WithBucket(bucket_name).WithKey(wavelet_idx_name);
        
        auto [start, end] = search_wavelet_tree_s3(s3_client, object_request_wavelet, query, strlen(query));
        auto results = log_idx_lookup_range(start, end);

        //print out the results
        for (int i = 0; i < results.size(); i++) {
            std::cout << results[i] << std::endl;
        }

        Aws::ShutdownAPI(options);
        

    } else {
        std::cout << "invalid mode" << std::endl;
    }

    return 0;
}
