#include "fts.h"

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
        std::cout << "size:" << size << std::endl;
        fseek(fp, 0, SEEK_SET);

        Text = (char *)malloc(size);
        fread(Text, 1, size, fp);
        fclose(fp);
        
        // TODO: currently this skips the last \n by force

        // get the log_idx and wavelet_tree
        auto [wavelet_tree, log_idx, C] = bwt_and_build_wavelet(Text);
        
        FILE *wavelet_fp = fopen(argv[3], "wb");
        write_wavelet_tree_to_disk(wavelet_tree, C, size, wavelet_fp);
        fclose(wavelet_fp);

        FILE *log_idx_fp = fopen(argv[4], "wb");
        write_log_idx_to_disk(log_idx, log_idx_fp);
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

        const char * wavelet_filename = argv[3];
        const char * log_idx_filename = argv[4];
        const char * query = argv[5];

        VirtualFileRegion * wavelet_vfr = new DiskVirtualFileRegion(wavelet_filename);
        VirtualFileRegion * log_idx_vfr = new DiskVirtualFileRegion(log_idx_filename);
        auto matched_pos = search_vfr(wavelet_vfr, log_idx_vfr, query);

        // print out the matchd pos 

        for (size_t pos : matched_pos) {
            std::cout << pos << std::endl;
        }

        for (size_t pos : matched_pos) {
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
