#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <iostream>
#include <tuple>
#include <divsufsort.h>
#include <cassert>
#include <unordered_map>
#include "s3.h"
#include "compressor.h"
#include <sstream>


#define ALPHABET 256
typedef uint64_t bwt_int;
// each "page" in this btree is just a size B * 2 array, where index 2x is the element and 2x + 1 is -1 if leaf and the leave page id if not
typedef std::vector<std::tuple<bwt_int, bwt_int>> btree_page_t;
typedef std::vector<btree_page_t> btree_t;

#define B 1024

std::string compress_page(const btree_page_t& page, CompressionAlgorithm algorithm = CompressionAlgorithm::ZSTD) {
    Compressor compressorZstd(CompressionAlgorithm::ZSTD);
    // convert btree_page_t to std::string
    std::vector<bwt_int> serialized_page = {};
    for (int i = 0; i < page.size(); i++) {
        serialized_page.push_back(std::get<0>(page[i]));
        serialized_page.push_back(std::get<1>(page[i]));
    }
    if (page.size() < B) {
        bwt_int fill = -1;
        for (int i = 0; i < B - page.size(); i++) {
            serialized_page.push_back(fill);
            serialized_page.push_back(fill);
        }
    }
    // now convert serialized_page.data() to const char *
    std::string compressedZstd = compressorZstd.compress(reinterpret_cast<const char*>(serialized_page.data()), serialized_page.size() * sizeof(bwt_int));
    return compressedZstd;
}

btree_page_t decompress_page(const std::string& compressed_page, CompressionAlgorithm algorithm = CompressionAlgorithm::ZSTD) {
    Compressor compressorZstd(CompressionAlgorithm::ZSTD);
    std::string decompressedZstd = compressorZstd.decompress(compressed_page);
    const bwt_int* data_ptr = reinterpret_cast<const bwt_int*>(decompressedZstd.data());
    const std::vector<bwt_int> data = std::vector<bwt_int>(data_ptr, data_ptr + B * 2);
    btree_page_t page = {};
    for (int i = 0; i < B; i++) {
        // don't read in the fillers
        if (data[i * 2] == -1 && data[i * 2 + 1] == -1) {
            break;
        }
        page.push_back(std::make_tuple(data[i * 2], data[i * 2 + 1]));
    }
    return page;
}

/* 
The on disk data format of our collection of btrees.

size of metadata page, max 8192 bytes
the first section is ALPHABET * (4 byte, 4 byte) + 4 bytes + 4 bytes. The first 4 bytes tell you which byte the btree pages for that character starts,
the second 4 bytes tells you the value of C for that character
the final 8 bytes, first 4 bytes denote the total length of the file and the second 4 bytes denote the length of the original corpus
the second section has variable size. you need to read it in its entirety to make sense as it tells you the encoding of our non ASCII tokens
this section could be empty. the first four bytes in that section (byte alphabet * 8 - alphabet *8 + 4) will tell you its size

starting from byte 8192, all of the pages of all the btrees are laid out in sequential order. each page is B * 8 bytes long
*/

void write_btrees_to_disk(std::vector<btree_t>& btrees, std::vector<bwt_int>& C, size_t n, std::string filename) {
    FILE *fp = fopen(filename.c_str(), "wb");

    // write the metadata page
    bwt_int offset = 8192;
    for (int i = 0; i < ALPHABET; i++) {
        printf("%c (%ld):", i, C[i]);
        std::cout << offset << std::endl;
        fwrite(&offset, sizeof(bwt_int), 1, fp);
        fwrite(&C[i], sizeof(bwt_int), 1, fp);
        offset += btrees[i].size() * B * sizeof(bwt_int) * 2;
    }
    fwrite(&offset, sizeof(bwt_int), 1, fp);
    fwrite(&n, sizeof(bwt_int), 1, fp);

    // write the encoding section, skip for now, fill file up to 8192 bytes with 0
    size_t zero = 0;
    fwrite(&zero, sizeof(char), 8192 - ALPHABET * sizeof(bwt_int) * 2 - sizeof(bwt_int) * 2, fp);

    // write the btree pages
    for (int i = 0; i < ALPHABET; i++) {
        if (btrees[i].size() == 0) {
            continue;
        }
        for (int j = 0; j < btrees[i].size(); j++) {
            btree_page_t page = btrees[i][j];
            for (int k = 0; k < page.size(); k++) {
                fwrite(&std::get<0>(page[k]), sizeof(bwt_int), 1, fp);
                fwrite(&std::get<1>(page[k]), sizeof(bwt_int), 1, fp);
            }
            if (page.size() < B) {
                bwt_int fill = -1;
                for (int k = 0; k < B - page.size(); k++) {
                    fwrite(&fill, sizeof(bwt_int), 1, fp);
                    fwrite(&fill, sizeof(bwt_int), 1, fp);
                }
            }
        }
    }

    fclose(fp);
}

btree_t construct_btree( bwt_int * elements, size_t n )
{
    btree_t btree;
    std::vector<std::tuple<bwt_int, bwt_int>> leaf_mins = {};
    std::stringstream compressed_tree;
    std::vector<bwt_int> compressed_page_offsets = {};
    //leaves
    size_t page_count = 0;
    
    compressed_page_offsets.push_back(0);
    for (size_t i = 0; i < n; i += B) {
        btree_page_t page;
        for (size_t j = i; j < std::min(i + B, n); j++) {
            page.push_back(std::make_tuple(elements[j], -1));
        }
        btree.push_back(page);
        compressed_tree << compress_page(page);
        compressed_page_offsets.push_back(compressed_tree.tellp());
        leaf_mins.push_back(std::make_tuple(elements[i], page_count));
        page_count++;
    }

    //now make the intermediate nodes
    while (leaf_mins.size() > B) {
        std::vector<std::tuple<bwt_int, bwt_int>> new_leaf_mins = {};
        for (size_t i = 0; i < leaf_mins.size(); i += B) {
            btree_page_t page;
            for (size_t j = i; j < std::min(i + B, leaf_mins.size()); j++) {
                page.push_back(leaf_mins[j]);
            }
            btree.push_back(page);
            new_leaf_mins.push_back(std::make_tuple(std::get<0>(leaf_mins[i]), page_count));
            page_count++;
        }
        leaf_mins = new_leaf_mins;
    }

    // make the root page
    btree_page_t root_page;
    for (int i = 0; i < leaf_mins.size(); i++) {
        root_page.push_back(leaf_mins[i]);
    }
    btree.push_back(root_page);    

    return btree;
}

bwt_int lookup(btree_t& btree, size_t page_id, bwt_int i) {
    // first go to the root page at the end of the btree

    btree_page_t page = btree[page_id];

    int result = page.size() - 1;
    for (int l = 0; l < page.size(); l++) {
        if (std::get<0>(page[l]) >= i) {
            result = l - 1;
            break;
        }
    }

    // result now holds the index of the element just smaller than i, it might be -1 if the first element is smaller than i
    // since we always send the probe to the page which is smaller than it, and the first element of the root is the smallest in the dataset
    // this must mean that i is not in the dataset
    if(result == -1) {
        assert(page_id == btree.size() - 1);
        return 0;
    }

    // now l is the index of the element just smaller than i
    // std::cout << "result: " << result << std::endl;
    std::tuple<bwt_int, bwt_int> elem = page[result];
    if(std::get<1>(elem) == -1) {
        // this is a leaf node, so we're done
        return page_id * B + result + 1;
    } else {
        // this is an intermediate node, so we need to go to the next page
        return lookup(btree, std::get<1>(elem), i);
    }

}

void search(std::vector<btree_t>& btrees, std::vector<bwt_int>& C, const char *P, size_t Psize, size_t n) {
    bwt_int start = 0;
    bwt_int end = n + 1;
    // use the FM index to search for the probe
    for(int i = Psize - 1; i >= 0; i --) {
        char c  = P[i];
        std::cout << "c: " << c << std::endl;
        if (btrees[c].size() == 0) {
            std::cout << "not found" << std::endl;
            return;
        }
        start = C[c] + lookup(btrees[c], btrees[c].size() - 1, start);
        end = C[c] + lookup(btrees[c], btrees[c].size() - 1, end);
        if (start >= end) {
            std::cout << "not found" << std::endl;
            return;
        }
    }
    std::cout << "start: " << start << std::endl;
    std::cout << "end: " << end << std::endl;
}

std::vector<bwt_int> load_data_from_file(FILE * fp, size_t btree_offset, size_t page_id, int & block_access) {
    fseek(fp, btree_offset + page_id * sizeof(bwt_int) * 2 * B, SEEK_SET);
    bwt_int data[B * 2];
    block_access += 1;
    fread(data, sizeof(bwt_int), B * 2, fp);
    std::vector<bwt_int> ret(data, data + B * 2);
    return ret;
}


btree_page_t build_page(const std::vector<bwt_int>& data) {
    btree_page_t page = {};
    for (int j = 0; j < B; j++) {
        // don't read in the fillers
        if (data[j * 2] == -1 && data[j * 2 + 1] == -1) {
            break;
        }
        page.push_back(std::make_tuple(data[j * 2], data[j * 2 + 1]));
    }
    return page;
}

std::tuple<bool, bwt_int> lookup_page (btree_page_t & page, bwt_int i, size_t page_id) {
    int result = page.size() - 1;
    for (int l = 0; l < page.size(); l++) {
        if (std::get<0>(page[l]) >= i) {
            result = l - 1;
            break;
        }
    }

    // result now holds the index of the element just smaller than i, it might be -1 if the first element is smaller than i
    // since we always send the probe to the page which is smaller than it, and the first element of the root is the smallest in the dataset
    // this must mean that i is not in the dataset
    if(result == -1) {
        return std::make_tuple(true, 0);
    }
    std::tuple<bwt_int, bwt_int> elem = page[result];
    if(std::get<1>(elem) == -1) {
        return std::make_tuple(true, page_id * B + result + 1);
    } else {
        return std::make_tuple(false, std::get<1>(elem));
    }
}


bwt_int lookup_from_file(FILE * fp, size_t btree_offset, size_t page_id, bwt_int i, int & block_access) {
    auto data = load_data_from_file(fp, btree_offset, page_id, block_access);
    auto page = build_page(data);
    std::tuple<bool, bwt_int> result = lookup_page(page, i, page_id);
    if (std::get<0>(result) == true) {
        return std::get<1>(result);
    } else {
        return lookup_from_file(fp, btree_offset, std::get<1>(result), i, block_access);
    }
}

bwt_int lookup_from_s3(const Aws::S3::S3Client & s3_client, Aws::S3::Model::GetObjectRequest & object_request, size_t btree_offset, size_t page_id, bwt_int i, int & block_access) {
    
    size_t start_byte = btree_offset + page_id * sizeof(bwt_int) * 2 * B;
    size_t end_byte = start_byte + sizeof(bwt_int) * 2 * B - 1;
    std::vector<char> buffer = read_byte_range_from_S3(s3_client, object_request, start_byte, end_byte);
    const bwt_int* data_ptr = reinterpret_cast<const bwt_int*>(buffer.data());
    const std::vector<bwt_int> data = std::vector<bwt_int>(data_ptr, data_ptr + B * 2);

    block_access += 1;
    auto page = build_page(data);
    std::tuple<bool, bwt_int> result = lookup_page(page, i, page_id);
    if (std::get<0>(result) == true) {
        return std::get<1>(result);
    } else {
        return lookup_from_s3(s3_client, object_request, btree_offset, std::get<1>(result), i, block_access);
    }

}

std::tuple<bwt_int, bwt_int> search_from_file(std::string filename, const char *P, size_t Psize) {

    FILE *fp = fopen(filename.c_str(), "rb");
    // first read the first ALPHABET * 8 bytes
    bwt_int data[ALPHABET * 2 + 2];
    fread(data, sizeof(bwt_int), ALPHABET * 2 + 2, fp);
    std::vector<bwt_int> offsets;
    std::vector<bwt_int> C;
    for (int i = 0; i < ALPHABET; i++) {
        offsets.push_back(data[i * 2]);
        C.push_back(data[i * 2 + 1]);
    }
    offsets.push_back(data[ALPHABET * 2]);
    size_t n = data[ALPHABET * 2 + 1];
    std::cout << "corpus length: " << n << std::endl;

    bwt_int start = 0;
    bwt_int end = n + 1;
    int block_access = 0;
    // use the FM index to search for the probe
    for(int i = Psize - 1; i >= 0; i --) {
        char c  = P[i];
        size_t num_pages = (offsets[c + 1] - offsets[c]) / (B * sizeof(bwt_int) * 2);
        if (num_pages == 0) {
            std::cout << "not found" << std::endl;
            std::make_tuple(-1, -1);
        }
        //set the file pointer to the root page of this tree
        start = C[c] + lookup_from_file(fp, offsets[c], num_pages - 1 , start, block_access);
        end = C[c] + lookup_from_file(fp, offsets[c], num_pages - 1, end, block_access);
        if (start >= end) {
            std::cout << "not found" << std::endl;
            std::make_tuple(-1, -1);
        }
    }
    std::cout << "start: " << start << std::endl;
    std::cout << "end: " << end << std::endl;
    std::cout << "total block accesses: " << block_access << std::endl;
    fclose(fp);
    return std::make_tuple(start, end);
    
}

std::tuple<bwt_int, bwt_int> search_from_s3_object(const Aws::S3::S3Client& s3_client, const Aws::String bucket_name, const Aws::String object_name, const char *P, size_t Psize) {

    // read the first ALPHABET * 8 bytes
    Aws::S3::Model::GetObjectRequest object_request;
    object_request.WithBucket(bucket_name).WithKey(object_name);
    std::vector<char> buffer = read_byte_range_from_S3(s3_client, object_request, 0,  ALPHABET * sizeof(bwt_int) * 2 + sizeof(bwt_int) * 2);
    const bwt_int* data = reinterpret_cast<const bwt_int*>(buffer.data());

    std::vector<bwt_int> offsets;
    std::vector<bwt_int> C;
    for (int i = 0; i < ALPHABET; i++) {
        offsets.push_back(data[i * 2]);
        C.push_back(data[i * 2 + 1]);
    }
    offsets.push_back(data[ALPHABET * 2]);
    size_t n = data[ALPHABET * 2 + 1];
    std::cout << "corpus length: " << n << std::endl;

    bwt_int start = 0;
    bwt_int end = n + 1;
    int block_access = 0;
    // use the FM index to search for the probe
    for(int i = Psize - 1; i >= 0; i --) {
        char c  = P[i];
        size_t num_pages = (offsets[c + 1] - offsets[c]) / (B * sizeof(bwt_int) * 2);
        if (num_pages == 0) {
            std::cout << "not found" << std::endl;
            return std::make_tuple(-1, -1);
        }
        //set the file pointer to the root page of this tree
        start = C[c] + lookup_from_s3(s3_client, object_request, offsets[c], num_pages - 1 , start, block_access);
        end = C[c] + lookup_from_s3(s3_client, object_request, offsets[c], num_pages - 1, end, block_access);
        if (start >= end) {
            std::cout << "not found" << std::endl;
            return std::make_tuple(-1, -1);
        }
    }
    std::cout << "start: " << start << std::endl;
    std::cout << "end: " << end << std::endl;
    std::cout << "total block accesses: " << block_access << std::endl;
    return std::make_tuple(start, end);
    
}

std::ostream& operator<<(std::ostream& os, const btree_t& tree) {
    for (const auto& row : tree) {
        for (const auto& elem : row) {
            os << "(" << std::get<0>(elem) << ", " << std::get<1>(elem) << ") ";
        }
        os << std::endl;
    }
    return os;
}

int main(int argc, const char *argv[1]) {

    // usage: fts index <infile> <log_idx_file> <fm_file>
    // usage: fts query-disk <infile> <log_idx_file> <fm_file> <pattern>
    // usage: fts query-s3 <bucket> <infile> <log_idx_file> <fm_file> <pattern>

    const char * mode = argv[1];

    if (strcmp(mode, "index") == 0) {

        assert(argc == 5);
        
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

        std::vector<bwt_int> C(ALPHABET, 0);
        std::vector<std::vector<bwt_int>> FM_index;
        for (int i = 0; i < ALPHABET; i++) {
            FM_index.push_back({});
        }

        int n = strlen(Text);
        // allocate
        int *SA = (int *)malloc(n * sizeof(int));
        // sort
        divsufsort((unsigned char *)Text, SA,  strlen(Text));

        // since we are doing this for log files, we also need to keep track of *which* log every element of the suffix array points to.
        std::vector<bwt_int> log_idx (n + 1, 0);

        std::cout << Text[n-1] << std::endl;
        // assert(Text[n - 1] == '\n');
        //first make an auxiliary structure that records where each newline character is
        std::vector<bwt_int> newlines = {};
        for (int i = 0; i < n; i++) {
            if (Text[i] == '\n') {
                newlines.push_back(i);
            }
        }

        std::cout << "detected " << newlines.size() << " logs " << std::endl;
        std::cout << newlines[newlines.size() - 1] << std::endl;

        // the suffix array does not output the last character as the first character so add it here
        FM_index[Text[n - 1]].push_back(0);
        // get the second to last element of newlines, since the last element is always the length of the file assuming file ends with "\n"
        log_idx[0] = newlines[newlines.size() - 2];

        for(int i = 0; i < n; ++i) {
            // printf("%c", Text[SA[i] - 1]);
            // printf("\n");
            char c = Text[SA[i] - 1];
            FM_index[c].push_back(i + 1);

            // we need to binary search in the newlines array to find the index of the log that this suffix array element belongs to
            // we can do this by binary searching for the first element in newlines that is larger than SA[i] - 1   
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

        // write out log_idx to a binary file on disk
        FILE *log_idx_fp = fopen(argv[3], "wb");
        fwrite(&log_idx[0], sizeof(bwt_int), log_idx.size(), log_idx_fp);

        for(int i = 0; i < ALPHABET; i++) {
            for(int j = 0; j < i; j ++)
                C[i] += FM_index[j].size();
        }

        std::vector<btree_t> btrees = {};
        for(int i = 0; i < ALPHABET; i ++) {
            if (FM_index[i].size() == 0) {
                btrees.push_back({});
                continue;
            }
            printf("%c (%ld):", i, C[i]);
            // for (int j = 0; j < FM_index[i].size(); j++) {
            //     printf("%d ", FM_index[i][j]);
            // }
            printf("\n");
            btree_t btree = construct_btree(&FM_index[i][0], FM_index[i].size());
            btrees.push_back(btree);
            
            // std::cout << "btree: " << btree << std::endl;
            // std::cout << "lookup: " << lookup(btree, btree.size() - 1, 0) << std::endl;
        }

        // search(btrees, C, P, Psize, n);
        write_btrees_to_disk(btrees, C, n, argv[4]);
    } else if (strcmp(mode, "query-disk") == 0)  
    {

        assert(argc == 6);

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

        const char *P = argv[5];
        size_t Psize = strlen(P);

        //read in log_idx from argv[2]
        FILE *log_idx_fp = fopen(argv[3], "rb");
        fseek(log_idx_fp, 0, SEEK_END);
        size_t log_idx_size = ftell(log_idx_fp);
        fseek(log_idx_fp, 0, SEEK_SET);
        std::vector<bwt_int> log_idx (log_idx_size / sizeof(bwt_int));
        fread(&log_idx[0], sizeof(bwt_int), log_idx_size / sizeof(bwt_int), log_idx_fp);

        std::cout << "Query: " << P << " length "<< Psize << std::endl;
        auto result = search_from_file(argv[4], P, Psize);
        // auto result = search_from_s3_object("cluster-dump", "test.btree", P, Psize);
        bwt_int start = std::get<0>(result);
        bwt_int end = std::get<1>(result);

        for (int i = start; i < end; i++) {
            printf("log_idx %ld ", log_idx[i]);
            // now print the original text from bytes log_idx[i] to log_idx[i + 1]
            for (int j = log_idx[i] + 1; j < log_idx[i] + 1000; j++) {
                printf("%c", Text[j]);
                if(Text[j] == '\n') {
                    break;
                }
            }
            printf("\n");
        }

    } else if (strcmp(mode, "query-s3") == 0)  
    {

        assert(argc == 7);

        std::string bucket = argv[2];
        std::string object = argv[3];
        std::string log_idx_file = argv[4];
        std::string fm_file = argv[5];
        const char *P = argv[6];
        size_t Psize = strlen(P);

        Aws::SDKOptions options;
        Aws::InitAPI(options);

        Aws::Client::ClientConfiguration clientConfig;
        clientConfig.region = "us-west-2";
        clientConfig.connectTimeoutMs = 30000; // 30 seconds
        clientConfig.requestTimeoutMs = 30000; // 30 seconds
        Aws::S3::S3Client s3_client(clientConfig);

        auto result = search_from_s3_object(s3_client, "cluster-dump", "test.btree", P, Psize);

        Aws::ShutdownAPI(options);

    } else {
        std::cout << "invalid mode" << std::endl;
    }

    return 0;
}
