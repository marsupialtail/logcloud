#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <iostream>
#include <tuple>
#include <divsufsort.h>
#include <cassert>
#include <unordered_map>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>


#define ALPHABET 256
typedef uint32_t bwt_int;
// each "page" in this btree is just a size B * 2 array, where index 2x is the element and 2x + 1 is -1 if leaf and the leave page id if not
typedef std::vector<std::tuple<bwt_int, bwt_int>> btree_page_t;
typedef std::vector<btree_page_t> btree_t;

#define B 1024

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
        printf("%c (%d):", i, C[i]);
        std::cout << offset << std::endl;
        fwrite(&offset, 4, 1, fp);
        fwrite(&C[i], 4, 1, fp);
        offset += btrees[i].size() * B * 8;
    }
    fwrite(&offset, 4, 1, fp);
    fwrite(&n, 4, 1, fp);

    // write the encoding section, skip for now, fill file up to 8192 bytes with 0
    size_t zero = 0;
    fwrite(&zero, sizeof(char), 8192 - ALPHABET * 8 - 8, fp);

    // write the btree pages
    for (int i = 0; i < ALPHABET; i++) {
        if (btrees[i].size() == 0) {
            continue;
        }
        for (int j = 0; j < btrees[i].size(); j++) {
            btree_page_t page = btrees[i][j];
            for (int k = 0; k < page.size(); k++) {
                fwrite(&std::get<0>(page[k]), 4, 1, fp);
                fwrite(&std::get<1>(page[k]), 4, 1, fp);
            }
            if (page.size() < B) {
                bwt_int fill = -1;
                for (int k = 0; k < B - page.size(); k++) {
                    fwrite(&fill, 4, 1, fp);
                    fwrite(&fill, 4, 1, fp);
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

    //leaves
    size_t page_count = 0;
    for (size_t i = 0; i < n; i += B) {
        btree_page_t page;
        for (size_t j = i; j < std::min(i + B, n); j++) {
            page.push_back(std::make_tuple(elements[j], -1));
        }
        btree.push_back(page);
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

bwt_int lookup_from_file(FILE * fp, size_t btree_offset, size_t page_id, bwt_int i, int & block_access) {
    // first go to the root page at the end of the btree

    fseek(fp, btree_offset + page_id * 8 * B, SEEK_SET);
    btree_page_t page = {};
    bwt_int data[B * 2];
    block_access += 1;
    fread(data, 4, B * 2, fp);
    for (int j = 0; j < B; j++) {
        // don't read in the fillers
        if (data[j * 2] == -1 && data[j * 2 + 1] == -1) {
            break;
        }
        page.push_back(std::make_tuple(data[j * 2], data[j * 2 + 1]));
    }

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
        // assert(page_id == btree.size() - 1);
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
        return lookup_from_file(fp, btree_offset, std::get<1>(elem), i, block_access);
    }

}

void search_from_file(std::string filename, const char *P, size_t Psize) {

    std::cout << "WARNING: THIS ONLY WORKS FOR btrees FILES SMALLER THAN 4GB, offset datatype is u32!!!" << std::endl;

    FILE *fp = fopen(filename.c_str(), "rb");
    // first read the first ALPHABET * 8 bytes
    bwt_int data[ALPHABET * 2 + 2];
    fread(data, 4, ALPHABET * 2 + 2, fp);
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
        size_t num_pages = (offsets[c + 1] - offsets[c]) / (B * 8);
        if (num_pages == 0) {
            std::cout << "not found" << std::endl;
            return;
        }
        //set the file pointer to the root page of this tree
        start = C[c] + lookup_from_file(fp, offsets[c], num_pages - 1 , start, block_access);
        end = C[c] + lookup_from_file(fp, offsets[c], num_pages - 1, end, block_access);
        if (start >= end) {
            std::cout << "not found" << std::endl;
            return;
        }
    }
    std::cout << "start: " << start << std::endl;
    std::cout << "end: " << end << std::endl;
    std::cout << "total block accesses: " << block_access << std::endl;
    fclose(fp);
    
}

bwt_int lookup_from_s3(Aws::S3::S3Client & s3_client, Aws::S3::Model::GetObjectRequest & object_request, size_t btree_offset, size_t page_id, bwt_int i, int & block_access) {
    // first go to the root page at the end of the btree

    // fseek(fp, btree_offset + page_id * 8 * B, SEEK_SET);
    btree_page_t page = {};
    // bwt_int data[B * 2];
    size_t start_byte = btree_offset + page_id * 8 * B;
    size_t end_byte = start_byte + 8 * B - 1;
    object_request.SetRange("bytes=" + std::to_string(start_byte) + "-" + std::to_string(end_byte));
    auto get_object_outcome = s3_client.GetObject(object_request);
    assert(get_object_outcome.IsSuccess());
    auto &retrieved_data = get_object_outcome.GetResultWithOwnership().GetBody();
    // Create a buffer to hold the data.
    std::vector<char> buffer(std::istreambuf_iterator<char>(retrieved_data), {});
    const bwt_int* data = reinterpret_cast<const bwt_int*>(buffer.data());

    block_access += 1;
    // fread(data, 4, B * 2, fp);
    for (int j = 0; j < B; j++) {
        // don't read in the fillers
        if (data[j * 2] == -1 && data[j * 2 + 1] == -1) {
            break;
        }
        page.push_back(std::make_tuple(data[j * 2], data[j * 2 + 1]));
    }

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
        // assert(page_id == btree.size() - 1);
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
        return lookup_from_s3(s3_client, object_request, btree_offset, std::get<1>(elem), i, block_access);
    }

}

void search_from_s3_object(std::string bucket, std::string object, const char *P, size_t Psize) {

    std::cout << "WARNING: THIS ONLY WORKS FOR btrees objects SMALLER THAN 4GB, offset datatype is u32!!!" << std::endl;

    Aws::SDKOptions options;
    Aws::InitAPI(options);
    const Aws::String bucket_name = bucket; 
    const Aws::String object_name = object;

    Aws::Client::ClientConfiguration clientConfig;
    clientConfig.region = "us-west-2";
    clientConfig.connectTimeoutMs = 30000; // 30 seconds
    clientConfig.requestTimeoutMs = 30000; // 30 seconds
    Aws::S3::S3Client s3_client(clientConfig);

    Aws::S3::Model::GetObjectRequest object_request;
    object_request.WithBucket(bucket_name).WithKey(object_name);

    // first read the first ALPHABET * 8 bytes
    // bwt_int data[ALPHABET * 2 + 2];
    // fread(data, 4, ALPHABET * 2 + 2, fp);

    object_request.SetRange("bytes=" + std::to_string(0) + "-" + std::to_string(ALPHABET * 8 + 8));
    auto get_object_outcome = s3_client.GetObject(object_request);
    assert(get_object_outcome.IsSuccess());
    auto &retrieved_data = get_object_outcome.GetResultWithOwnership().GetBody();
    // Create a buffer to hold the data.
    std::vector<char> buffer(std::istreambuf_iterator<char>(retrieved_data), {});
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
        size_t num_pages = (offsets[c + 1] - offsets[c]) / (B * 8);
        if (num_pages == 0) {
            std::cout << "not found" << std::endl;
            return;
        }
        //set the file pointer to the root page of this tree
        start = C[c] + lookup_from_s3(s3_client, object_request, offsets[c], num_pages - 1 , start, block_access);
        end = C[c] + lookup_from_s3(s3_client, object_request, offsets[c], num_pages - 1, end, block_access);
        if (start >= end) {
            std::cout << "not found" << std::endl;
            return;
        }
    }
    std::cout << "start: " << start << std::endl;
    std::cout << "end: " << end << std::endl;
    std::cout << "total block accesses: " << block_access << std::endl;
    
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
    // intput data

    char *Text;
    // open the file handle with filename in argv[1] into Text
    FILE *fp = fopen(argv[1], "r");
    fseek(fp, 0, SEEK_END);
    size_t size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    // TODO: currently this skips the last \n by force

    Text = (char *)malloc(size - 1);
    fread(Text, 1, size -1, fp);
    fclose(fp);
    std::cout << Text[size-2] << std::endl;

    const char *P = argv[2];
    size_t Psize = strlen(P);

    std::cout << "Query: " << P << " length "<< Psize << std::endl;

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
    std::vector<bwt_int> log_idx = {};

    // the suffix array does not output the last character as the first character so add it here
    FM_index[Text[n - 1]].push_back(0);
    for(int i = 0; i < n; ++i) {
        // printf("%c", Text[SA[i] - 1]);
        // printf("\n");
        char c = Text[SA[i] - 1];
        FM_index[c].push_back(i + 1);
    }

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
        printf("%c (%d):", i, C[i]);
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
    // write_btrees_to_disk(btrees, C, n, "test.btree");
    // search_from_file("test.btree", P, Psize);
    search_from_s3_object("cluster-dump", "test.btree", P, Psize);


    return 0;
}
