/*
This contains the posting list class, a class that stores a list of sorted lists, where the first element in those lists is also sorted.
The class contains serialization, deserialization and lookup methods.

The layout of the serialized data chunk: 
- bit array indicating whether or not each posting list has more than one item
- count array indicating the number of items in each posting list with more than one item
- all the posting lists concatenated together
- 8 bytes denoting the number of posting lists
*/
#pragma once
#include <cassert>
#include "../compressor.h"
#include <vector>
typedef uint32_t plist_size_t;

class PListChunk {
    public:
        // Constructor method from a vector of vector of size_t, take ownership of it from the caller
        PListChunk(std::vector<std::vector<plist_size_t>> &&data) : data_(std::move(data)), compressor_(CompressionAlgorithm::ZSTD) {}
        
        // Constructor method from a compressed string
        PListChunk(const std::string &data);

        // Serialize the data
        std::string serialize();

        std::vector<std::vector<plist_size_t>> &data() {
            return data_;
        }

        // return a copy
        std::vector<plist_size_t> lookup(size_t key) {
            assert(data_[key].size() > 0);
            return data_[key];
        }
    private:
        std::vector<std::vector<plist_size_t>> data_;
        Compressor compressor_;
};