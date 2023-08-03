#pragma once

#include <vector>
#include <string.h>
#include <iostream>
#include "compressor.h"
#include <tuple>

#define ALPHABET 256
#define LOG_ALPHABET 8
#define CHUNK_BITS 32768
typedef std::vector<bool> bitvector_t;
typedef std::vector<bitvector_t> wavelet_tree_t;

size_t bitvector_rank(const bitvector_t & bitvector, bool bit, size_t pos) {
    // iterate through the bits of bitvector
    size_t rank = 0;
    for (size_t i = 0; i < pos; i++) {
        if (bitvector[i] == bit) {
            rank ++;
        }
    }
    return rank;
}