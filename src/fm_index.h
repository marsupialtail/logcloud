#pragma once

#include <cassert>
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <map>

#include <divsufsort.h>
#include "glog/logging.h"

#include "compressor.h"
#include "vfr.h"

#define LOG_IDX_CHUNK_BYTES (1024 * 1024)
#define GIVEUP 100
#define ALPHABET 256
#define FM_IDX_CHUNK_CHARS 1000000 // fm index chunking granualarity, has nothing to do with chunking for hawaii
typedef std::tuple<std::map<char, size_t>, std::string> fm_chunk;
typedef std::vector<fm_chunk> fm_index_t;

std::string serializeMap(const std::map<char, size_t> &map);
std::map<char, size_t> deserializeMap(const std::string &serializedString);

std::string serializeChunk(const fm_chunk &chunk);

fm_chunk deserializeChunk(const std::string &serializedChunk);

size_t searchChunk(const fm_chunk &chunk, char c, size_t pos);
void write_fm_index_to_disk(const fm_index_t &tree,
							const std::vector<size_t> &C, size_t n, FILE *fp);

std::tuple<size_t, std::vector<size_t>, std::vector<size_t>>
read_metadata_from_file(VirtualFileRegion *vfr);

std::tuple<size_t, size_t>
search_fm_index(VirtualFileRegion *vfr, const char *P, size_t Psize, bool early_exit);

fm_index_t construct_fm_index(const char *P, size_t Psize);

std::vector<size_t> search_vfr(VirtualFileRegion *wavelet_vfr,
							   VirtualFileRegion *log_idx_vfr,
							   std::string query,
                               bool early_exit = true);

std::tuple<fm_index_t, std::vector<size_t>, std::vector<size_t>>
bwt_and_build_fm_index(char *Text, size_t block_lines = -1);

void write_log_idx_to_disk(std::vector<size_t> log_idx, FILE *log_idx_fp);