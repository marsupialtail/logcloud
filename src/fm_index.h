#pragma once

#include "compressor.h"
#include "glog/logging.h"
#include "vfr.h"
#include <iostream>
#include <map>
#include <string.h>
#include <tuple>
#include <vector>

#define ALPHABET 256
#define CHUNK_CHARS 1000000
typedef std::tuple<std::map<char, size_t>, std::string> fm_chunk;
typedef std::vector<fm_chunk> fm_index_t;

std::string serializeMap(const std::map<char, size_t> &map) {
  std::stringstream ss;
  for (const auto &pair : map) {
    ss << pair.first << ':' << pair.second << ',';
  }
  std::string serializedString = ss.str();
  if (serializedString.size() > 0) {
    serializedString.pop_back();
  } // Remove the trailing comma
  return serializedString;
}

std::map<char, size_t> deserializeMap(const std::string &serializedString) {

  if (serializedString.size() == 0) {
    return {};
  }

  std::map<char, size_t> map;
  std::stringstream ss(serializedString);
  char key;
  size_t value;

  while (ss >> key && ss.ignore() && ss >> value) {
    map[key] = value;
    ss.ignore(); // Skip the comma
  }

  return map;
}

std::string serializeChunk(const fm_chunk &chunk) {
  Compressor compressor(CompressionAlgorithm::ZSTD);
  std::string serialized_map = serializeMap(std::get<0>(chunk));
  // compress it
  std::string compressed_map =
      compressor.compress(serialized_map.c_str(), serialized_map.size());
  size_t compressed_map_size = compressed_map.size();
  // we are going to write out this chunk as length of compressed map,
  // compressed map, compressed string
  std::string compressed_chunk = compressor.compress(std::get<1>(chunk).c_str(),
                                                     std::get<1>(chunk).size());

  std::string serialized_chunk;
  serialized_chunk.resize(sizeof(size_t) + compressed_map_size +
                          compressed_chunk.size());
  memcpy((void *)serialized_chunk.data(), &compressed_map_size, sizeof(size_t));
  memcpy((void *)(serialized_chunk.data() + sizeof(size_t)),
         compressed_map.data(), compressed_map_size);
  memcpy(
      (void *)(serialized_chunk.data() + sizeof(size_t) + compressed_map_size),
      compressed_chunk.data(), compressed_chunk.size());
  return serialized_chunk;
}

fm_chunk deserializeChunk(const std::string &serializedChunk) {
  Compressor compressor(CompressionAlgorithm::ZSTD);
  size_t compressed_map_size;
  memcpy(&compressed_map_size, serializedChunk.data(), sizeof(size_t));
  std::string compressed_map(serializedChunk.data() + sizeof(size_t),
                             compressed_map_size);
  std::string decompressed_map = compressor.decompress(compressed_map);
  std::map<char, size_t> map = deserializeMap(decompressed_map);
  std::string compressed_chunk(
      serializedChunk.data() + sizeof(size_t) + compressed_map_size,
      serializedChunk.size() - sizeof(size_t) - compressed_map_size);
  std::string decompressed_chunk = compressor.decompress(compressed_chunk);
  return std::make_tuple(map, decompressed_chunk);
}

size_t searchChunk(const fm_chunk &chunk, char c, size_t pos) {

  size_t starting_offset;
  if (std::get<0>(chunk).find(c) != std::get<0>(chunk).end()) {
    starting_offset = std::get<0>(chunk).at(c);
  } else {
    starting_offset = 0;
  }
  for (size_t i = 0; i < pos; i++) {
    if (std::get<1>(chunk)[i] == c) {
      starting_offset++;
    }
  }
  return starting_offset;
}

void write_fm_index_to_disk(const fm_index_t &tree,
                            const std::vector<size_t> &C, size_t n, FILE *fp) {

  /*
     The wavelet tree consists of a vector of compressed fmchunks

     The layout of the file will be a list of compressed chunks of variable
     lengths. This will be followed by a metadata page The metadata page will
     contain the following information:
     - Chunk offsets: the byte offsets of each chunk
     - C vector: the C vector for the FM index
     - Last eight bytes of the file will be how long the metadata page is.
  */
  size_t total_length = 0;
  // iterate through the bitvectors
  std::vector<size_t> offsets = {0};

  Compressor compressor(CompressionAlgorithm::ZSTD);
  size_t base_offset = ftell(fp);

  for (size_t i = 0; i < tree.size(); i++) {
    std::string serialized_chunk = serializeChunk(tree[i]);
    fwrite(serialized_chunk.data(), 1, serialized_chunk.size(), fp);
    offsets.push_back(offsets.back() + serialized_chunk.size());
    total_length += serialized_chunk.size();
  }

  LOG(INFO) << total_length << std::endl;
  LOG(INFO) << "number of chunks" << offsets.size() << std::endl;
  // compress the offsets too
  std::string compressed_offsets = compressor.compress(
      (char *)offsets.data(), offsets.size() * sizeof(size_t));
  size_t compressed_offsets_byte_offset = ftell(fp) - base_offset;
  fwrite(compressed_offsets.data(), 1, compressed_offsets.size(), fp);
  LOG(INFO) << "compressed offsets size " << compressed_offsets.size()
            << std::endl;

  // now write out the C vector
  std::string compressed_C =
      compressor.compress((char *)C.data(), C.size() * sizeof(size_t));
  size_t compressed_C_byte_offset = ftell(fp) - base_offset;
  fwrite(compressed_C.data(), 1, compressed_C.size(), fp);

  // now write out the byte_offsets as three 8-byte numbers
  fwrite(&compressed_offsets_byte_offset, 1, sizeof(size_t), fp);
  fwrite(&compressed_C_byte_offset, 1, sizeof(size_t), fp);
  fwrite(&n, 1, sizeof(size_t), fp);
}

std::tuple<size_t, std::vector<size_t>, std::vector<size_t>>
read_metadata_from_file(VirtualFileRegion *vfr) {

  size_t file_size = vfr->size();
  vfr->vfseek(file_size - 24, SEEK_SET);

  std::vector<char> buffer(24);
  vfr->vfread(buffer.data(), 24);
  const size_t *data = reinterpret_cast<const size_t *>(buffer.data());
  size_t compressed_offsets_byte_offset = data[0];
  size_t compressed_C_byte_offset = data[1];
  size_t n = data[2];

  LOG(INFO) << "compressed_offsets_byte_offset: "
            << compressed_offsets_byte_offset << std::endl;
  LOG(INFO) << "compressed_C_byte_offset: " << compressed_C_byte_offset
            << std::endl;
  LOG(INFO) << "file size: " << file_size << std::endl;

  vfr->vfseek(compressed_offsets_byte_offset, SEEK_SET);
  buffer.clear();
  buffer.resize(file_size - 24 - compressed_offsets_byte_offset);
  vfr->vfread(buffer.data(), buffer.size());

  Compressor compressor(CompressionAlgorithm::ZSTD);
  std::string compressed_offsets(buffer.begin(),
                                 buffer.begin() + compressed_C_byte_offset -
                                     compressed_offsets_byte_offset);
  std::string decompressed_offsets = compressor.decompress(compressed_offsets);
  std::vector<size_t> offsets(decompressed_offsets.size() / sizeof(size_t));
  memcpy(offsets.data(), decompressed_offsets.data(),
         decompressed_offsets.size());

  std::string compressed_C(buffer.begin() + compressed_C_byte_offset -
                               compressed_offsets_byte_offset,
                           buffer.end());
  std::string decompressed_C = compressor.decompress(compressed_C);
  std::vector<size_t> C(decompressed_C.size() / sizeof(size_t));
  memcpy(C.data(), decompressed_C.data(), decompressed_C.size());

  vfr->reset();
  return std::make_tuple(n, C, offsets);
}

std::tuple<size_t, size_t>
search_wavelet_tree_file(VirtualFileRegion *vfr, const char *P, size_t Psize) {

  auto [n, C, offsets] = read_metadata_from_file(vfr);
  size_t start = 0;
  size_t end = n + 1;

  size_t previous_range = -1;
  // use the FM index to search for the probe
  for (int i = Psize - 1; i >= 0; i--) {
    char c = P[i];
    LOG(INFO) << "c: " << c << std::endl;

    size_t start_byte = offsets[start / CHUNK_CHARS];
    size_t end_byte = offsets[start / CHUNK_CHARS + 1];
    vfr->vfseek(start_byte, SEEK_SET);
    std::string serialized_chunk;
    serialized_chunk.resize(end_byte - start_byte);
    vfr->vfread((void *)serialized_chunk.data(), serialized_chunk.size());
    auto chunk = deserializeChunk(serialized_chunk);

    start = C[c] + searchChunk(chunk, c, start % CHUNK_CHARS);

    start_byte = offsets[end / CHUNK_CHARS];
    end_byte = offsets[end / CHUNK_CHARS + 1];
    vfr->vfseek(start_byte, SEEK_SET);
    serialized_chunk.clear();
    serialized_chunk.resize(end_byte - start_byte);
    vfr->vfread((void *)serialized_chunk.data(), serialized_chunk.size());
    chunk = deserializeChunk(serialized_chunk);

    end = C[c] + searchChunk(chunk, c, end % CHUNK_CHARS);

    LOG(INFO) << "start: " << start << std::endl;
    LOG(INFO) << "end: " << end << std::endl;
    LOG(INFO) << "range: " << end - start << std::endl;
    if (start >= end) {
      LOG(INFO) << "not found" << std::endl;
      return std::make_tuple(-1, -1);
    }
    if (end - start == previous_range) {
      LOG(INFO) << "early exit" << std::endl;
      return std::make_tuple(start, end);
    }
    previous_range = end - start;
  }

  return std::make_tuple(start, end);
}

fm_index_t construct_wavelet_tree(const char *P, size_t Psize) {

  fm_index_t to_hit = {};
  std::map<char, size_t> current_chunk_char_counts = {};
  std::map<char, size_t> next_chunk_char_counts = {};
  std::string curr_chunk = "";
  for (int idx = 0; idx < Psize; idx++) {
    char c = P[idx];
    next_chunk_char_counts[c]++;
    curr_chunk += c;
    if (curr_chunk.size() == CHUNK_CHARS) {
      std::tuple<std::map<char, size_t>, std::string> fm_chunk =
          std::make_tuple(current_chunk_char_counts, curr_chunk);
      to_hit.push_back(fm_chunk);
      curr_chunk = "";
      // copy assignment!
      current_chunk_char_counts = next_chunk_char_counts;
    }
  }
  std::tuple<std::map<char, size_t>, std::string> fm_chunk =
      std::make_tuple(current_chunk_char_counts, curr_chunk);
  to_hit.push_back(fm_chunk);
  return to_hit;
}
