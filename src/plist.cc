#include "plist.h"

PListChunk::PListChunk(const std::string &compressed_data)
    : compressor_(CompressionAlgorithm::ZSTD) {

  // decompress the data
  std::string data = compressor_.decompress(compressed_data);

  // reinterpret the last 8 bytes of data as the number of posting lists
  plist_size_t num_posting_lists = *reinterpret_cast<const plist_size_t *>(
      data.data() + data.size() - sizeof(plist_size_t));

  // Read the bit array
  std::vector<bool> bit_array;
  for (plist_size_t i = 0; i < num_posting_lists; ++i) {
    bit_array.push_back(data[i / 8] & (1 << (i % 8)));
  }

  plist_size_t cursor = (num_posting_lists + 7) / 8;

  // Read the count array
  std::vector<plist_size_t> count_array;
  for (bool bit : bit_array) {
    if (bit) {
      count_array.push_back(
          *reinterpret_cast<const plist_size_t *>(data.data() + cursor));
      cursor += sizeof(plist_size_t);
    } else {
      count_array.push_back(1);
    }
  }

  data_ = {};
  // the actual posting lists must have size_t, plist_size_t is just for counts
  // etc. and usually can be 4 bytes

  for (plist_size_t count : count_array) {
    std::vector<plist_size_t> posting_list;
    for (plist_size_t i = 0; i < count; ++i) {
      posting_list.push_back(
          *reinterpret_cast<const plist_size_t *>(data.data() + cursor));
      cursor += sizeof(plist_size_t);
    }
    data_.push_back(posting_list);
  }
};

std::string PListChunk::serialize() {
  // Calculate the size of the serialized data
  size_t bit_array_size = (data_.size() + 7) / 8;
  size_t count_array_size = 0;
  for (const std::vector<plist_size_t> &posting_list : data_) {
    if (posting_list.size() > 1) {
      count_array_size += sizeof(plist_size_t);
    }
  }
  size_t posting_lists_size = 0;
  for (const std::vector<plist_size_t> &posting_list : data_) {
    posting_lists_size += posting_list.size() * sizeof(plist_size_t);
  }
  size_t size = bit_array_size + count_array_size + posting_lists_size +
                sizeof(plist_size_t);

  // Allocate the serialized data
  std::string serialized(size, '\0');

  // Write the bit array
  plist_size_t num_posting_lists = data_.size();
  for (plist_size_t i = 0; i < num_posting_lists; ++i) {
    if (data_[i].size() > 1) {
      serialized[i / 8] |= (1 << (i % 8));
    }
  }

  plist_size_t cursor = (num_posting_lists + 7) / 8;

  // Write the count array
  for (const std::vector<plist_size_t> &posting_list : data_) {
    if (posting_list.size() > 1) {
      *reinterpret_cast<plist_size_t *>(serialized.data() + cursor) =
          posting_list.size();
      cursor += sizeof(plist_size_t);
    }
  }

  // Write the actual posting lists
  for (const std::vector<plist_size_t> &posting_list : data_) {
    for (plist_size_t item : posting_list) {
      *reinterpret_cast<plist_size_t *>(serialized.data() + cursor) = item;
      cursor += sizeof(plist_size_t);
    }
  }

  // Write the number of posting lists
  *reinterpret_cast<plist_size_t *>(serialized.data() + cursor) =
      num_posting_lists;

  std::string compressed_result =
      compressor_.compress(serialized.data(), serialized.size());
  return compressed_result;
}