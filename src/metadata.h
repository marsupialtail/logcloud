#pragma once
#include "compressor.h"
#include "glog/logging.h"
#include <cstdint>
#include <cstdio>
#include <map>
#include <string>
#include <vector>
// define an abstract class MetadataPage

class MetadataPage {
  public:
	virtual ~MetadataPage() = default;
	virtual std::string compress() = 0;
};

class HawaiiMetadataPage : public MetadataPage {

	/*
   Reminder:
   The layout of the compressed metadata page
    - num_types: 8 bytes indicating the number of types, N
    - type_order: 8 bytes for each type in a list of length N, indicating the order of the types
    in the blocks, e.g. 1, 21, 53, 63
    - byte_offsets: (16 bytes * N + 8 bytes) for each type in a list of length N, indicating the byte offset for 
    the fm index and the logidx for each type. 
   */

  public:
	size_t num_types;
	std::vector<int> type_order = {};
	std::vector<size_t> byte_offsets = {};

	HawaiiMetadataPage(size_t num_types, 
					   std::vector<int> type_order, 
					   std::vector<size_t> byte_offsets)
		: num_types(num_types),
		  type_order(type_order),
		  byte_offsets(byte_offsets) {}

	HawaiiMetadataPage(std::string compressed_page) {
		std::string decompressed_metadata_page =
			Compressor(CompressionAlgorithm::ZSTD)
				.decompress(compressed_page);

		// read the number of types
		size_t num_types = *reinterpret_cast<const size_t *>(
			decompressed_metadata_page.data());
		LOG(INFO) << "num types: " << num_types << "\n";

		// read the type order
		for (size_t i = 0; i < num_types; ++i) {
			type_order.push_back(*reinterpret_cast<const int *>(
				decompressed_metadata_page.data() + sizeof(size_t) +
				i * sizeof(int)));
			LOG(INFO) << "type order: " << type_order[i] << "\n";
		}

		// read the byte offsets
		for (size_t i = 0; i < num_types * 2 + 1; ++i) {
			byte_offsets.push_back(*reinterpret_cast<const size_t *>(
				decompressed_metadata_page.data() + sizeof(size_t) + num_types * sizeof(int) + i * sizeof(size_t)));
			LOG(INFO) << "group offsets: " << byte_offsets[i] << "\n";
		}
	}

	std::string compress() {
		std::string metadata_page = "";

		metadata_page += std::string((char *)&num_types, sizeof(size_t));

		for (int item : type_order) {
			metadata_page += std::string((char *)&item, sizeof(int));
		}

		for (size_t byte_offset : byte_offsets) {
			metadata_page += std::string((char *)&byte_offset, sizeof(size_t));
		}

		std::string compressed_metadata_page =
			Compressor(CompressionAlgorithm::ZSTD)
				.compress(metadata_page.c_str(), metadata_page.size());

		return compressed_metadata_page;
	}

	~HawaiiMetadataPage() = default;
};