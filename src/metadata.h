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

class OahuMetadataPage : public MetadataPage {

    /* The metadata page will have the following data structures:
	- num_types: 8 bytes indicating the number of types, N
    - num blocks: 8 bytes indicating the number of blocks, M
    - type_order: 8 bytes for each type in a list of length N, indicating the order of the types
	- 8 bytes for each type in a list of length N, indicating the offset into
	the block-byte-offset array for each type
	- 8 bytes for each block, denoting block offset */

    public:
        size_t num_types;
        size_t num_blocks;
        std::vector<int> type_order = {};
        std::vector<size_t> type_offsets = {};
        std::vector<size_t> byte_offsets = {};

        OahuMetadataPage(size_t num_types, size_t num_blocks, std::vector<int> type_order, std::vector<size_t> type_offsets, std::vector<size_t> byte_offsets)
            : num_types(num_types), num_blocks(num_blocks), type_order(type_order), type_offsets(type_offsets), byte_offsets(byte_offsets) {}
        
        OahuMetadataPage(std::string compressed_page) {
            Compressor compressor(CompressionAlgorithm::ZSTD);
            std::string decompressed_metadata_page =
                compressor.decompress(compressed_page);

            // read the number of types
            num_types =
                *reinterpret_cast<const size_t *>(decompressed_metadata_page.data());
            // read the number of blocks
            num_blocks = *reinterpret_cast<const size_t *>(
                decompressed_metadata_page.data() + sizeof(size_t));

            // read the type order
            for (size_t i = 0; i < num_types; ++i) {
                type_order.push_back(*reinterpret_cast<const int *>(
                    decompressed_metadata_page.data() + 2 * sizeof(size_t) +
                    i * sizeof(int)));
            }

            // read the type offsets
            for (size_t i = 0; i < num_types + 1; ++i) {
                type_offsets.push_back(*reinterpret_cast<const size_t *>(
                    decompressed_metadata_page.data() + 2 * sizeof(size_t) +
                    num_types * sizeof(int) + i * sizeof(size_t)));
            }

            // read the block offsets
            for (size_t i = 0; i < num_blocks + 1; ++i) {
                byte_offsets.push_back(*reinterpret_cast<const size_t *>(
                    decompressed_metadata_page.data() + 2 * sizeof(size_t) + num_types * sizeof(int)
                    + num_types * sizeof(size_t) + sizeof(size_t) + i * sizeof(size_t)));
            }       
        }

        std::string compress() {
            std::string metadata_page = "";

            metadata_page += std::string((char *)&num_types, sizeof(size_t));
            metadata_page += std::string((char *)&num_blocks, sizeof(size_t));

            for (int item : type_order) {
                metadata_page += std::string((char *)&item, sizeof(int));
            }

            for (size_t type_offset : type_offsets) {
                metadata_page += std::string((char *)&type_offset, sizeof(size_t));
            }

            for (size_t byte_offset : byte_offsets) {
                metadata_page += std::string((char *)&byte_offset, sizeof(size_t));
            }

            std::string compressed_metadata_page =
                Compressor(CompressionAlgorithm::ZSTD)
                    .compress(metadata_page.c_str(), metadata_page.size());

            return compressed_metadata_page;
        }
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
		num_types = *reinterpret_cast<const size_t *>(
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