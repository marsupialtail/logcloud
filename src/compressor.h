#pragma once
#include <iostream>
#include <lz4.h>
#include <snappy.h>
#include <stdexcept>
#include <string>
#include <zstd.h>

enum class CompressionAlgorithm { ZSTD, SNAPPY, LZ4 };

class Compressor {
  public:
	explicit Compressor(CompressionAlgorithm algorithm)
		: algorithm_(algorithm) {}

	std::string compress(const char *data, size_t size,
						 int compression_level = 5) {
		if (size == 0) {
			return "";
		}
		switch (algorithm_) {
		case CompressionAlgorithm::ZSTD:
			return compressZstd(data, size, compression_level);
		case CompressionAlgorithm::SNAPPY:
			return compressSnappy(data, size);
		case CompressionAlgorithm::LZ4:
			return compressLZ4(data, size);
		default:
			throw std::runtime_error("Unsupported compression algorithm.");
		}
	}

	std::string decompress(const std::string &compressedData) {
		switch (algorithm_) {
		case CompressionAlgorithm::ZSTD:
			return decompressZstd(compressedData);
		case CompressionAlgorithm::SNAPPY:
			return decompressSnappy(compressedData);
		case CompressionAlgorithm::LZ4:
			return decompressLZ4(compressedData);
		default:
			throw std::runtime_error("Unsupported compression algorithm.");
		}
	}

  private:
	CompressionAlgorithm algorithm_;

	std::string compressZstd(const char *data, size_t size,
							 int compression_level = 1) {
		size_t compressedSize = ZSTD_compressBound(size);
		// ZSTD_CCtx* cctx = ZSTD_createCCtx();
		// if(cctx == NULL) {
		//     std::cerr << "Failed to create ZSTD_CCtx\n";
		//     exit(1);
		// }
		// if (ZSTD_isError(ZSTD_CCtx_setParameter(cctx, ZSTD_c_nbWorkers, 8)))
		// {
		//     std::cout << "Failed to set worker threads\n";
		//     exit(1);
		// }
		std::string compressed(compressedSize, '\0');
		compressedSize = ZSTD_compress(compressed.data(), compressedSize, data,
									   size, compression_level);
		// compressedSize = ZSTD_compressCCtx(cctx, compressed.data(),
		// compressedSize, data, size, compression_level); ZSTD_freeCCtx(cctx);
		return std::string(compressed.data(), compressedSize);
	}

	std::string decompressZstd(const std::string &compressedData) {
		size_t decompressedSize = ZSTD_getDecompressedSize(
			compressedData.c_str(), compressedData.size());
		if (decompressedSize == -1) {
			throw std::runtime_error("Decompression failed.");
		}
		std::string decompressed(decompressedSize, '\0');
		decompressedSize =
			ZSTD_decompress(decompressed.data(), decompressedSize,
							compressedData.c_str(), compressedData.size());
		return std::string(decompressed.data(), decompressedSize);
	}

	std::string compressSnappy(const char *data, size_t size) {
		std::string compressed;
		snappy::Compress(data, size, &compressed);
		return compressed;
	}

	std::string decompressSnappy(const std::string &compressedData) {
		std::string decompressed;
		if (!snappy::Uncompress(compressedData.data(), compressedData.size(),
								&decompressed)) {
			throw std::runtime_error("Failed to decompress data using Snappy.");
		}
		return decompressed;
	}

	std::string compressLZ4(const char *data, size_t size) {
		int compressedSize = LZ4_compressBound(size);
		std::string compressed(compressedSize, '\0');
		int actualSize =
			LZ4_compress_default(data, compressed.data(), size, compressedSize);
		return std::string(compressed.data(), actualSize);
	}

	std::string decompressLZ4(const std::string &compressedData) {
		// Get the decompressed size as an upper bound (assumption: decompressed
		// size <= compressed size * 4)
		int decompressedSize = compressedData.size() * 4;
		std::string decompressed(decompressedSize, '\0');

		// Perform the decompression
		int actualSize =
			LZ4_decompress_safe(compressedData.c_str(), decompressed.data(),
								compressedData.size(), decompressedSize);

		if (actualSize < 0) {
			throw std::runtime_error("Failed to decompress data using LZ4.");
		}

		// Resize the decompressed string to the actual decompressed size
		decompressed.resize(actualSize);

		return decompressed;
	}
};