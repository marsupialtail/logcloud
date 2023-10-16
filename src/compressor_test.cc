#include "compressor.h"
#include <cassert>

int main() {
    std::string inputData = "This is a test string for compression.";

    Compressor compressorZstd(CompressionAlgorithm::ZSTD);
    std::string compressedZstd = compressorZstd.compress(inputData.c_str(), inputData.size());
    std::string decompressedZstd = compressorZstd.decompress(compressedZstd);
    assert(inputData == decompressedZstd);

    Compressor compressorSnappy(CompressionAlgorithm::SNAPPY);
    std::string compressedSnappy = compressorSnappy.compress(inputData.c_str(), inputData.size());
    std::string decompressedSnappy = compressorSnappy.decompress(compressedSnappy);
    assert(inputData == decompressedSnappy);

    Compressor compressorLZ4(CompressionAlgorithm::LZ4);
    std::string compressedLZ4 = compressorLZ4.compress(inputData.c_str(), inputData.size());
    std::string decompressedLZ4 = compressorLZ4.decompress(compressedLZ4);
    assert(inputData == decompressedLZ4);

    return 0;
}
