#include "index.h"

/*
This step is going to first discover all the compressed/compacted_type* files
For each of the compacted type, we are going to read in the data and write the
plists each compacted type will have its own blocking parameter, such that the
each block is within X bytes compressed we need to record all the blocking
parameters for each compacted type Then we are going to write the compressed
blocks for each type into a single file, we need to remember the offset for each
block

The layout of each compressed block
8 bytes indicating length of compressed strings | compressed strings |
compressed posting list. This is not compressed again

The layout of the entire file
type_A_block_0 | type_A_block_1 | ... | type_B_block_0 | .... | compressed
metadata page | 8 bytes indicating the length of the compressed metadata page

For layout of the metadata file refer to metadata.h

*/

#define BLOCK_BYTE_LIMIT 1000000 // chunk size for oahu
#define BRUTE_THRESHOLD 5 // if the number of chunks is less than this, brute force, don't build fm index

using namespace std;

std::vector<plist_size_t> search_oahu(VirtualFileRegion *vfr, 
                                      int query_type,
									  std::vector<size_t> chunks,
									  std::string query_str) {
	// read the metadata page

	// read the last 8 bytes to figure out the length of the metadata page
	vfr->vfseek(-sizeof(size_t), SEEK_END);
	size_t metadata_page_length;
	vfr->vfread(&metadata_page_length, sizeof(size_t));

	// read the metadata page
	vfr->vfseek(-sizeof(size_t) - metadata_page_length, SEEK_END);
	std::string metadata_page;
	metadata_page.resize(metadata_page_length);
	vfr->vfread(&metadata_page[0], metadata_page_length);

    Compressor compressor(CompressionAlgorithm::ZSTD);

	// decompress the metadata page
	OahuMetadataPage oahu_metadata_page(metadata_page);
    std::vector<int>& type_order = oahu_metadata_page.type_order;
    std::vector<size_t>& type_offsets = oahu_metadata_page.type_offsets;
    std::vector<size_t>& block_offsets = oahu_metadata_page.byte_offsets;

	// figure out where in the type_order is query_type
	auto it = std::find(type_order.begin(), type_order.end(), query_type);
	// if not found return empty vector
	if (it == type_order.end()) {
		return {};
	}

	size_t type_index = std::distance(type_order.begin(), it);
	size_t type_offset = type_offsets[type_index];
	size_t num_chunks = type_offsets.at(type_index + 1) - type_offset;

	// go through the blocks
	std::vector<plist_size_t> row_groups = {};

#pragma omp parallel for
	for (size_t i = 0; i < std::min(chunks.size(), num_chunks); ++i) {
		size_t block_offset = block_offsets[type_offset + chunks[i]];
		size_t next_block_offset = block_offsets[type_offset + chunks[i] + 1];
		size_t block_size = next_block_offset - block_offset;

		// read the block

		VirtualFileRegion *local_vfr = vfr->slice(block_offset, block_size);
		std::string block;
		block.resize(block_size);
		local_vfr->vfread(&block[0], block_size);

		// read the length of the compressed strings
		size_t compressed_strings_length =
			*reinterpret_cast<const size_t *>(block.data());
		std::string compressed_strings =
			block.substr(sizeof(size_t), compressed_strings_length);
		std::string decompressed_strings =
			compressor.decompress(compressed_strings);

		// read the compressed posting list
		std::string compressed_plist =
			block.substr(sizeof(size_t) + compressed_strings_length);
		PListChunk plist(compressed_plist);

		// decompressed strings will be delimited by \n, figure out which lines
		// actually contain the query_str
		std::istringstream iss(decompressed_strings);
		std::string line;
		size_t line_number = 0;
		while (std::getline(iss, line)) {
			// check if line contains query_str anywhere in the line
			if (("\n" + line + "\n").find(query_str) != std::string::npos) {
				// lookup the query string
				std::vector<plist_size_t> result = plist.lookup(line_number);
#pragma omp critical
				{
					row_groups.insert(row_groups.end(), result.begin(),
									  result.end());
				}
			}
			++line_number;
		}
	}

	return row_groups;
}

std::map<int, size_t> write_oahu(
    std::map<int, std::string> type_input_files,
    std::string output_name) {

	// first figure out the number of types by listing all
	// compressed/compacted_type* files
	std::vector<int> types;
	for (const auto &entry :
		 std::filesystem::directory_iterator("compressed")) {
		std::string path = entry.path();
		if (path.find("compacted_type") != std::string::npos &&
			path.find("lineno") == std::string::npos) {
			int type = stoi(path.substr(path.find("compacted_type") + 15));
			// 0 is for dictionary items. We don't need to process them
			if (type != 0) {
				types.push_back(type);
			}
		}
	}

	auto compressor = Compressor(CompressionAlgorithm::ZSTD);

	FILE *fp = fopen((output_name + ".oahu").c_str(), "wb");
	std::vector<size_t> byte_offsets = {0};
	std::vector<size_t> type_offsets = {0};

	std::map<int, size_t> type_uncompressed_lines_in_block = {};

	// go through the types
	for (int type : types) {
		std::string string_file_path =
			"compressed/compacted_type_" + std::to_string(type);
		std::string lineno_file_path =
			"compressed/compacted_type_" + std::to_string(type) + "_lineno";
		std::ifstream string_file(string_file_path);
		std::ifstream lineno_file(lineno_file_path);

		// we are just going to see how many strings can fit under
		// BLOCK_BYTE_LIMIT bytes compressed. The posting list will be tiny
		// compressed.
		std::string buffer = "";
		std::vector<std::vector<plist_size_t>> lineno_buffer = {};

		std::string str_line;
		std::string lineno_line;
		size_t uncompressed_lines_in_block = 0;
		size_t blocks_written = 0;
		size_t lines_in_buffer = 0;

		while (std::getline(string_file, str_line)) {

			buffer += str_line + "\n";
			lines_in_buffer += 1;

			std::getline(lineno_file, lineno_line);

			std::istringstream iss(lineno_line);
			std::vector<plist_size_t> numbers;
			plist_size_t number;

			while (iss >> number) {
				numbers.push_back(number);
			}
			lineno_buffer.push_back(numbers);

			if (uncompressed_lines_in_block == 0 &&
				buffer.size() > BLOCK_BYTE_LIMIT / 2) {
				// compress the buffer
				std::string compressed_buffer =
					compressor.compress(buffer.c_str(), buffer.size());
				// figure out how many lines you need such that the compressed
				// thing will have size BLOCK_BYTE_LIMIT
				uncompressed_lines_in_block =
					((float)BLOCK_BYTE_LIMIT /
					 (float)compressed_buffer.size()) *
					lines_in_buffer;
			}

			if (uncompressed_lines_in_block > 0 &&
				lines_in_buffer == uncompressed_lines_in_block) {
				// we have a block
				// compress the buffer
				std::string compressed_buffer =
					compressor.compress(buffer.c_str(), buffer.size());
				PListChunk plist(std::move(lineno_buffer));
				std::string serialized3 = plist.serialize();
				// reset the buffer
				buffer = "";
				lines_in_buffer = 0;
				lineno_buffer = {};
				size_t compressed_buffer_size = compressed_buffer.size();
				fwrite(&compressed_buffer_size, sizeof(size_t), 1, fp);
				fwrite(compressed_buffer.c_str(), sizeof(char),
					   compressed_buffer.size(), fp);
				fwrite(serialized3.c_str(), sizeof(char), serialized3.size(),
					   fp);

				byte_offsets.push_back(byte_offsets.back() +
									   compressed_buffer.size() +
									   serialized3.size() + sizeof(size_t));
				blocks_written += 1;
			}
		}

		std::string compressed_buffer =
			compressor.compress(buffer.c_str(), buffer.size());
		PListChunk plist(std::move(lineno_buffer));
		std::string serialized3 = plist.serialize();
		// reset the buffer
		buffer = "";
		lineno_buffer = {};
		size_t compressed_buffer_size = compressed_buffer.size();
		fwrite(&compressed_buffer_size, sizeof(size_t), 1, fp);
		fwrite(compressed_buffer.c_str(), sizeof(char),
			   compressed_buffer.size(), fp);
		fwrite(serialized3.c_str(), sizeof(char), serialized3.size(), fp);
		blocks_written += 1;

		LOG(INFO) << "type: " << type << " blocks written: " << blocks_written
				  << "\n";

		byte_offsets.push_back(byte_offsets.back() + compressed_buffer.size() +
							   serialized3.size() + sizeof(size_t));

		type_offsets.push_back(byte_offsets.size() - 1);

        if (blocks_written >= BRUTE_THRESHOLD)
        {
            type_uncompressed_lines_in_block[type] = uncompressed_lines_in_block;
        }   
		LOG(INFO) << "type: " << type << " uncompressed lines in block: "
				  << uncompressed_lines_in_block << "\n";
	}


	size_t num_types = types.size();
	size_t num_blocks = byte_offsets.size() - 1;
    OahuMetadataPage oahu_metadata_page(
        num_types, num_blocks, types, type_offsets , byte_offsets);
    std::string compressed_metadata_page = oahu_metadata_page.compress();
    size_t compressed_metadata_page_size = compressed_metadata_page.size();

	// write the compressed metadata page

	fwrite(compressed_metadata_page.c_str(), sizeof(char),
		   compressed_metadata_page.size(), fp);
	fwrite(&compressed_metadata_page_size, sizeof(size_t), 1, fp);

	fclose(fp);

	return type_uncompressed_lines_in_block;
}

/*
The layout for this file:
type_A_fm_index | type_A_logidx | type_B_fm_index | ... | compressed metadata page | 8 bytes = size of metadata page
metadata page format refer to metadata.h for HawaiiMetadataPage

We will create a single FM index for each type. We did away with the notion of groups
because experiments indicate that the compression size is pretty insensitive to the 
dynamic range of the logidx. This is a lot simpler and will be faster for querying.
*/

void write_hawaii(std::string filename, 
    std::map<int, std::string> type_input_files,
    std::map<int, size_t> type_uncompressed_lines_in_block) {

	std::vector<size_t> byte_offsets = {0};
	std::vector<int> type_order = {};

	FILE *fp = fopen((filename + ".hawaii").c_str(), "wb");

	for (auto item : type_uncompressed_lines_in_block) {

        int type = item.first;

        type_order.push_back(type);
		size_t uncompressed_lines_in_block = item.second;
		std::ifstream string_file(type_input_files.at(type));
		std::string buffer = "\n";
		std::string str_line;
		while (std::getline(string_file, str_line)) {
			buffer += str_line + "\n";
		}

		auto [fm_index, log_idx, C] =
			bwt_and_build_fm_index(buffer.data(), uncompressed_lines_in_block);
		
        write_fm_index_to_disk(fm_index, C, buffer.size(), fp);
        byte_offsets.push_back(ftell(fp));
		write_log_idx_to_disk(log_idx, fp);
        byte_offsets.push_back(ftell(fp));

	}

	size_t num_types = type_order.size();
    
	HawaiiMetadataPage hawaii_metadata_page(
		num_types, type_order, byte_offsets);
	std::string compressed_metadata_page = hawaii_metadata_page.compress();
	size_t compressed_metadata_page_size = compressed_metadata_page.size();

	fwrite(compressed_metadata_page.c_str(), sizeof(char),
		   compressed_metadata_page.size(), fp);
	fwrite(&compressed_metadata_page_size, sizeof(size_t), 1, fp);

	fclose(fp);
}

std::map<int, std::set<size_t>> search_hawaii(VirtualFileRegion *vfr,
											  std::vector<int> types,
											  std::string query,
                                              bool early_exit) {
	// read in the metadata page size and then the metadata page and then
	// decompress everything

	vfr->vfseek(-sizeof(size_t), SEEK_END);
	size_t metadata_page_length;
	vfr->vfread(&metadata_page_length, sizeof(size_t));

	// read the metadata page
	vfr->vfseek(-sizeof(size_t) - metadata_page_length, SEEK_END);
	std::string metadata_page;
	metadata_page.resize(metadata_page_length);
	vfr->vfread(&metadata_page[0], metadata_page_length);

	HawaiiMetadataPage hawaii_metadata_page(metadata_page);
	std::vector<int>& type_order =
		hawaii_metadata_page.type_order;
	std::vector<size_t>& byte_offsets = hawaii_metadata_page.byte_offsets;

	std::map<int, std::set<size_t>> type_chunks = {};

#pragma omp parallel for
	for (int type : types) {

		VirtualFileRegion *local_vfr = vfr->slice(0, vfr->size());

		size_t type_index = std::distance(type_order.begin(), 
            std::find(type_order.begin(), type_order.end(), type));
        
		// if not found return empty, FM index was not used for this type
		if (type_index == hawaii_metadata_page.num_types) {
#pragma omp critical
            {
			    type_chunks[type] = {(size_t)-1};
            }
			continue;
		}

		LOG(INFO) << "searching FM index " << type << "\n";

        size_t fm_index_offset = byte_offsets[type_index * 2];
        size_t logidx_offset = byte_offsets[type_index * 2 + 1];
        size_t fm_index_size = logidx_offset - fm_index_offset;
        size_t logidx_size = byte_offsets[type_index * 2 + 2] - logidx_offset;

        // note that each threads operates on a slice, which is a copy with
        // own cursor.
        VirtualFileRegion *fm_index_vfr =
            local_vfr->slice(fm_index_offset, fm_index_size);
        VirtualFileRegion *logidx_vfr =
            local_vfr->slice(logidx_offset, logidx_size);

        auto matched_pos = search_vfr(fm_index_vfr, logidx_vfr, query, early_exit);
        std::set<size_t> chunks(matched_pos.begin(), matched_pos.end());

#pragma omp critical
        {
            type_chunks[type] = std::move(chunks);
        }
	}

	return type_chunks;
}

std::set<size_t> search_hawaii_oahu(VirtualFileRegion *vfr_hawaii,
									VirtualFileRegion *vfr_oahu,
									std::string query, size_t limit) {

	// if split_index_prefix starts with s3://

	std::string processed_query = "";
	for (int i = 0; i < query.size(); i++) {
		if (query[i] != '\n') {
			processed_query += query[i];
		}
	}

	LOG(INFO) << "query: " << processed_query << "\n";

	int query_type = get_type(processed_query.c_str());
	LOG(INFO) << "deduced type: " << query_type << "\n";
	std::vector<int> types_to_search = get_all_types(query_type);

	// print out types to search
	for (int type : types_to_search) {
		LOG(INFO) << "type to search: " << type << "\n";
	}

	std::set<size_t> results;

	std::map<int, std::set<size_t>> result =
		search_hawaii(vfr_hawaii, types_to_search, query);

	// you cannot parallelize this loop here because vfr_hawaii is going to be
	// shared across threads and will have conflicts on the cursor_!

	// print out this result
	for (auto &item : result) {

		int type = item.first;
		std::set<size_t> chunks = item.second;

		LOG(INFO) << "searching type " << type << "\n";
		for (size_t chunk : chunks) {
			LOG(INFO) << "chunk " << chunk << "\n";
		}
	}

	for (auto &item : result) {

		int type = item.first;
		std::set<size_t> chunks = item.second;

		LOG(INFO) << "searching type " << type << "\n";
		std::vector<plist_size_t> found;

		if (chunks == std::set<size_t>{(size_t)-1}) {
			LOG(INFO) << "type not found, brute forcing Oahu\n";
			std::vector<size_t> chunks_to_search(BRUTE_THRESHOLD);
			std::iota(chunks_to_search.begin(), chunks_to_search.end(), 0);
			found = search_oahu(vfr_oahu, type, chunks_to_search, query);
		} else {
			// only search up to limit chunks
			std::vector<size_t> result_vec(chunks.begin(), chunks.end());
			std::vector<size_t> chunks_to_search(
				result_vec.begin(),
				result_vec.begin() + std::min(result_vec.size(), limit));
			found = search_oahu(vfr_oahu, type, chunks_to_search, query);
		}

		for (plist_size_t r : found) {
			results.insert(r);
		}
	}

	return results;
}

std::vector<size_t> search_all(std::string split_index_prefix,
							   std::string query, size_t limit) {

	/*
	Expects a split_index_prefix of the form
	s3://bucket/index-name/indices/split_id or path/index-name/indices/split_id
	*/

	VirtualFileRegion *vfr_hawaii;
	VirtualFileRegion *vfr_oahu;
	VirtualFileRegion *vfr_kauai;

	Aws::SDKOptions options;
	Aws::InitAPI(options);

	// print out the split_index_prefix
	LOG(INFO) << "split_index_prefix: " << split_index_prefix << "\n";

	Aws::Client::ClientConfiguration clientConfig;
	clientConfig.region = "us-west-2";
	clientConfig.connectTimeoutMs = 10000; // 10 seconds
	clientConfig.requestTimeoutMs = 10000; // 10 seconds
	Aws::S3::S3Client s3_client = Aws::S3::S3Client(clientConfig);

	if (split_index_prefix.find("s3://") != std::string::npos) {

		split_index_prefix = split_index_prefix.substr(5);
		std::string bucket =
			split_index_prefix.substr(0, split_index_prefix.find("/"));
		std::string prefix =
			split_index_prefix.substr(split_index_prefix.find("/") + 1);

		// print out the bucket and prefix
		LOG(INFO) << "bucket: " << bucket << "\n";
		LOG(INFO) << "prefix: " << prefix << "\n";

		vfr_hawaii = new S3VirtualFileRegion(s3_client, bucket,
											 prefix + ".hawaii", "us-west-2");
		vfr_oahu = new S3VirtualFileRegion(s3_client, bucket, prefix + ".oahu",
										   "us-west-2");
		vfr_kauai = new S3VirtualFileRegion(s3_client, bucket,
											prefix + ".kauai", "us-west-2");

	} else {
		vfr_hawaii = new DiskVirtualFileRegion(split_index_prefix + ".hawaii");
		vfr_oahu = new DiskVirtualFileRegion(split_index_prefix + ".oahu");
		vfr_kauai = new DiskVirtualFileRegion(split_index_prefix + ".kauai");
	}

	std::pair<int, std::vector<plist_size_t>> result =
		search_kauai(vfr_kauai, query, 1, limit);

	std::vector<size_t> return_results = {};

	if (result.first == 0) {
		// you have to brute force

		return_results.push_back(-1);

	} else if (result.first == 1) {
		// have to convert plist_size_t to size_t
		return_results.insert(return_results.end(), result.second.begin(),
							  result.second.end());

	} else if (result.first == 2) {

		std::vector<size_t> current_results(result.second.begin(),
											result.second.end());
		std::set<size_t> next_results =
			search_hawaii_oahu(vfr_hawaii, vfr_oahu, query, limit);
		return_results = current_results;
		return_results.insert(return_results.end(), next_results.begin(),
							  next_results.end());

	} else {
		assert(false);
	}

	Aws::ShutdownAPI(options);
	return return_results;

	free(vfr_hawaii);
	free(vfr_oahu);
	free(vfr_kauai);
}

extern "C" {
// expects index_prefix in the format bucket/index_name/split_id/index_name
Vector search_python(const char *split_index_prefix, const char *query,
					 size_t limit) {

	google::InitGoogleLogging("rottnest");

	std::vector<size_t> results = search_all(split_index_prefix, query, limit);
	Vector v = pack_vector(results);

	google::ShutdownGoogleLogging();

	return v;
}

void index_python(const char *index_name, size_t num_groups) {

	google::InitGoogleLogging("rottnest");

	std::string index_name_str(index_name);
	compact(num_groups);
	write_kauai(index_name_str, num_groups);
	auto type_uncompressed_lines_in_block = write_oahu(index_name_str);

    std::map<int, std::string> type_input_files = {};
    for (auto type : type_uncompressed_lines_in_block)
    {
        type_input_files[type.first] = "compressed/compacted_type_" + std::to_string(type.first);
    }

	write_hawaii(index_name_str, type_input_files, type_uncompressed_lines_in_block);

	google::ShutdownGoogleLogging();
}
}