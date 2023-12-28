#include "fm_index.h"

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
	std::string compressed_chunk = compressor.compress(
		std::get<1>(chunk).c_str(), std::get<1>(chunk).size());

	std::string serialized_chunk;
	serialized_chunk.resize(sizeof(size_t) + compressed_map_size +
							compressed_chunk.size());
	memcpy((void *)serialized_chunk.data(), &compressed_map_size,
		   sizeof(size_t));
	memcpy((void *)(serialized_chunk.data() + sizeof(size_t)),
		   compressed_map.data(), compressed_map_size);
	memcpy((void *)(serialized_chunk.data() + sizeof(size_t) +
					compressed_map_size),
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
				   The layout of the file will be a list of compressed chunks of
	   variable lengths. This will be followed by a metadata page The metadata
	   page will contain the following information:
				   - Chunk offsets: the byte offsets of each chunk
				   - C vector: the C vector for the FM index
				   - Last eight bytes of the file will be how long the metadata
	   page is.
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
	std::string decompressed_offsets =
		compressor.decompress(compressed_offsets);
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
search_fm_index(VirtualFileRegion *vfr, const char *P, size_t Psize, bool early_exit) {

	auto [n, C, offsets] = read_metadata_from_file(vfr);
	size_t start = 0;
	size_t end = n + 1;

	size_t previous_range = -1;
	// use the FM index to search for the probe
	for (int i = Psize - 1; i >= 0; i--) {
		char c = P[i];
		LOG(INFO) << "c: " << c << std::endl;

		size_t start_byte = offsets[start / FM_IDX_CHUNK_CHARS];
		size_t end_byte = offsets[start / FM_IDX_CHUNK_CHARS + 1];
		vfr->vfseek(start_byte, SEEK_SET);
		std::string serialized_chunk;
		serialized_chunk.resize(end_byte - start_byte);
		vfr->vfread((void *)serialized_chunk.data(), serialized_chunk.size());
		auto chunk = deserializeChunk(serialized_chunk);

		start = C[c] + searchChunk(chunk, c, start % FM_IDX_CHUNK_CHARS);

		start_byte = offsets[end / FM_IDX_CHUNK_CHARS];
		end_byte = offsets[end / FM_IDX_CHUNK_CHARS + 1];
		vfr->vfseek(start_byte, SEEK_SET);
		serialized_chunk.clear();
		serialized_chunk.resize(end_byte - start_byte);
		vfr->vfread((void *)serialized_chunk.data(), serialized_chunk.size());
		chunk = deserializeChunk(serialized_chunk);

		end = C[c] + searchChunk(chunk, c, end % FM_IDX_CHUNK_CHARS);

		LOG(INFO) << "start: " << start << std::endl;
		LOG(INFO) << "end: " << end << std::endl;
		LOG(INFO) << "range: " << end - start << std::endl;
		if (start >= end) {
			LOG(INFO) << "not found" << std::endl;
			return std::make_tuple(-1, -1);
		}
		if (early_exit && (end - start == previous_range)) {
			LOG(INFO) << "early exit" << std::endl;
			return std::make_tuple(start, end);
		}
		previous_range = end - start;
	}

	return std::make_tuple(start, end);
}

fm_index_t construct_fm_index(const char *P, size_t Psize) {

	fm_index_t to_hit = {};
	std::map<char, size_t> current_chunk_char_counts = {};
	std::map<char, size_t> next_chunk_char_counts = {};
	std::string curr_chunk = "";
	for (int idx = 0; idx < Psize; idx++) {
		char c = P[idx];
		next_chunk_char_counts[c]++;
		curr_chunk += c;
		if (curr_chunk.size() == FM_IDX_CHUNK_CHARS) {
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

std::vector<size_t> search_vfr(VirtualFileRegion *wavelet_vfr,
							   VirtualFileRegion *log_idx_vfr,
							   std::string query,
                               bool early_exit) {
	size_t log_idx_size = log_idx_vfr->size();
	size_t compressed_offsets_byte_offset;

	log_idx_vfr->vfseek(-sizeof(size_t), SEEK_END);
	auto start_time = std::chrono::high_resolution_clock::now();
	log_idx_vfr->vfread(&compressed_offsets_byte_offset, sizeof(size_t));
	auto stop = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
		stop - start_time);

	log_idx_vfr->vfseek(compressed_offsets_byte_offset, SEEK_SET);
	Compressor compressor(CompressionAlgorithm::ZSTD);
	std::string compressed_offsets;
	compressed_offsets.resize(log_idx_size - compressed_offsets_byte_offset -
							  8);
	log_idx_vfr->vfread((void *)compressed_offsets.data(),
						compressed_offsets.size());
	std::string decompressed_offsets =
		compressor.decompress(compressed_offsets);
	std::vector<size_t> chunk_offsets(decompressed_offsets.size() /
									  sizeof(size_t));
	memcpy(chunk_offsets.data(), decompressed_offsets.data(),
		   decompressed_offsets.size());

	auto batch_log_idx_lookup = [&chunk_offsets, log_idx_vfr](size_t start_idx,
															  size_t end_idx) {
		size_t start_chunk_offset = chunk_offsets[start_idx / LOG_IDX_CHUNK_BYTES];
		size_t end_chunk_offset = chunk_offsets[end_idx / LOG_IDX_CHUNK_BYTES + 1];
		size_t total_chunks = end_idx / LOG_IDX_CHUNK_BYTES - start_idx / LOG_IDX_CHUNK_BYTES + 1;

		log_idx_vfr->vfseek(start_chunk_offset, SEEK_SET);
		std::string compressed_chunks;
		compressed_chunks.resize(end_chunk_offset - start_chunk_offset);
		log_idx_vfr->vfread((void *)compressed_chunks.data(),
							compressed_chunks.size());

		// this contains possibly multiple chunks! We need to decode them
		// separately
		std::vector<size_t> results = {};
		for (int i = 0; i < total_chunks; i++) {
			size_t chunk_offset = chunk_offsets[start_idx / LOG_IDX_CHUNK_BYTES + i];
			std::string compressed_chunk = compressed_chunks.substr(
				chunk_offset - start_chunk_offset,
				chunk_offsets[start_idx / LOG_IDX_CHUNK_BYTES + i + 1] - chunk_offset);
			compressed_chunk.resize(chunk_offsets[start_idx / LOG_IDX_CHUNK_BYTES + i + 1] -
									chunk_offset);
			Compressor compressor(CompressionAlgorithm::ZSTD);
			std::string decompressed_chunk =
				compressor.decompress(compressed_chunk);
			std::vector<size_t> log_idx(decompressed_chunk.size() /
										sizeof(size_t));
			memcpy(log_idx.data(), decompressed_chunk.data(),
				   decompressed_chunk.size());
			// if this is the first chunk, skip to start_idx, if last chunk,
			// stop and end_idx, otherwise add the entire chunk to the results
			if (i == 0) {
				if (total_chunks == 1) {
					results.insert(results.end(),
								   log_idx.begin() + start_idx % LOG_IDX_CHUNK_BYTES,
								   log_idx.begin() + end_idx % LOG_IDX_CHUNK_BYTES);
				} else {
					results.insert(results.end(),
								   log_idx.begin() + start_idx % LOG_IDX_CHUNK_BYTES,
								   log_idx.end());
				}
			} else if (i == total_chunks - 1) {
				results.insert(results.end(), log_idx.begin(),
							   log_idx.begin() + end_idx % LOG_IDX_CHUNK_BYTES);
			} else {
				results.insert(results.end(), log_idx.begin(), log_idx.end());
			}
		}

		return results;
	};

	LOG(INFO) << "num reads: " << wavelet_vfr->num_reads << std::endl;
	LOG(INFO) << "num bytes read: " << wavelet_vfr->num_bytes_read << std::endl;

	auto [start, end] =
		search_fm_index(wavelet_vfr, query.c_str(), query.size(), early_exit);

	LOG(INFO) << "num reads: " << wavelet_vfr->num_reads << std::endl;
	LOG(INFO) << "num bytes read: " << wavelet_vfr->num_bytes_read << std::endl;

	std::vector<size_t> matched_pos = {};

	if (start == -1 || end == -1) {
		LOG(INFO) << "no matches" << std::endl;
		return {(size_t)-1};
	}

	if (false) { //(end - start > GIVEUP) {
		LOG(INFO) << "too many matches, giving up" << std::endl;
		return {(size_t)-1};
	} else {
		std::vector<size_t> pos = batch_log_idx_lookup(start, end);
		matched_pos.insert(matched_pos.end(), pos.begin(), pos.end());

		// doesn't actually matter which vfr since these are static variables
		LOG(INFO) << "num reads: " << log_idx_vfr->num_reads << std::endl;
		LOG(INFO) << "num bytes read: " << log_idx_vfr->num_bytes_read
				  << std::endl;
		wavelet_vfr->reset();
		log_idx_vfr->reset();
	}

	// print out start, end and matched_pos
	LOG(INFO) << "start: " << start << std::endl;
	LOG(INFO) << "end: " << end << std::endl;

	return matched_pos;
}

std::tuple<fm_index_t, std::vector<size_t>, std::vector<size_t>>
bwt_and_build_fm_index(char *Text, size_t block_lines) {

	std::vector<size_t> C(ALPHABET, 0);

	int n = strlen(Text);
	LOG(INFO) << "n:" << n << std::endl;
	// allocate
	int *SA = (int *)malloc(n * sizeof(int));

    auto start_time = std::chrono::high_resolution_clock::now();
	divsufsort((unsigned char *)Text, SA, strlen(Text));
    auto stop = std::chrono::high_resolution_clock::now();
	auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
		stop - start_time);
	LOG(INFO) << "divsufsort took " << duration.count()
			  << " milliseconds" << std::endl;

	std::vector<size_t> log_idx(n + 1, 0);

	LOG(INFO) << Text[n - 1] << std::endl;
	// assert(Text[n - 1] == '\n');
	// first make an auxiliary structure that records where each newline
	// character is
	std::vector<size_t> newlines = {};
    size_t counter = 0;

	for (int i = 0; i < n; i++) {
		if (Text[i] == '\n') {
            counter += 1;
            if (counter % block_lines == 0) {
                newlines.push_back(i);
                counter = 0;
            }
		}
	}

	LOG(INFO) << "detected " << newlines.size() << " logs " << std::endl;
    assert(block_lines > 0);
	LOG(INFO) << "block lines " << block_lines << std::endl;
	LOG(INFO) << newlines[newlines.size() - 1] << std::endl;

	std::vector<size_t> total_chars(ALPHABET, 0);

	// the suffix array does not output the last character as the first
	// character so add it here FM_index[Text[n - 1]].push_back(0);
	total_chars[Text[n - 1]]++;

	// get the second to last element of newlines, since the last element is
	// always the length of the file assuming file ends with "\n"
	log_idx[0] = newlines[newlines.size() - 2];

	std::vector<char> last_chars = {Text[n - 1]};

	start_time = std::chrono::high_resolution_clock::now();
	for (int i = 0; i < n; ++i) {
		// printf("%c\n", Text[SA[i] - 1]);
		last_chars.push_back(Text[SA[i] - 1]);
		char c = Text[SA[i] - 1];
		total_chars[c]++;

		auto it = std::lower_bound(newlines.begin(), newlines.end(), SA[i]);
		log_idx[i + 1] = it - newlines.begin();
	}

	stop = std::chrono::high_resolution_clock::now();
	duration = std::chrono::duration_cast<std::chrono::milliseconds>(
		stop - start_time);
	LOG(INFO) << "log_idx binary search took " << duration.count()
			  << " milliseconds" << std::endl;

	for (int i = 0; i < ALPHABET; i++) {
		for (int j = 0; j < i; j++)
			C[i] += total_chars[j];
	}

	fm_index_t fm_index = construct_fm_index(last_chars.data(), n + 1);

	return std::make_tuple(fm_index, log_idx, C);
}

void write_log_idx_to_disk(std::vector<size_t> log_idx, FILE *log_idx_fp) {
	std::vector<size_t> chunk_offsets = {0};
	// iterate over chunks of log_idx_fp with size LOG_IDX_CHUNK_BYTES, compress each of them, and
	// record the offsets
	Compressor compressor(CompressionAlgorithm::ZSTD);

	size_t base_offset = ftell(log_idx_fp);

	for (int i = 0; i < log_idx.size(); i += LOG_IDX_CHUNK_BYTES) {
		std::vector<size_t> chunk(log_idx.begin() + i,
								  log_idx.begin() +
									  std::min(i + LOG_IDX_CHUNK_BYTES, int(log_idx.size())));
		std::string compressed_chunk = compressor.compress(
			(char *)chunk.data(), chunk.size() * sizeof(size_t), 5);
		fwrite(compressed_chunk.data(), 1, compressed_chunk.size(), log_idx_fp);
		chunk_offsets.push_back(chunk_offsets.back() + compressed_chunk.size());
	}
	// now write the compressed_offsets
	size_t compressed_offsets_byte_offset = ftell(log_idx_fp) - base_offset;
	LOG(INFO) << "log_idx compressed array size: "
			  << compressed_offsets_byte_offset << std::endl;
	std::string compressed_offsets = compressor.compress(
		(char *)chunk_offsets.data(), chunk_offsets.size() * sizeof(size_t));
	fwrite(compressed_offsets.data(), 1, compressed_offsets.size(), log_idx_fp);
	LOG(INFO) << "log_idx compressed_offsets size: "
			  << compressed_offsets.size() << std::endl;
	// now write 8 bytes, the byte offset
	fwrite(&compressed_offsets_byte_offset, 1, sizeof(size_t), log_idx_fp);
}
