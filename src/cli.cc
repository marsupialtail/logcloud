#include "index.h"

int main(int argc, char *argv[]) {

	google::InitGoogleLogging("rottnest");

	std::string mode = argv[1];

	// figure out the number of groups by counting how many folders named 0, 1,
	// ... are present in compressed

	if (mode == "index") {
		std::string index_name = argv[2];
		size_t num_groups = std::stoul(argv[3]);
		compact(num_groups);
		write_kauai(index_name, num_groups);
		auto type_uncompressed_lines_in_block = write_oahu(index_name);

        std::map<int, std::string> type_input_files = {};
        for (auto type : type_uncompressed_lines_in_block)
        {
            type_input_files[type.first] = "compressed/compacted_type_" + std::to_string(type.first);
        }

		write_hawaii(index_name, type_input_files, type_uncompressed_lines_in_block);
	} else if (mode == "search") {
		std::string split_index_prefix = argv[2];
		std::string query = argv[3];
		size_t limit = std::stoul(argv[4]);
		std::vector<size_t> results =
			search_all(split_index_prefix, query, limit);
		LOG(INFO) << "results: \n";
		for (size_t r : results) {
			LOG(INFO) << r << "\n";
		}

	} else {
		std::cout << "Usage: " << argv[0] << " <mode> <optional:query>"
				  << std::endl;
	}

	google::ShutdownGoogleLogging();
	return 0;
}