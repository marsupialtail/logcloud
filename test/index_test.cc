#include "index.h"

const std::vector<std::string> queries = {"system", "openstack", "openstack-1", "1bad-44dc-8505", "10036", "T9xqQRK4yyc"};
const std::vector<size_t> chunk_sizes = {1, 1000};

std::set<size_t> brute_force_search(std::string file_name, std::string keyword, size_t chunk_size)
{
    std::ifstream file(file_name);
    std::string line;
    // line_number starts at 1, because in hawaii we prepend the text with a \n
    size_t line_number = 1;
    std::set<size_t> result;
    while (std::getline(file, line))
    {
        if (line.find(keyword) != std::string::npos)
        {
            result.insert(line_number / chunk_size);
        }
        line_number++;
    }

    return result;
}

int test_hawaii(size_t chunk_size)
{
    
    std::vector<int> types = {1, 53, 63};
    std::map<int, std::string> type_input_files = {};
    std::map<int, size_t> type_uncompressed_lines_in_block = {};
    for (auto type : types)
    {
        type_input_files[type] = "test/data/compacted_type_" + std::to_string(type);
        type_uncompressed_lines_in_block[type] = chunk_size;
    }

	write_hawaii("test", type_input_files, type_uncompressed_lines_in_block);
    VirtualFileRegion * vfr_hawaii = new DiskVirtualFileRegion("test.hawaii");

    for (auto query : queries)
    {
        std::map<int, std::set<size_t>> result = search_hawaii(vfr_hawaii, types, query, false);

        for (auto type : types)
        {
            std::set<size_t> type_result = brute_force_search(type_input_files[type], query, chunk_size);
            if (type_result.size() > 0 )
            {
                assert(result[type] == type_result);
                std::cout << "type: " << type << std::endl;
                std::cout << "result: ";
                for (auto i : result[type])
                {
                    std::cout << i << " ";
                }
                std::cout << std::endl;
                std::cout << "brute force result: ";
                for (auto i : type_result)
                {
                    std::cout << i << " ";
                }
                std::cout << std::endl;
            }
        }
    }

    return 0;
}

int main()
{
    google::InitGoogleLogging("rottnest");
    for (auto chunk_size : chunk_sizes)
    {
        test_hawaii(chunk_size);
    }
}