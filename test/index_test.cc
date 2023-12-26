#include "index.h"

#define CHUNK_SIZE 100000

std::set<size_t> brute_force_search(std::string file_name, std::string keyword)
{
    std::ifstream file(file_name);
    std::string line;
    size_t line_number = 0;
    std::set<size_t> result;
    while (std::getline(file, line))
    {
        if (line.find(keyword) != std::string::npos)
        {
            result.insert(line_number / CHUNK_SIZE);
        }
        line_number++;
    }

    return result;
}

int main()
{
    google::InitGoogleLogging("rottnest");
    
    std::vector<int> types = {1, 53, 63};
    std::map<int, std::string> type_input_files = {};
    std::map<int, size_t> type_uncompressed_lines_in_block = {};
    for (auto type : types)
    {
        type_input_files[type] = "data/compacted_type_" + std::to_string(type);
        type_uncompressed_lines_in_block[type] = CHUNK_SIZE;
    }

	//write_hawaii("test", type_input_files, type_uncompressed_lines_in_block);
    VirtualFileRegion * vfr_hawaii = new DiskVirtualFileRegion("test.hawaii");

    std::vector<std::string> queries = {"openstack", "openstack-119", "1bad-44dc-8505", "10036", "T9xqQRK4yyc"};

    for (auto query : queries)
    {
        std::map<int, std::set<size_t>> result = search_hawaii(vfr_hawaii, types, query, false);

        for (auto type : types)
        {
            std::set<size_t> type_result = brute_force_search(type_input_files[type], query);
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
}