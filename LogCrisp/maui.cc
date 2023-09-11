#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <vector>
#include <iomanip>
#include <string>
#include "plist.h"

#define ROW_GROUP_SIZE 100000

/*
The maui file will have three sections:
The first is things that if you match you are dead. These contain the common templates (most of them) and the dictionary items.
Posting lists are not even stored for these items. If a part of your query match this, then that part is useless in filtering. If 
entire query matches, your query is useless since it will have a lot of hits. In the future we might support some kind of time based
count binning for this as well but in the future!

The second is things that if you match you are done. These are the outlier types. This is ideal scenario, you can circumvent matching Oahu
and Hawaii. Posting lists are stored for these items. Currently you always search these, in future you can decide based on types.

The third is things that if you match, you need to record that you matched and still have to search other things. This is the outleir templates
Posting lists are stored for these. You always need to search these. 

*/

int main(int argc, char* argv[]) {

    size_t num_chunks = std::stoul(argv[1]);

    FILE *fp = fopen("compressed/hadoop.maui", "wb");

    std::map<int, std::vector<size_t>> template_posting_lists = {};
    std::vector<std::string> outliers = {};
    std::vector<std::vector<plist_size_t>> outlier_linenos = {};

    plist_size_t lineno = 0;
    for (size_t chunk = 0; chunk < num_chunks; ++chunk) {
        std::ostringstream oss;
        oss << "compressed/chunk" << std::setw(4) << std::setfill('0') << chunk << ".eid";
        std::string chunk_filename = oss.str();
        std::cout << "processing chunk file: " << chunk_filename << std::endl;

        oss.str("");
        oss << "compressed/chunk" << std::setw(4) << std::setfill('0') << chunk << ".outlier";
        std::string outlier_filename = oss.str();

        std::ifstream eid_file(chunk_filename);
        std::ifstream outlier_file(outlier_filename);

        if (!eid_file.is_open()) {
            std::cerr << "Error opening input file: " << chunk_filename << "\n";
            return 1;
        }

        std::string line;
        while (std::getline(eid_file, line)) {
            int eid = std::stoi(line);

            if (eid < 0) {
                std::string item;
                std::getline(outlier_file, item);
                outliers.push_back(item);
                outlier_linenos.push_back({lineno});
            } else {
                if (template_posting_lists.find(eid) == template_posting_lists.end()) {
                    template_posting_lists[eid] = {};
                }
                if (template_posting_lists[eid].size() == 0 || template_posting_lists[eid].back() != lineno / ROW_GROUP_SIZE) {
                    template_posting_lists[eid].push_back(lineno / ROW_GROUP_SIZE);
                }
            }
            lineno ++;
            if (lineno == -1) {
                std::cout << "overflow" << std::endl;
                return 1;
            }
            
        }
    }

    PListChunk plist(std::move(outlier_linenos));
    std::string serialized = plist.serialize();

}