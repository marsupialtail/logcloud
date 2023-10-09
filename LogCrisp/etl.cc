#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <map>
#include <set>
#include <unordered_map>
#include <algorithm>
#include <string>
#include <iomanip>
#include <cassert>

const int DICT_NUM_THRESHOLD = 100;
const int DICT_SAMPLE_CHUNKS = 5;
const double DICT_CHUNK_RATIO_THRESHOLD = 0.6;
const int ROW_GROUP_SIZE = 100000;
const int COMPACTION_WINDOW = 1000000;
const int OUTLIER_THRESHOLD = 1000;
typedef std::pair<int, int> variable_t;

std::pair<int, int> variable_str_to_tup(const std::string &variable) {
    std::istringstream iss(variable);
    int a, b;
    char underscore, V;
    iss >> V >> a >> underscore >> V >> b;
    return {a, b};
}

std::vector<std::string> variable_to_paths(size_t total_chunks, const std::pair<int, int> &variable, int chunks) {
    std::vector<std::string> result;
    for (int i = 0; i < total_chunks; ++i) {
        std::string path = "compressed/variable_" + std::to_string(i) + "/E" + std::to_string(variable.first) + "_V" + std::to_string(variable.second);
        std::ifstream file(path);
        if (file) {
            result.push_back(path);
            if (result.size() == chunks) {
                break;
            }
        }
    }
    return result;
}

std::map<int, std::vector<std::string>> sample_variable(size_t total_chunks, const std::pair<int, int> &variable, int chunks = 2) {
    auto paths = variable_to_paths(total_chunks, variable, chunks);
    std::map<int, std::vector<std::string>> lines;
    int counter = 0;

    for (const auto &path : paths) {
        std::ifstream file(path);
        std::string line;
        while (std::getline(file, line)) {
            lines[counter].push_back(line);
        }
        counter++;
    }
    return lines;
}

std::string join(const std::vector<std::string>& vec, const std::string& delim) {
    assert(! vec.empty());
    std::string result;
    for (const auto &str : vec) {
        result += str + delim;
    }
    result.erase(result.size() - delim.size());
    return result;
}

std::pair<std::map<variable_t, int>, std::map<int, std::set<variable_t>>> get_variable_info(int total_chunks) {
    std::map<variable_t, int> variable_to_type;
    std::map<int, std::set<variable_t>> chunk_variables;
    for (int chunk = 0; chunk < total_chunks; ++chunk) {
        std::string variable_tag_file = "compressed/variable_" + std::to_string(chunk) + "_tag.txt";
        std::ifstream file(variable_tag_file);
        std::string line;
        while (std::getline(file, line)) {
            std::istringstream iss(line);
            std::string variable_str;
            int tag;
            iss >> variable_str >> tag;
            auto variable = variable_str_to_tup(variable_str);
            variable_to_type[variable] = tag;
            chunk_variables[chunk].insert(variable);
        }
    }
    return {variable_to_type, chunk_variables};
}

int main(int argc, char *argv[]) {

    std::string filename = argv[1];
    size_t total_chunks = std::stoi(argv[2]);

    auto [variable_to_type, chunk_variables] = get_variable_info(total_chunks);

    for (const auto& [key, val] : variable_to_type) {
        std::cout << "(" << key.first << ", " << key.second << "): " << val << "; ";
    }
    std::cout << std::endl;

    std::set<variable_t> variables;
    for (const auto& kv : variable_to_type) {
        variables.insert(kv.first);
    }
    std::set<std::string> dictionary_items;

    for (const auto &variable : variables) {
        auto lines = sample_variable(total_chunks, variable, DICT_SAMPLE_CHUNKS);
        std::vector<std::unordered_map<std::string, size_t>> counters = {};
        std::set<std::string> items;

        for (const auto& [key, vec] : lines) {
            std::unordered_map<std::string, size_t> counter;
            for (const auto& item : vec) {
                counter[item]++;
            }
            counters.push_back(counter);
            items.insert(vec.begin(), vec.end());
        }

        for (const auto& item : items) {
            int num_chunks = 0;
            int num_times = 0;

            for (auto & counter: counters) {
                if (counter[item] > 0) {
                    num_chunks++;
                    num_times += counter[item];
                }
            }

            // if (num_times > DICT_NUM_THRESHOLD && static_cast<double>(num_chunks) / DICT_SAMPLE_CHUNKS > DICT_CHUNK_RATIO_THRESHOLD) {
            if (static_cast<double>(num_chunks) / DICT_SAMPLE_CHUNKS > DICT_CHUNK_RATIO_THRESHOLD) {
                dictionary_items.insert(item);
            }
        }
    }

    for (const auto& item : dictionary_items) { std::cout << item << " ";}
    std::cout << std::endl;
    std::cout << "DICTIONARY SIZE: " << dictionary_items.size() << std::endl;

    std::map<int, std::vector<variable_t>> eid_to_variables;
    std::set<int> touched_types = {0};

    for (const auto &variable : variables) {
        int eid = variable.first;
        eid_to_variables[eid].push_back(variable);
        touched_types.insert(variable_to_type[variable]);
    }

    for (const auto& [eid, vars] : eid_to_variables) {
        std::cout << "EID: " << eid << " Variables: ";
        for (const auto& var : vars) {
            std::cout << "(" << var.first << ", " << var.second << ") ";
        }
        std::cout << std::endl;
    }

    const int COMPACTION_WINDOW = 1000000;
    const bool DEBUG = false;
    std::map<variable_t, std::ifstream*> variable_files = {};

    size_t current_line_number = 0;

    std::map<int, std::ofstream*> compacted_type_files;
    std::map<int, std::ofstream*> compacted_lineno_files;
    std::ofstream outlier_file("compressed/outlier");
    std::ofstream outlier_lineno_file("compressed/outlier_lineno");
    const int OUTLIER_THRESHOLD = 1000;

    std::map<int, std::vector<std::string>> expanded_items;
    std::map<int, std::vector<size_t>> expanded_lineno;

    std::map<int, std::ofstream*> type_files;
    std::map<int, std::ofstream*> type_lineno_files;
    if (DEBUG) {
        for (const int &t : touched_types) {
            type_files[t] = new std::ofstream("compressed/type_" + std::to_string(t));
            type_lineno_files[t] = new std::ofstream("compressed/type_" + std::to_string(t) + "_lineno");
        }
    }

    std::string all_outliers = "";
    std::vector<size_t> outlier_linenos = {};

    FILE *fp = fopen(("compressed/" + filename + ".maui").c_str(), "wb");

    for (int chunk = 0; chunk < total_chunks; ++chunk) {
        // for (const auto &variable : variables) {
        //     if (chunk_variables[chunk].find(variable) != chunk_variables[chunk].end()) {
        //         std::string filename = "compressed/variable_" + std::to_string(chunk) + "/E" 
        //                                 + std::to_string(variable.first) + "_V" + std::to_string(variable.second);
        //         variable_files[variable] = new std::ifstream(filename);
        //     }
        // }

        for (const auto & variable: chunk_variables[chunk]) {
            std::string filename = "compressed/variable_" + std::to_string(chunk) + "/E" 
                                    + std::to_string(variable.first) + "_V" + std::to_string(variable.second);
            variable_files[variable] = new std::ifstream(filename);
        }

        std::ostringstream oss;
        oss << "compressed/chunk" << std::setw(4) << std::setfill('0') << chunk << ".eid";
        std::string chunk_filename = oss.str();
        std::cout << "processing chunk file: " << chunk_filename << std::endl;

        // oss.str("");
        // oss << "compressed/chunk" << std::setw(4) << std::setfill('0') << chunk << ".outlier";
        // std::string outlier_filename = oss.str();

        std::ifstream eid_file(chunk_filename);

        std::string line;
        std::vector<int> chunk_eids;
        while (std::getline(eid_file, line)) {
            chunk_eids.push_back(std::stoi(line));
        }
        eid_file.close();

        for (int eid : chunk_eids) {

            if (eid < 0) {
                // this is an outlier. outliers will be treated separately.
                current_line_number++;
                continue;
            } 
            else if (eid_to_variables.find(eid) == eid_to_variables.end()) {

                // this template does not have variables, skip it

                current_line_number++;
                continue;
            } 
            else {
                auto this_variables = eid_to_variables[eid];
                std::map<int, std::vector<std::string>> type_vars;

                for (const auto &variable : this_variables) {
                    std::string item;
                    std::getline(*variable_files[variable], item);

                    int t = (dictionary_items.find(item) != dictionary_items.end()) ? 0 : variable_to_type[variable];

                    type_vars[t].push_back(item);
                }

                for (const auto &entry : type_vars) {
                    int t = entry.first;
                    if (expanded_items.find(t) == expanded_items.end()) {
                        expanded_items[t] = {};
                        expanded_lineno[t] = {};
                    }
                    expanded_items[t].insert(expanded_items[t].end(), entry.second.begin(), entry.second.end());
                    expanded_lineno[t].resize(expanded_lineno[t].size() + entry.second.size(), current_line_number);
                }

                if (DEBUG) {
                    for (const auto &entry : type_vars) {
                        int t = entry.first;
                        
                        (*type_files[t]) << join(entry.second, " ") << "\n";
                        (*type_lineno_files[t]) << current_line_number << "\n";
                    }
                }

                current_line_number++;
            }
        }

        for (const int &t : touched_types) {
            if ((expanded_items[t].size() > COMPACTION_WINDOW || chunk == total_chunks - 1) && !expanded_items[t].empty()) {
                // Sort expanded_items and expanded_lineno based on expanded_items
                std::vector<std::pair<std::string, size_t>> paired;
                for (size_t i = 0; i < expanded_items[t].size(); ++i) {
                    paired.emplace_back(expanded_items[t][i], expanded_lineno[t][i]);
                }

                std::sort(paired.begin(), paired.end(), 
                        [](const std::pair<std::string, size_t> &a, const std::pair<std::string, size_t> &b) {
                           return a.first == b.first ? a.second < b.second : a.first < b.first;
                        });

                for (size_t i = 0; i < paired.size(); ++i) {
                    expanded_items[t][i] = paired[i].first;
                    expanded_lineno[t][i] = paired[i].second;
                }

                std::vector<std::string> compacted_items;
                std::vector<std::vector<size_t>> compacted_lineno;
                std::string last_item;

                for (size_t i = 0; i < expanded_items[t].size(); ++i) {
                    if (expanded_items[t][i] != last_item) {
                        compacted_items.push_back(expanded_items[t][i]);
                        compacted_lineno.push_back({expanded_lineno[t][i]});
                        last_item = expanded_items[t][i];
                    } else {
                        compacted_lineno.back().push_back(expanded_lineno[t][i]);
                    }
                }

                // Sort compacted_items and compacted_lineno based on the first element of compacted_lineno
                std::vector<std::pair<std::string, std::vector<size_t>>> compacted_paired;
                for (size_t i = 0; i < compacted_items.size(); ++i) {
                    compacted_paired.emplace_back(compacted_items[i], compacted_lineno[i]);
                }

                std::sort(compacted_paired.begin(), compacted_paired.end(), 
                        [](const std::pair<std::string, std::vector<size_t>> &a, const std::pair<std::string, std::vector<size_t>> &b) {
                            return a.second[0] == b.second[0] ? a.first < b.first : a.second[0] < b.second[0];
                        });

                for (size_t i = 0; i < compacted_paired.size(); ++i) {
                    compacted_items[i] = compacted_paired[i].first;
                    compacted_lineno[i] = compacted_paired[i].second;
                }

                if (compacted_items.size() > OUTLIER_THRESHOLD) {
                    if (compacted_type_files[t] == nullptr) {
                        compacted_type_files[t] = new std::ofstream("compressed/compacted_type_" + std::to_string(t));
                        compacted_lineno_files[t] = new std::ofstream("compressed/compacted_type_" + std::to_string(t) + "_lineno");
                    }
                    for (size_t i = 0; i < compacted_items.size(); ++i) {
                        *compacted_type_files[t] << compacted_items[i] << "\n";
                        for (int num : compacted_lineno[i]) {
                            *compacted_lineno_files[t] << num << " ";
                        }
                        *compacted_lineno_files[t] << "\n";
                    }
                } else {
                    for (size_t i = 0; i < compacted_items.size(); ++i) {
                        outlier_file << compacted_items[i] << "\n";
                        for (int num : compacted_lineno[i]) {
                            outlier_lineno_file << num << " ";
                        }
                        outlier_lineno_file << "\n";
                    }
                }

                expanded_items[t].clear();
                expanded_lineno[t].clear();
            }
        }


        for (const auto &entry : variable_files) {
            entry.second->close();
        }
    }


    for (const int &t : touched_types) {
        if (compacted_type_files[t] != nullptr) {
            compacted_type_files[t]->close();
            delete compacted_type_files[t];
            compacted_lineno_files[t]->close();
            delete compacted_lineno_files[t];
        }
    }

    outlier_file.close();
    outlier_lineno_file.close();

    if (DEBUG) {
        for (const int &t : touched_types) {
            type_files[t]->close();
            type_lineno_files[t]->close();
        }
    }

    return 0;
}
