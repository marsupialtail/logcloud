#pragma once

#include "cassert"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <numeric>
#include <sstream>

#include "compactor.h"
#include "fm_index.h"
#include "kauai.h"
#include "metadata.h"
#include "plist.h"
#include "python_interface.h"
#include "type_util.h"
#include "vfr.h"
#include <glog/logging.h>

std::vector<plist_size_t> search_oahu(VirtualFileRegion *vfr, int query_type,
									  std::vector<size_t> chunks,
									  std::string query_str);

std::map<int, size_t> write_oahu(std::string output_name);

void write_hawaii(std::string filename, 
    std::map<int, std::string> type_input_files,
    std::map<int, size_t> type_uncompressed_lines_in_block);


std::map<int, std::set<size_t>> search_hawaii(VirtualFileRegion *vfr,
											  std::vector<int> types,
											  std::string query,
                                              bool early_exit = true);

std::set<size_t> search_hawaii_oahu(VirtualFileRegion *vfr_hawaii,
									VirtualFileRegion *vfr_oahu,
									std::string query, size_t limit);

std::vector<size_t> search_all(std::string split_index_prefix,
							   std::string query, size_t limit);