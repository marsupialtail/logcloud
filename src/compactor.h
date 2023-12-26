#pragma once
#include "compressor.h"
#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <glog/logging.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

const int ROW_GROUP_SIZE = 100000;
const double DICT_RATIO_THRESHOLD =
	0.5; // if something appears in more than 50% of row groups, then it is a
		 // dictionary

void mergeFiles(const std::vector<std::string> &inputFilenames,
				const std::vector<std::string> &inputFilenamesLinenumbers,
				const std::string &outputFilename,
				const std::string &outputFilenameLinenumbers,
				size_t num_row_groups);

int compact(int num_groups);