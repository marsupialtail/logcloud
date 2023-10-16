
#pragma once
#include <iostream>
#include <fstream>
#include <sstream>
#include <map>
#include <set>
#include <vector>
#include <iomanip>
#include <string>
#include <algorithm>
#include <filesystem>
#include "plist.h"
#include <utility>
#include "vfr.h"

std::pair<int, std::vector<plist_size_t>> search_kauai(VirtualFileRegion * vfr, std::string query, int mode, int k);
int write_kauai(std::string filename, int num_groups);
