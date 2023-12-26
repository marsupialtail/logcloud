
#pragma once
#include "plist.h"
#include "vfr.h"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

std::pair<int, std::vector<plist_size_t>>
search_kauai(VirtualFileRegion *vfr, std::string query, int mode, int k);
int write_kauai(std::string filename, int num_groups);
