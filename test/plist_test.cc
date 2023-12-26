#include "cassert"
#include "plist.h"
#include <fstream>
#include <iostream>
#include <sstream>

int main(int argc, char *argv[]) {

  std::vector<std::vector<plist_size_t>> data = {
      {1, 2, 3}, {4, 5, 6}, {7, 8, 9}};
  // make a copy of data
  auto data_copy = data;
  PListChunk plist(std::move(data));
  std::string serialized = plist.serialize();
  PListChunk plist2(serialized);
  std::string serialized2 = plist2.serialize();
  assert(serialized == serialized2);

  auto result = plist2.data();
  // check that result is the same as data_copy
  assert(result == data_copy);
  assert(plist2.lookup(1) == data_copy[1]);
  return 0;
}
