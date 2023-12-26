#include <vector>

typedef struct {
  size_t *data;
  size_t size;
} Vector;

// now have a function that packs a std::vector into a Vector
Vector pack_vector(std::vector<size_t> v) {
  Vector result;
  result.size = v.size();
  result.data = (size_t *)malloc(sizeof(size_t) * result.size);
  for (size_t i = 0; i < result.size; ++i) {
    result.data[i] = v[i];
  }
  return result;
}