#include "fm_index.h"

int main(int argc, const char *argv[1]) {

  // usage: fts index <infile> <log_idx_file> <fm_file>
  // usage: fts query-disk <infile> <log_idx_file> <fm_file> <pattern>
  // usage: fts query-s3 <bucket> <infile> <log_idx_file> <fm_file> <pattern>

  const char *mode = argv[1];

  if (strcmp(mode, "index") == 0) {

    // assert(argc == 5);

    char *Text;
    // open the file handle with filename in argv[1] into Text
    FILE *fp = fopen(argv[2], "r");
    fseek(fp, 0, SEEK_END);
    size_t size = ftell(fp);
    std::cout << "size:" << size << std::endl;
    fseek(fp, 0, SEEK_SET);

    Text = (char *)malloc(size);
    fread(Text, 1, size, fp);
    fclose(fp);

    // TODO: currently this skips the last \n by force

    // get the log_idx and fm_index
    auto [fm_index, log_idx, C] = bwt_and_build_fm_index(Text);

    FILE *wavelet_fp = fopen(argv[3], "wb");
    write_fm_index_to_disk(fm_index, C, size, wavelet_fp);
    fclose(wavelet_fp);

    FILE *log_idx_fp = fopen(argv[4], "wb");
    write_log_idx_to_disk(log_idx, log_idx_fp);
    fclose(log_idx_fp);

  } else if (strcmp(mode, "query-disk") == 0) {
    FILE *fp = fopen(argv[2], "r");
    fseek(fp, 0, SEEK_END);
    size_t size = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    char *Text = (char *)malloc(size);
    fread(Text, 1, size, fp);
    fclose(fp);

    const char *wavelet_filename = argv[3];
    const char *log_idx_filename = argv[4];
    const char *query = argv[5];

    VirtualFileRegion *wavelet_vfr =
        new DiskVirtualFileRegion(wavelet_filename);
    VirtualFileRegion *log_idx_vfr =
        new DiskVirtualFileRegion(log_idx_filename);
    auto matched_pos = search_vfr(wavelet_vfr, log_idx_vfr, query);

    // print out the matchd pos

    for (size_t pos : matched_pos) {
      std::cout << pos << std::endl;
    }

    for (size_t pos : matched_pos) {
      for (int j = pos + 1; j < pos + 1000; j++) {
        printf("%c", Text[j]);
        if (Text[j] == '\n') {
          break;
        }
      }
      printf("\n");
    }

  }else {
    std::cout << "invalid mode" << std::endl;
  }

  return 0;
}
