#include "kauai.h"

int main(int argc, char *argv[]) {

  // first argument will be mode, which is either 'index' or 'search'. The
  // second argument will be the number of chunks to index or the string to
  // search

  if (argc != 4) {
    std::cout << "Usage: " << argv[0]
              << " index <name> <num_groups> or query <name> <query>"
              << std::endl;
    return 1;
  }

  std::string mode = argv[1];

  Aws::SDKOptions options;
  Aws::InitAPI(options);

  Aws::Client::ClientConfiguration clientConfig;
  clientConfig.region = "us-west-2";
  clientConfig.connectTimeoutMs = 10000; // 10 seconds
  clientConfig.requestTimeoutMs = 10000; // 10 seconds
  Aws::S3::S3Client s3_client = Aws::S3::S3Client(clientConfig);

  if (mode == "index") {
    size_t num_groups = std::stoul(argv[3]);
    write_kauai(argv[2], num_groups);
  } else if (mode == "search") {
    std::string query = argv[3];
    // VirtualFileRegion * vfr = new
    // DiskVirtualFileRegion("compressed/hadoop.kauai");
    VirtualFileRegion *vfr =
        new S3VirtualFileRegion(s3_client, "cluster-dump",
                                std::string(argv[2]) + ".kauai", "us-west-2");
    std::pair<int, std::vector<plist_size_t>> result =
        search_kauai(vfr, query, 1, 1000);
    std::cout << "result: " << result.first
              << " length:" << result.second.size() << std::endl;
    for (plist_size_t row_group : result.second) {
      std::cout << row_group << " ";
    }
    std::cout << std::endl;
  } else {
    std::cout << "Usage: " << argv[0] << " <mode> <num_groups>" << std::endl;
    return 1;
  }

  Aws::ShutdownAPI(options);
}