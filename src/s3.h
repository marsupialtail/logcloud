#pragma once
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

inline size_t
get_object_size(const Aws::S3::S3Client &s3_client,
                Aws::S3::Model::GetObjectRequest &object_request) {
  Aws::S3::Model::HeadObjectRequest head_object_request;
  head_object_request.WithBucket(object_request.GetBucket())
      .WithKey(object_request.GetKey());
  auto head_object_outcome = s3_client.HeadObject(head_object_request);
  size_t file_size;
  if (head_object_outcome.IsSuccess()) {
    file_size = head_object_outcome.GetResult().GetContentLength();
  } else {
    std::cout << "Error: " << head_object_outcome.GetError().GetMessage()
              << std::endl;
    exit(1);
  }
  return file_size;
}

inline std::vector<char>
read_byte_range_from_S3(const Aws::S3::S3Client &s3_client,
                        Aws::S3::Model::GetObjectRequest &object_request,
                        size_t start, size_t end) {
  object_request.SetRange("bytes=" + std::to_string(start) + "-" +
                          std::to_string(end));
  auto get_object_outcome = s3_client.GetObject(object_request);
  assert(get_object_outcome.IsSuccess());
  auto &retrieved_data = get_object_outcome.GetResultWithOwnership().GetBody();
  // Create a buffer to hold the data.
  std::vector<char> buffer(std::istreambuf_iterator<char>(retrieved_data), {});
  return buffer;
}
