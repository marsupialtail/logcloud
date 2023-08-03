#pragma once
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>

std::vector<char> read_byte_range_from_S3(const Aws::S3::S3Client& s3_client, Aws::S3::Model::GetObjectRequest & object_request, size_t start, size_t end)
{
    object_request.SetRange("bytes=" + std::to_string(start) + "-" + std::to_string(end));
    auto get_object_outcome = s3_client.GetObject(object_request);
    assert(get_object_outcome.IsSuccess());
    auto &retrieved_data = get_object_outcome.GetResultWithOwnership().GetBody();
    // Create a buffer to hold the data.
    std::vector<char> buffer(std::istreambuf_iterator<char>(retrieved_data), {});
    return buffer;
}