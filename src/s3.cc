#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <cassert>
#include <chrono>
#include <tuple>
#include <vector>

#define N 1

int main(int argc, const char *argv[1]) {
	Aws::SDKOptions options;
	Aws::InitAPI(options);
	const Aws::String bucket_name = "tpch-sf100-csv";
	const Aws::String object_name = "customer.tbl";

	Aws::Client::ClientConfiguration clientConfig;
	clientConfig.region = "us-west-2";
	clientConfig.connectTimeoutMs = 30000; // 30 seconds
	clientConfig.requestTimeoutMs = 30000; // 30 seconds
	Aws::S3::S3Client s3_client(clientConfig);

	Aws::S3::Model::GetObjectRequest object_request;
	object_request.WithBucket(bucket_name).WithKey(object_name);

	// generate a random vector of length 1000 of tuple(start_byte, end_bytes)

	std::vector<std::tuple<int, int>> ranges;
	for (int i = 0; i < N; i++) {
		int start_byte = rand() % 100000000;
		int end_byte = start_byte + atoi(argv[1]);
		ranges.push_back(std::make_tuple(start_byte, end_byte));
	}

	// start the timer
	auto start = std::chrono::high_resolution_clock::now();

	for (int i = 0; i < N; i++) {
		int start_byte = std::get<0>(ranges[i]);
		int end_byte = std::get<1>(ranges[i]);
		object_request.SetRange("bytes=" + std::to_string(start_byte) + "-" +
								std::to_string(end_byte));
		auto get_object_outcome = s3_client.GetObject(object_request);
		assert(get_object_outcome.IsSuccess());
		auto &retrieved_data =
			get_object_outcome.GetResultWithOwnership().GetBody();
		// Create a buffer to hold the data.
		std::stringstream buffer;
		buffer << retrieved_data.rdbuf();

		// Print the downloaded bytes.
		std::cout << "Downloaded data size: " << buffer.str().size()
				  << std::endl;
	}

	// end the timer
	auto end = std::chrono::high_resolution_clock::now();
	auto duration =
		std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
	std::cout << "Time taken to get object: " << duration.count()
			  << "milliseconds" << std::endl;

	// if (get_object_outcome.IsSuccess()) {
	//     std::cout << "Successfully retrieved object within the byte range."
	//     << std::endl;
	// } else {
	//     std::cout << "Error getting object: " <<
	//     get_object_outcome.GetError().GetExceptionName()
	//               << " " << get_object_outcome.GetError().GetMessage() <<
	//               std::endl;
	// }
	Aws::ShutdownAPI(options);
	return 0;
}
