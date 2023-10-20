#include <iostream>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>

class S3Downloader {
private:
    using Request = std::tuple<std::string, std::string, std::promise<std::vector<uint8_t>>>;

    std::queue<Request> requests;
    std::mutex mtx;
    std::condition_variable cond;
    bool stop = false;

    void workerThread() {
        while (true) {
            Request request;
            {
                std::unique_lock<std::mutex> lock(mtx);
                cond.wait(lock, [this]() { return !requests.empty() || stop; });

                if (stop && requests.empty()) {
                    return;
                }

                request = requests.front();
                requests.pop();
            }

            auto bytes = downloadFromS3(std::get<0>(request), std::get<1>(request));
            std::get<2>(request).set_value(bytes);
        }
    }

    std::vector<uint8_t> downloadFromS3(const std::string &bucket, const std::string &key) {
        Aws::S3::S3Client s3_client;
        Aws::S3::Model::GetObjectRequest object_request;
        object_request.SetBucket(bucket.c_str());
        object_request.SetKey(key.c_str());

        auto get_object_outcome = s3_client.GetObject(object_request);

        std::vector<uint8_t> data;

        if (get_object_outcome.IsSuccess()) {
            auto &body = get_object_outcome.GetResultWithOwnership().GetBody();
            std::istreambuf_iterator<char> begin(body), end;
            data.assign(begin, end);
        } else {
            std::cerr << "Error downloading " << key << " from " << bucket << ": "
                      << get_object_outcome.GetError().GetMessage() << std::endl;
        }

        return data;
    }

public:
    S3Downloader() {
        for (int i = 0; i < std::thread::hardware_concurrency(); ++i) {
            std::thread(&S3Downloader::workerThread, this).detach();
        }
    }

    ~S3Downloader() {
        {
            std::unique_lock<std::mutex> lock(mtx);
            stop = true;
        }
        cond.notify_all();
    }

    std::future<std::vector<uint8_t>> requestDownload(const std::string &bucket, const std::string &key) {
        std::promise<std::vector<uint8_t>> promise;
        std::future<std::vector<uint8_t>> future = promise.get_future();

        {
            std::unique_lock<std::mutex> lock(mtx);
            requests.push({bucket, key, std::move(promise)});
        }
        cond.notify_one();

        return future;
    }
};

int main() {
    Aws::SDKOptions options;
    Aws::InitAPI(options);

    S3Downloader downloader;
    auto future = downloader.requestDownload("your_bucket_name", "your_file_key");
    std::vector<uint8_t> data = future.get();

    std::cout << "Downloaded data size: " << data.size() << std::endl;

    Aws::ShutdownAPI(options);
    return 0;
}
