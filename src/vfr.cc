#include "vfr.h"

std::atomic<int> VirtualFileRegion::num_reads(0);
std::atomic<int> VirtualFileRegion::num_bytes_read(0);

DiskVirtualFileRegion::DiskVirtualFileRegion(std::string filename, long start, long length)
    : filename_(filename), start_(start) {
    file_ = fopen(filename.c_str(), "rb");
    if (file_) {
        fseek(file_, 0, SEEK_END);
        long fileSize = ftell(file_);
        end_ = (start + length <= fileSize) ? start + length : fileSize;
        size_ = end_ - start_;
        fseek(file_, start_, SEEK_SET);
    } else {
        std::cout << "Error opening file: " << filename << std::endl;
        exit(1);
    }
}

DiskVirtualFileRegion::DiskVirtualFileRegion(std::string filename)
    : filename_(filename), start_(0) {
    file_ = fopen(filename.c_str(), "rb");
    if (file_) {
        fseek(file_, 0, SEEK_END);
        long fileSize = ftell(file_);
        end_ = fileSize;
        size_ = end_ - start_;
        fseek(file_, 0, SEEK_SET);
    } else {
        std::cout << "Error opening file: " << filename << std::endl;
        exit(1);
    }
}

DiskVirtualFileRegion::~DiskVirtualFileRegion() {
    if (file_) {
        fclose(file_);
    }
}

long DiskVirtualFileRegion::size() const {
    return size_;
}

int DiskVirtualFileRegion::vfseek(long offset, int origin) {
    switch (origin) {
        case SEEK_SET:
            return fseek(file_, start_ + offset, SEEK_SET);
        case SEEK_CUR:
            return fseek(file_, offset, SEEK_CUR);
        case SEEK_END:
            return fseek(file_, end_ + offset, SEEK_SET);
        default:
            return -1;
    }
}

long DiskVirtualFileRegion::vftell() {
    long current = ftell(file_);
    return current - start_;
}

void DiskVirtualFileRegion::vfread(void* buffer, long size) {
    if (ftell(file_) + size > end_) {
        std::cout << "fread failed, size: " << size << " current pos: " << ftell(file_) << " end: " << end_ << std::endl;
        assert(false);
    }
    num_reads++;
    num_bytes_read += size;

    if (!fread(buffer, size, 1, file_)) {
        std::cout << "fread failed, size: " << size << " current pos: " << ftell(file_) << " end: " << end_ << std::endl;
        assert(false);
    }
}

VirtualFileRegion* DiskVirtualFileRegion::slice(long start, long length) {
    if (start + length > end_) {
        assert(false);
    }
    return new DiskVirtualFileRegion(filename_, start_ + start, length);
}

void DiskVirtualFileRegion::reset() {
    fseek(file_, start_, SEEK_SET);
}

S3VirtualFileRegion::S3VirtualFileRegion(const Aws::S3::S3Client& s3_client, std::string bucket_name, std::string object_name, std::string region)
    : s3_client_(s3_client), bucket_name_(bucket_name), object_name_(object_name), region_(region), start_(0), cursor_(0) {
    
    object_request_ = Aws::S3::Model::GetObjectRequest();
    object_request_.WithBucket(bucket_name).WithKey(object_name);

    auto file_size = this->object_size();
    end_ = file_size;
    size_ = end_ - start_;
}

S3VirtualFileRegion::S3VirtualFileRegion(const Aws::S3::S3Client& s3_client, std::string bucket_name, std::string object_name, std::string region, long start, long length)
    : s3_client_(s3_client), bucket_name_(bucket_name), object_name_(object_name), region_(region), start_(start), cursor_(start) {
    
    object_request_ = Aws::S3::Model::GetObjectRequest();
    object_request_.WithBucket(bucket_name).WithKey(object_name);

    end_ = start + length;
    size_ = end_ - start_;
}

S3VirtualFileRegion::~S3VirtualFileRegion() {
}

long S3VirtualFileRegion::size() const {
    return size_;
}

long S3VirtualFileRegion::object_size() {
    Aws::S3::Model::HeadObjectRequest head_object_request;
    head_object_request.WithBucket(object_request_.GetBucket()).WithKey(object_request_.GetKey());
    auto head_object_outcome = s3_client_.HeadObject(head_object_request);
    long file_size;
    if (head_object_outcome.IsSuccess()) {
        file_size = head_object_outcome.GetResult().GetContentLength();
    } else {
        std::cout << "Error: " << head_object_outcome.GetError().GetMessage() << std::endl;
        exit(1);
    }
    return file_size;
}

int S3VirtualFileRegion::vfseek(long offset, int origin) {
    switch (origin) {
        case SEEK_SET:
            cursor_ = start_ + offset;
            return 0;
        case SEEK_CUR:
            cursor_ += offset;
            return 0;
        case SEEK_END:
            cursor_ = end_ + offset;
            return 0;
        default:
            return -1;
    }
}

long S3VirtualFileRegion::vftell() {
    return cursor_;
}

void S3VirtualFileRegion::reset() {
    cursor_ = start_;
}

void S3VirtualFileRegion::vfread(void* buffer, long size) {
    
    num_reads++;
    num_bytes_read += size;

    Aws::S3::Model::GetObjectRequest object_request;
    object_request.WithBucket(bucket_name_).WithKey(object_name_);

    if (cursor_ + size > end_) {
        std::cout << "cursor: " << cursor_ << " size: " << size << " end: " << end_ << std::endl;
        assert(false);
    }

    
    // std::cout << object_request_.GetBucket() << " " << object_request_.GetKey() << " " << cursor_ << " " << size << std::endl;
    std::string argument = "bytes=" + std::to_string(cursor_) + "-" + std::to_string(cursor_ + size - 1);
    // std::string argument = "bytes=" + std::to_string(cursor_) + "-" + std::to_string(cursor_ + 1000);
    object_request.SetRange(argument.c_str());
    auto start_time = std::chrono::high_resolution_clock::now();
    auto get_object_outcome = s3_client_.GetObject(object_request);
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop-start_time);
    std::cout << "s3 get took " << duration.count() << " milliseconds" << std::endl;
    assert(get_object_outcome.IsSuccess());
    auto &retrieved_data = get_object_outcome.GetResultWithOwnership().GetBody();
    retrieved_data.read(static_cast<char*>(buffer), size);
}

VirtualFileRegion* S3VirtualFileRegion::slice(long start, long length) {
    if (start + length > end_) {
        assert(false);
    }
    return new S3VirtualFileRegion(s3_client_, bucket_name_, object_name_, region_, start_ + start, length);
}

// Aws::S3::S3Client S3VirtualFileRegion::CreateS3Client(const std::string& region) {
//     Aws::Client::ClientConfiguration clientConfig;
//     clientConfig.region = region;
//     clientConfig.connectTimeoutMs = 1000; // 10 seconds
//     clientConfig.requestTimeoutMs = 1000; // 10 seconds
//     return Aws::S3::S3Client(clientConfig);
// }