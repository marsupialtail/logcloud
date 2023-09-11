#pragma once
#include <iostream>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <cassert>
#include "s3.h"

class VirtualFileRegion {
public:
    static std::atomic<int> num_reads;
    static std::atomic<int> num_bytes_read;

    virtual ~VirtualFileRegion() = default;

    virtual size_t size() const = 0;
    virtual int vfseek(size_t offset, int origin) = 0;
    virtual size_t vftell() = 0;
    virtual void vfread(void* buffer, size_t size) = 0;
    virtual void reset() = 0;
    virtual VirtualFileRegion* slice(size_t start, size_t length) = 0;
};

class DiskVirtualFileRegion : public VirtualFileRegion {
private:
    const char* filename_;
    FILE* file_;
    size_t start_;
    size_t end_;
    size_t size_;

public:
    DiskVirtualFileRegion(const char* filename, size_t start, size_t length);
    DiskVirtualFileRegion(const char* filename);
    ~DiskVirtualFileRegion();
    size_t size() const override;
    int vfseek(size_t offset, int origin) override;
    size_t vftell() override;
    void vfread(void* buffer, size_t size) override;
    void reset() override;
    VirtualFileRegion* slice(size_t start, size_t length) override;
};

class S3VirtualFileRegion : public VirtualFileRegion {
private:
    size_t start_;
    size_t end_;
    size_t size_;
    std::string bucket_name_;
    std::string object_name_;
    std::string region_;
    size_t cursor_;

    Aws::S3::S3Client s3_client_;
    Aws::S3::Model::GetObjectRequest object_request_;

    Aws::S3::S3Client CreateS3Client(const std::string& region);

public:
    S3VirtualFileRegion(std::string bucket_name, std::string object_name, std::string region);
    S3VirtualFileRegion(std::string bucket_name, std::string object_name, std::string region, size_t start, size_t length);
    ~S3VirtualFileRegion();
    size_t size() const override;
    size_t object_size();
    int vfseek(size_t offset, int origin) override;
    size_t vftell() override;
    void vfread(void* buffer, size_t size) override;
    void reset() override;
    VirtualFileRegion* slice(size_t start, size_t length) override;
};
