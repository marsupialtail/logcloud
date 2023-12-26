#pragma once
#include "s3.h"
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>

class VirtualFileRegion {
  public:
	static std::atomic<int> num_reads;
	static std::atomic<int> num_bytes_read;

	virtual ~VirtualFileRegion() = default;

	virtual long size() const = 0;
	virtual int vfseek(long offset, int origin) = 0;
	virtual long vftell() = 0;
	virtual void vfread(void *buffer, long size) = 0;
	virtual void reset() = 0;
	virtual VirtualFileRegion *slice(long start, long length) = 0;
};

class DiskVirtualFileRegion : public VirtualFileRegion {
  private:
	std::string filename_;
	FILE *file_;
	long start_;
	long end_;
	long size_;

  public:
	DiskVirtualFileRegion(std::string filename, long start, long length);
	DiskVirtualFileRegion(std::string filename);
	~DiskVirtualFileRegion();
	long size() const override;
	int vfseek(long offset, int origin) override;
	long vftell() override;
	void vfread(void *buffer, long size) override;
	void reset() override;
	VirtualFileRegion *slice(long start, long length) override;
};

class S3VirtualFileRegion : public VirtualFileRegion {
  private:
	long start_;
	long end_;
	long size_;
	std::string bucket_name_;
	std::string object_name_;
	std::string region_;
	long cursor_;

	const Aws::S3::S3Client &s3_client_;
	Aws::S3::Model::GetObjectRequest object_request_;

	Aws::S3::S3Client CreateS3Client(const std::string &region);

  public:
	S3VirtualFileRegion(const Aws::S3::S3Client &s3_client,
						std::string bucket_name, std::string object_name,
						std::string region);
	S3VirtualFileRegion(const Aws::S3::S3Client &s3_client,
						std::string bucket_name, std::string object_name,
						std::string region, long start, long length);
	~S3VirtualFileRegion();
	long size() const override;
	long object_size();
	int vfseek(long offset, int origin) override;
	long vftell() override;
	void vfread(void *buffer, long size) override;
	void reset() override;
	VirtualFileRegion *slice(long start, long length) override;
};
