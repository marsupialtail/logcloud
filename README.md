# Rottnest
Unified data lake storage for metrics , logs and traces. 

# Quick start
This is verified to work on a fresh Ubuntu 22 instance (r6id.2xlarge) on AWS.

1. Clone the repo. This is mainly for the example-logs folder. Please unzip everything AND remove all the compressed zst files. The presence of zst files in this folder will mess things up.
2. pip3 install rottnest, **On AWS Linux you will also have to pip3 install typing_extensions**
3. Run this command `rottnest-index --mode batch --dir example-logs/ --index_interval 1024 --compaction_interval 204800 --index_name example --prefix_bytes 24 --prefix_format "%Y-%m-%d %T"`. Point it to where you example-logs are. It should just work
4. It should produce a folder called `example`, which is the index. You can copy it to an S3 bucket of your choosing, e.g. `aws s3 sync example s3://cluster-dump/example`.
5. Once it's done uploaded, you can try to run a search as follows: `rottnest-search --index_path s3://cluster-dump/example --query 44960 --limit 100`
