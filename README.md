# fts
Full text index. Not open source.

# Quick start
This is verified to work on a fresh Ubuntu 22 instance (r6id.2xlarge) on AWS.

1. Clone the repo. You will see some example logs in example-logs. Go decompress them. zstd -d * should do it. Then remove the zst files!
2. run install.sh. This will setup necessary dependencies. I will install two packages, AWS C++ SDK and libdivsufsort from scratch, more stable this way.
   This might require you to press Y a couple of times, this should take around five minutes.
3. Now run make.
4. export PYARROW_PATH=$(python3 -c "import pyarrow; print(pyarrow.__file__.replace('__init__.py',''))")
5. Make sure you have removed the zst files from the example-logs. Now run this command:
   LD_LIBRARY_PATH=aws-sdk-cpp/build/lib:libdivsufsort/build/lib/:${PYARROW_PATH}/ python3 rex.py --mode batch --dir example-logs/ --index_interval 1024 --compaction_interval 204800 --index_name example --prefix_bytes 24 --prefix_format "%Y-%m-%d %T"

