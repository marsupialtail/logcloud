all:
	g++ -o test -O3 -g fts.cc vfr.cc -L:/home/ziheng/libdivsufsort/build/lib -I /home/ziheng/aws-sdk-cpp/build/include/ -L /home/ziheng/aws-sdk-cpp/build/lib/ -l:libdivsufsort.so.3.0.1 -laws-cpp-sdk-s3 -laws-cpp-sdk-core -llz4 -lsnappy -lzstd
