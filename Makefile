# Compiler
CXX = g++

# Compiler flags
CXXFLAGS = -fopenmp -O3 -g -fPIC -std=c++17

# Linker flags
LDFLAGS = 

# Libraries to link
LIBS = -ldivsufsort -laws-cpp-sdk-s3 -laws-cpp-sdk-core -llz4 -lsnappy -lzstd -lglog

# Source files
SRCS = src/index.cc src/fm_index.cc src/compactor.cc src/vfr.cc src/kauai.cc src/plist.cc

# Object files
OBJS = $(SRCS:.cc=.o)

# Target executable
EXEC = index

# Target shared object
LIB = libindex.so

all: $(EXEC) $(LIB)

$(EXEC): $(OBJS)
	$(CXX) $(CXXFLAGS) src/cli.cc $(OBJS) -o $(EXEC) $(LDFLAGS) $(LIBS)

$(LIB): $(OBJS)
	$(CXX) $(CXXFLAGS) -shared $(OBJS) -o $(LIB) $(LDFLAGS) $(LIBS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(EXEC) $(LIB)

CXXTESTFLAGS = 

TESTS = plist_test compressor_test

# Rule to make all tests
test: $(TESTS)

# Individual test rules
plist_test: test/plist_test.cc src/plist.cc
	$(CXX) $(CXXFLAGS) $(CXXTESTFLAGS) $^ -I src/ -o $@ -lzstd -llz4 -lsnappy

# fts_test: test/fts_test.cc src/vfr.cc # Add other dependencies
#     $(CXX) $(CXXFLAGS) $(CXXTESTFLAGS) $^ -I src/ -o $@ # Add necessary libraries

# kauai_test: test/kauai_test.cc src/kauai.cc 
#     $(CXX) $(CXXFLAGS) $(CXXTESTFLAGS) $^ -I src/ -o $@ # Add necessary libraries

compressor_test: test/compressor_test.cc 
	$(CXX) $(CXXFLAGS) $(CXXTESTFLAGS) $^ -I src/ -o $@ -lzstd -llz4 -lsnappy

# Clean test executables
clean-tests:
	rm -f $(TESTS)

# Extending the PHONY target
.PHONY: all clean test clean-tests
