# Compiler
CXX = g++

# Compiler flags
CXXFLAGS = -fopenmp -O3 -g -I /home/ziheng/aws-sdk-cpp/build/include/ -fPIC

# Linker flags
LDFLAGS = -L /home/ziheng/aws-sdk-cpp/build/lib/ -l:libdivsufsort.so.3.0.1 -laws-cpp-sdk-s3 -laws-cpp-sdk-core -llz4 -lsnappy -lzstd

# Source files
SRCS = index.cc ../vfr.cc kauai.cc plist.cc

# Object files
OBJS = $(SRCS:.cc=.o)

# Target executable
EXEC = index

# Target shared object
LIB = libindex.so

all: $(EXEC) $(LIB)

$(EXEC): $(OBJS)
	$(CXX) $(CXXFLAGS) $(OBJS) -o $(EXEC) $(LDFLAGS)

$(LIB): $(OBJS)
	$(CXX) -shared $(OBJS) -o $(LIB) $(LDFLAGS)

%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(EXEC) $(LIB)

.PHONY: all clean

