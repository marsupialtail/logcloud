# cc = g++
# EXEC = LogStore
# SRCS = main.cpp sample.cpp
# OBJS = $(SRCS:.cpp=.o)

# .cpp.o:
# 	$(cc) -std=c++11 -o $@ -c $<

# start:$(OBJS)
# 	$(cc) -o $(EXEC) $(OBJS)

# clean:
# 	rm -rf $(OBJS) $(EXEC)
LIBDIR = ../zstd-dev/lib
CPPFLAGS += -I$(LIBDIR)
LIB = $(LIBDIR)/libzstd.a
CC = g++ -fPIC
AR = ar
ARFLAGS = rcs
EXEC = Compressor
SRCS = main.cpp RuntimePattern.cpp StaticPattern.cpp util.cpp Coffer.cpp timer.cpp
OBJS = $(SRCS:.cpp=.o)
LIBS = -llzma -lprofiler
LIBPATH = -L ../gperftools-gperftools-2.7/libs

all: $(EXEC).a

$(EXEC).a: $(OBJS)
	$(AR) $(ARFLAGS) $@ $^

start: $(EXEC).a
	$(CC) -o $(EXEC) $(EXEC).a $(LIB) $(LIBPATH) $(LIBS)
	rm -rf $(OBJS)

.cpp.o:
	$(CC) -c -O2 -I$(LIBDIR) -std=c++11 -g -Wall -o $@ $<

clean:
	rm -rf $(OBJS) $(EXEC) $(EXEC).a
