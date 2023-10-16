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

LIBDIR =../../zstd-dev/lib
CPPFLAGS += -I$(LIBDIR)
LIB = $(LIBDIR)/libzstd.a

cc = g++
EXEC = DeCompressor
SRCS = main.cpp ../Coffer.cpp 
OBJS = $(SRCS:.cpp=.o)

LIBS = -llzma
LIBPATH = -L ../LZMA

start:$(OBJS)
	$(cc) $(OBJS) $(LIB) -o $(EXEC) $(LIBPATH) $(LIBS)
.cpp.o:
	$(cc) -O0 -c -I$(LIBDIR) -std=c++11 -g -Wall -o $@ -c $^ 
clean:
	rm -rf $(OBJS) $(EXEC)
