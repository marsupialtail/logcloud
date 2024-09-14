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

LIBDIR =../zstd-dev/lib
CPPFLAGS += -I$(LIBDIR)
LIB = $(LIBDIR)/libzstd.a

cc = g++ -fPIC
EXEC = Compressor
SRCS = main.cpp RuntimePattern.cpp StaticPattern.cpp util.cpp Coffer.cpp timer.cpp
OBJS = $(SRCS:.cpp=.o)

LIBS = -llzma -lprofiler
LIBPATH = -L ../gperftools-gperftools-2.7/libs

start:$(OBJS)
	$(cc) $(OBJS) $(LIB) -o $(EXEC) $(LIBPATH) $(LIBS) 
	rm -rf $(OBJS)

linked:$(OBJS)
	ld -r $(OBJS) -o $(EXEC).o

.cpp.o:
	# $(cc) -D_GLIBCXX_USE_CXX11_ABI=0  -c -O2 -I$(LIBDIR) -std=c++11 -g -Wall -o $@ -c $^ 
	$(cc)   -c -O2 -I$(LIBDIR) -std=c++11 -g -Wall -o $@ -c $^ 
clean:
	rm -rf $(OBJS) $(EXEC)
