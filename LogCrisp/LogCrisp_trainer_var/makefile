cc = g++
EXEC = Trainer
SRCS = main.cpp util.cpp LengthParser.cpp template.cpp Group.cpp timer.cpp
OBJS = $(SRCS:.cpp=.o)

LIBS = -llzma
LIBPATH = -L ./LZMA

start:$(OBJS)
	$(cc) -O2 -o $(EXEC) $(OBJS) $(LIBPATH) $(LIBS)
	rm -rf $(OBJS)
linked:$(OBJS)
	ld -r $(OBJS) -o $(EXEC).o
.cpp.o:
	$(cc) -D_GLIBCXX_USE_CXX11_ABI=0 -std=c++11 -g -o $@ -c $^
clean:
	rm -rf $(OBJS) $(EXEC)
