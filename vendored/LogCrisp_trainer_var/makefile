CC = g++ -fPIC
AR = ar
ARFLAGS = rcs
EXEC = Trainer
SRCS = main.cpp util.cpp LengthParser.cpp template.cpp Group.cpp timer.cpp
OBJS = $(SRCS:.cpp=.o)
LIBS = -llzma
LIBPATH = -L ./LZMA

all: $(EXEC).a

$(EXEC).a: $(OBJS)
	$(AR) $(ARFLAGS) $@ $^

start: $(EXEC).a
	$(CC) -O2 -o $(EXEC) $(EXEC).a $(LIBPATH) $(LIBS)
	rm -rf $(OBJS)

.cpp.o:
	$(CC) -std=c++11 -g -o $@ -c $^

clean:
	rm -rf $(OBJS) $(EXEC) $(EXEC).a
