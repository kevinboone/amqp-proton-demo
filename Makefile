# Makefile for the Qpid Proton examples

# If you're going to use valgrind or gdb with these examples, you'll need
#   -g, and it's usually easier to troubleshoot if you 
#   use -O0 to disable compiler optimizations
CFLAGS=-Wall -g -O0

LIBS=-lqpid-proton-cpp
LDFLAGS=$(LIBS)
SRCS=$(shell find src/ -type f -name "*.cpp")
BINS=$(patsubst src/%,bin/%,$(SRCS:.cpp=))

all: $(BINS) 

bin/%: src/%.cpp
	g++ -o $@ $(CFLAGS) $< $(LDFLAGS) 

clean:
	rm -rf bin/*

