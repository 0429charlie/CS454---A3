all: binder librpc.a

binder: binder.cpp
	g++ -std=c++11 -o binder binder.cpp

librpc.a: rpc.cpp
	g++ -std=c++11 -c rpc.cpp
	ar rcs librpc.a rpc.o
