main:
	g++ -std=c++17 -g -Wall -pthread -o main main.cpp

run:
	./main

del:
	del *.exe datafiles\*.* test.txt