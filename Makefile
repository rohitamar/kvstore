main:
	g++ -std=c++17 -g -Wall -pthread -o main main.cpp

run:
	./main

del:
	del *.exe newdatafiles\*.* datafiles\*.* test.txt