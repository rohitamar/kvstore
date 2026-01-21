main:
	g++ -std=c++17 -g -Wall -pthread -o main main.cpp

replay:
	g++ -std=c++17 -g -Wall -pthread -o replay replay.cpp

run:
	./main

del:
	del /Q *.exe datafiles\*.* logs\*.*