.PHONY: all configure build run clean 
RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))

all: build

wtr:
	g++ -std=c++17 -g -Wall -pthread -o wtr ./tests/writes_then_reads.cpp ./database/Rocask.cpp ./database/utils.cpp

war:
	g++ -std=c++17 -g -Wall -pthread -o war ./tests/writes_and_reads.cpp ./database/Rocask.cpp ./database/utils.cpp

build/Cmake:
	cmake -B build 

build: 
	cmake --build build

runapi:
	./build/Debug/api.exe $(RUN_ARGS)

cleandumps:
	if exist dumps\*.* del /Q dumps\*.*

clean:
	if exist *.exe del /Q *.exe 
	if exist datafiles\*.* del /Q datafiles\*.*
	cls