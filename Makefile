wtr:
	g++ -std=c++17 -g -Wall -pthread -o wtr ./tests/writes_then_reads.cpp

war:
	g++ -std=c++17 -g -Wall -pthread -o war ./tests/writes_and_reads.cpp

del:
	if exist *.exe del /Q *.exe 
	if exist datafiles\*.* del /Q datafiles\*.* 