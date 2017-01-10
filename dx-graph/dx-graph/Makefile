CC= mpic++


USE_BOOST= -L../boost/lib -I../boost/include
BIN_DIR= bin
ALGORITHMS_DIR= algorithms
MACRO1= -DCOMPACT_GRAPH 
#MACRO2= -DWEIGHT_EDGE
MACRO3= -DMY_DEBUG 
MACRO4= -DLOG
MULTI_THREAD= -lpthread

CXXFLAGS= -std=c++11 

ALL_TARGETS= pagerank wcc bfs 

all:
	make $(ALL_TARGETS)

pagerank: $(ALGORITHMS_DIR)/pagerank.cpp
	$(CC) $(ALGORITHMS_DIR)/pagerank.cpp $(CXXFLAGS) $(DEBUG) $(MACRO1) $(MACRO3) $(MACRO4) $(MULTI_THREAD) $(USE_BOOST) \
		-o $(BIN_DIR)/pagerank

wcc: $(ALGORITHMS_DIR)/wcc.cpp
	$(CC) $(ALGORITHMS_DIR)/wcc.cpp $(CXXFLAGS) $(DEBUG) $(MACRO1) $(USE_BOOST) \
		-o $(BIN_DIR)/wcc

bfs: $(ALGORITHMS_DIR)/bfs.cpp
	$(CC) $(ALGORITHMS_DIR)/bfs.cpp $(CXXFLAGS) $(DEBUG) $(MACRO1) $(USE_BOOST) \
		-o $(BIN_DIR)/bfs
.PHONY : clean
clean:
	rm $(BIN_DIR)/*

