#include <sstream>
#include <iostream>
#include <fstream>
#include <map>
#include <utility>
#include "../utils/types.h"
int main(int argc, char *argv[]){
	if (argc !=4){
}
	std::map<format::vertex_t, format::vertex_t> remapper;
	std::ifstream map_file(argv[1]);
	std::ifstream output_file(argv[2]);
	std::ofstream remap_file(argv[3]);
	
}
