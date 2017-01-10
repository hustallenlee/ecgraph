#include <iostream>
#include "format_convert.h"

//#define COMPACT_GRAPH
int main(int argc, char *argv[]){
	if (argc != 2){
		std::cout<< "wrong number arguments"<< std::endl;
		std::cout<< "example: format_convert snap__file";
	}
	//#ifdef COMPACT_GRAPH
	//std::cout<< "defined\n";
	//#else
	//std::cout<< "not defined\n";
	//#endif
	std::string s1(argv[1]);
	//std::string s2(argv[2]);
	format::format_convert fc(s1);
	fc.to_type();
	return 0;
}
