#ifndef _OLD_PAGERANK_H_
#define _OLD_PAGERANK_H_
#include "utils/buffer.h"
#include "utils/config.h"
#include "utils/log_wrapper.h"
#include "utils/types.h"
#include "utils/type_utils.h"
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <cmath>
#include <fstream>
#include <iomanip>
//#include <bitset>
//#define COMPACT_GRAPH

typedef struct{
	int degree;
	format::weight_t res[2];
}array;
void output(std::vector<array > &aux_array,int flag);
int main(int argc,char * argv[]){
	if(argc != 3){
		std::cout<<"Usage: execute_file filename iterator_times "
					<< std::endl;
		return 0;
	}
	std::stringstream record;
	std::string filename;
	int times = 0;
	int vertex_num = 0;
	int edge_num = 0;
	int type=1;
	char buf[1000000];
	array init_array;

	record.clear();
	record << argv[1];
	record >> filename;	
	
	record.clear();
	record << argv[2];
	record >> times;

	format::config conf;
	if ( !conf.open_fill(filename + ".ini")){
		LOG_TRIVIAL(fatal)<< "can not open config file "
							<<filename + ".ini";
		return 0;
	}
	record.clear();
	record<< conf["vertices"];
	record>> vertex_num;	

	record.clear();
	record<< conf["type"];
	record>> type;
	
	record.clear();
	record<< conf["edges"];
	record>> edge_num;
	
	format::weight_t init_weight = 0.15 / vertex_num;
	init_array.degree = 0;
	init_array.res[0] = init_weight;
	init_array.res[1] = init_weight;

	std::vector<bool> update_bitset(vertex_num, false);
	std::vector<array > aux_array(vertex_num, init_array); //auxiliary array
	int	edge_size = sizeof(format::edge_t);
	int flag = 1;   //indicate which is the old
	format::edge_t edge;
	int updated_num = 0;

	
	dx_lib::buffer disk_io(1000*1000*50);
	//std::cout<<"here1 "<<times<<std::endl;	

	disk_io.start_write(filename);
	
	int edge_num_once = 100000;      //read 1000 edges once a time
	int byte_num_once = edge_num_once * edge_size;
	int readed_bytes = 0;

	LOG_TRIVIAL(info)<<"initialize ... ";
	while( !disk_io.is_over()){			//the first scan, get every vertice's degree
		
		readed_bytes = disk_io.read(buf, byte_num_once);
	//	std::cout<<"readed "<< readed_bytes<<std::endl;
		
		int i = readed_bytes;
		while( i ){ 
			format::format_utils::read_edge(buf + readed_bytes - i, edge);
		
			//std::cout<<"test "<<sizeof(edge)<<std::endl;	
			aux_array[edge.src].degree += 1;
			i -= edge_size;
	//	std::cout<<"test "<< i<<std::endl;
			//std::cout<<"here2"<<std::endl;	
		}
		
	}
	disk_io.write_join();
	std::cout<<"after initialize "<<std::endl;
		
	while (times){
		LOG_TRIVIAL(info)<<"remain "<<times<<" iterator(s)";
		int readed_edges = 0;
		disk_io.start_write(filename);

		while( !disk_io.is_over()){			//the second scan, scatter and gather phase
			//std::cout<<"test"<<times<<std::endl;	
			readed_bytes = disk_io.read(buf, byte_num_once);
			int i = readed_bytes;
		//LOG_TRIVIAL(info)<<"once a time "<<readed_bytes;
			while (i){
				format::format_utils::read_edge(buf + readed_bytes -i , edge);	
			
				if (update_bitset[ edge.dst ] == false ){	
					//if the vertice is not converged, then update
					aux_array[edge.dst].res[1-flag] += 		//new
		 			0.85 * aux_array[edge.src].res[flag] / aux_array[edge.src].degree;
				}

				i -= edge_size;
			}
		}
		disk_io.write_join();
		//LOG_TRIVIAL(info)<<"test ";
		
		int pos = 0;
		for(auto iter = aux_array.begin(); iter != aux_array.end(); iter++){
			pos = iter - aux_array.begin();
			if ( update_bitset[ pos ] == false ){ 		//updated
				if ( fabs(iter->res[flag] - iter->res[1-flag]) <0.000000001){ //convergence
					update_bitset[ pos ] = true;
					updated_num ++;
				}
				iter->res[flag] = iter->res[1-flag]; //update the old
			}
			iter->res[1-flag] = init_weight;
		}
		//auto iter2 = update_bitset.begin();
		//for (iter2 = update_bitset.begin(); iter2 != update_bitset.end(); iter2++){
		//	if ((*iter2) == false){
		//		break;
		//	}
		//}
		//if ( iter2 == update_bitset.end() ){ //all bits are 1
		LOG_TRIVIAL(info)<<vertex_num - updated_num<<" / "<<vertex_num;
		if ( updated_num == vertex_num ){ //all bits are 1
			LOG_TRIVIAL(info) << "convergence and exit";
			break;
		}
		times --;
	}
	output(aux_array, 1);
	
}
void output(std::vector<array > &aux_array, int flag){
	std::ofstream out("output.csv", std::ios::out);
	auto begin = aux_array.begin();
	for (auto iter = aux_array.begin(); iter != aux_array.end(); iter++){
		out << iter -begin<<" " 
			<<std::fixed<<std::setprecision(16)
			<<iter -> res[flag]<<std::endl;
	}
}
#endif