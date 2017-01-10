#ifndef _BFS_H_
#define _BFS_H_
#include "utils/buffer.h"
#include "utils/config.h"
#include "utils/log_wrapper.h"
#include "utils/types.h"
#include "utils/type_utils.h"
#include "core/engine.h"
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <climits>
typedef struct{
	ecgraph::vertex_t label;
}array_t;

typedef struct{
	ecgraph::vertex_t id;
	ecgraph::vertex_t update_value;
}update_t;

class bfs: public engine<update_t >{
private:
	//std::vector<bool> *update_bitset;
    std::vector<array_t > *aux_array; //auxiliary array
    int edge_size;
	ecgraph::vertex_t root;
public:
	bfs(std::string fn, int mloop, int rt): engine(fn, mloop){


		//update_bitset = new std::vector<bool>;
    	aux_array = new std::vector<array_t>(m_vertex_num, {UINT_MAX}); //auxiliary array
    	edge_size = sizeof(ecgraph::edge_t);
		root = rt;
		//ua.resize(vertex_num, UINT_MAX);
		
	}

	~ bfs(){
		//delete update_bitset;
		delete aux_array;
	}
	void scatter() {
		//init_read();
		LOG_TRIVIAL(info)<<"scatter ...";
		ecgraph::edge_t edge;
		ecgraph::vertex_t min;
		ecgraph::vertex_t src;
		ecgraph::vertex_t dst;
		update_t update;

		if (super_step() == 0){//set every vertex's id

			(*aux_array)[root].label = 0;			
		}
		while( get_next_edge(edge) ){
			src = (*aux_array)[edge.src].label;
			dst = (*aux_array)[edge.dst].label;
				
			if (src < dst){
				update.id =  edge.dst + 1;
			}
			min = src < dst ? src : dst ;
			ua[edge.src] = min < ua[edge.src] ? min : ua[edge.src]; 
			ua[edge.dst] = min < ua[edge.dst] ? min : ua[edge.dst];
        }
	}

	bool gather(){
		LOG_TRIVIAL(info)<<"gatter ...";
		ecgraph::vertex_t updated_num = 0;
		auto start = ua.begin();
		int pos = 0;

		for (auto iter = ua.begin(); iter != ua.end(); iter ++){
			pos = iter - start;
			if ( (*iter) != UINT_MAX ){
				if (  (*aux_array)[pos].label  > ( (*iter) + 1) ){
						(*aux_array)[pos].label = (*iter) + 1;
						updated_num ++;
				}
			}
		}
		LOG_TRIVIAL(info)<<updated_num<<" / "<<m_vertex_num;
        if ( updated_num == 0 ){ //all bits are 1
           	LOG_TRIVIAL(info) << "convergence and exit after "
								<<super_step() 
								<< " iterations";
           	return true;
        }
		
		return false;
	}
	void output(){
    	std::ofstream out("output.csv", std::ios::out);
    	auto begin = (*aux_array).begin();
    	for (auto iter = begin; iter != (*aux_array).end(); iter++){
        	out << iter -begin + m_min_id<<" "
            	<<std::fixed<<std::setprecision(16)
            	<<iter -> label<<std::endl;
    	}
    	/*auto begin = (*aux_array).begin();
    	for (auto iter = begin; iter != (*aux_array).end(); iter++){
        	out << iter -begin<<" "
            	<<iter -> degree<<std::endl;
    	}*/
		
	}       
};

/*int main(int argc, char * argv[]){
	 if(argc != 3){
        std::cout<<"Usage: execute_file filename root "
                    << std::endl;
        return 0;
    }
	std::stringstream record;
	std::string filename;
	format::vertex_t root;

	record.clear();
	record<< argv[1];
	record>> filename;
	
	record.clear();
	record<< argv[2];
	record>> root;
	
	bfs bs(filename,INT_MAX, root);
	bs.run();
	return 0;
}*/
#endif