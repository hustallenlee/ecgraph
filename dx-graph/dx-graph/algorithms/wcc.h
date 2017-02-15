#ifndef _WCC_H_
#define _WCC_H_
#include "utils/buffer.h"
#include "utils/config.h"
#include "utils/log_wrapper.h"
#include "utils/types.h"
#include "utils/type_utils.h"
#include "core/engine.h"
#include "core/update.h"

#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <cmath>
#include <fstream>
#include <iomanip>


class wcc: public engine<update_weight_int_t>{
private:

    //std::vector<array_t > *aux_array; //auxiliary array
    int edge_size;

public:
	void init(){


		//update_bitset = new std::vector<bool>;
    	//aux_array = new std::vector<array_t>(m_vertex_num); //auxiliary array
    	edge_size = sizeof(ecgraph::edge_t);

	}

	void clear(){
		//delete update_bitset;
		//delete aux_array;
	}
	void scatter() {
		//init_read();
		LOG_TRIVIAL(info)<<"scatter ...";
		ecgraph::edge_t edge;
		//ecgraph::vertex_t min;
		ecgraph::vertex_t src;
		ecgraph::vertex_t dst;
		update_weight_int_t update;
		if (super_step() == 0){//set every vertex's id
			auto begin = result.begin();
			for (auto iter = result.begin(); iter != result.end(); iter ++){
					*iter = get_gobal_graph_vid(iter - begin);
			}
		}
		while (get_next_edge(edge)) {
			src = result[get_local_graph_vertices_offset(edge.src)];

			//if (src < dst) {
				update.id = edge.dst;
				update.update_value = src;
			//}
			/*if (src > dst) {
				update.id = edge.src;
				update.update_value = dst;
			}*/
			add_update(update, false);
		}

	}

	bool gather(){
		LOG_TRIVIAL(info)<<"worker("<<get_rank()<<") gatter ...";
		ecgraph::vertex_t updated_num = 0;
		update_weight_int_t update;
		//auto start = ua.begin();
		int pos = 0;

		/*for (auto iter = ua.begin(); iter != ua.end(); iter ++){
			pos = iter - start;
			if ( (*aux_array)[pos].label > (*iter) ){
				(*aux_array)[pos].label = (*iter);
				updated_num ++;
			}
		}*/
		while (get_update(update)) {
			if (result[get_local_graph_vertices_offset(update.id)] 
				> update.update_value) {
				result[get_local_graph_vertices_offset(update.id)] 
					= update.update_value;
				updated_num++;
			}
		}
		LOG_TRIVIAL(info) << "worker(" << get_rank() <<") "
			<< updated_num << " / " << get_graph_vertices_num();
		if (updated_num == 0) { //all bits are 1
			LOG_TRIVIAL(info) << "convergence and exit after "
				<< super_step() + 1
				<< " iterations";
			set_convergence();
			return true;
		}
		return false;
	}
	void output(){
    	std::ofstream out("wcc_output_"+std::to_string(get_rank()) + ".csv", std::ios::out);
    	auto begin = result.begin();
    	for (auto iter = begin; iter != result.end(); iter++){
        	out << get_gobal_graph_vid(iter -begin)<<" "
				<<std::setiosflags(std::ios::fixed)
            	<<*iter<<std::endl;
    	}
    	/*auto begin = (*aux_array).begin();
    	for (auto iter = begin; iter != (*aux_array).end(); iter++){
        	out << iter -begin<<" "
            	<<iter -> degree<<std::endl;
    	}*/
		
	}       
};

/*int main(int argc, char * argv[]){
	 if(argc != 2){
        std::cout<<"Usage: execute_file filename "
                    << std::endl;
        return 0;
    }
	std::stringstream record;
	std::string filename;

	record.clear();
	record<< argv[1];
	record>> filename;
	
	wcc cc(filename,INT_MAX);
	cc.run();
	return 0;
}
*/
#endif