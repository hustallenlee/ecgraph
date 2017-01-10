#include <mpi.h>
#include <iostream>
#include <cstdlib>

//#include "core/engine.h"
#include "core/controller.h"
#include "core/computation.h"
#include "core/graph_define.h"
#include "core/update.h"

#include "utils/log_wrapper.h"
#include "utils/buffer.h"
#include "utils/consistent_hash.h"
#include "utils/types.h"
#include "utils/type_utils.h"
#include "utils/mylock.h"
#include "utils/config.h"


//算法
#include "algorithms/pagerank.h"
//#include "algorithms/bfs.h"
//#include "algorithms/wcc.h"


//TODO
//#include "core/computation.h" 

//第一个参数为图数据文件名，对于不同的算法后续的参数不同
int main(int argc, char **argv) {
	int world_size, self_rank;
	int provided;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	if (provided != MPI_THREAD_MULTIPLE) {
		std::cout<<"MPI do not Support Multiple thread";
		exit(0);
	}
	MPI_Comm_rank(MPI_COMM_WORLD, &self_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);

	
	
	//rank值为1，2，3三个计算节点
	std::vector<ecgraph::vertex_t> compute_nodes = {1,2,3};
	ecgraph::graph_data gd(argv[1]);
	ecgraph::consistent_hash ring(compute_nodes, &gd);
	if (self_rank == CONTROLLER_RANK) { //this is a controller

		//ecgraph::buffer<ecgraph::edge_t> file_buffer(GRAPH_DATA_BUFFER_SIZE);
		//开始读文件
		
		//file_buffer.start_write(argv[1]);

		//控制节点
		controller controller_node(argc, argv,
									world_size, self_rank, 
									&ring);
		
		//初始化，主要是做初始图的划分，传送图分区到对应的计算节点，
		//controller_node.init();	

		//图数据等信息传送完毕，开始控制图计算的进行
		//my_sleep(8);
		#ifdef MY_DEBUG
		LOG_TRIVIAL(info) <<"rank "<< self_rank << " starting as a controller";
		#endif
		controller_node.start();
	}
	else {
		
		//ecgraph::buffer<update_weight_double_t> out_buffer(OUT_BUFFER_SIZE);
		//ecgraph::buffer<update_weight_double_t> in_buffer(IN_BUFFER_SIZE);
		
		//启动
		#ifdef MY_DEBUG
		LOG_TRIVIAL(info) <<"rank "<< self_rank 
			<<" starting as a compute node, run pagerank algorithm";
		#endif
		pagerank algo;


		//将算法放入
		computation<update_weight_double_t> compute_node(argc, argv, 
												world_size, self_rank, 
												&ring, &algo);
		
		compute_node.start();
	}
	MPI_Finalize();
	return 0;
}