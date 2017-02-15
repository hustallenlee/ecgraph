#include <mpi.h>
#include <iostream>
#include <cstdlib>

#include "core/engine.h"
#include "core/master.h"
#include "core/worker.h"
#include "core/graph_define.h"
#include "core/update.h"

#include "utils/log_wrapper.h"
#include "utils/buffer.h"
#include "utils/consistent_hash.h"
#include "utils/types.h"
#include "utils/type_utils.h"
#include "utils/mylock.h"
#include "utils/config.h"


//�㷨
//#include "algorithms/pagerank.h"
//#include "algorithms/bfs.h"
#include "algorithms/wcc.h"


//TODO
//#include "core/worker.h" 

//��һ������Ϊͼ�����ļ��������ڲ�ͬ���㷨�����Ĳ�����ͬ
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

	
	
	
	if (self_rank == MASTER_RANK) { //this is a master
		//rankֵΪ1��2��3��������ڵ�
		std::vector<ecgraph::vertex_t> worker_nodes = {1,2};
		ecgraph::graph_data gd(argv[1]);
		ecgraph::consistent_hash ring(worker_nodes, &gd);
		//ecgraph::buffer<ecgraph::edge_t> file_buffer(GRAPH_DATA_BUFFER_SIZE);
		//��ʼ���ļ�
		
		//file_buffer.start_write(argv[1]);

		//���ƽڵ�
		master master_node(argc, argv,
									world_size, self_rank, 
									&ring);
		
		//��ʼ������Ҫ������ʼͼ�Ļ��֣�����ͼ��������Ӧ�ļ���ڵ㣬
		//master_node.init();	

		//ͼ���ݵ���Ϣ������ϣ���ʼ����ͼ����Ľ���
		//my_sleep(8);
		#ifdef MY_DEBUG
		LOG_TRIVIAL(info) <<"rank "<< self_rank << " starting as a master";
		#endif
		master_node.start();
	}
	else {
		
		//ecgraph::buffer<update_weight_double_t> out_buffer(OUT_BUFFER_SIZE);
		//ecgraph::buffer<update_weight_double_t> in_buffer(IN_BUFFER_SIZE);
		
		//����
		#ifdef MY_DEBUG
		LOG_TRIVIAL(info) <<"rank "<< self_rank 
			<<" starting as a worker, run wcc algorithm";
		#endif
		wcc algo;

		//���㷨����
		worker<update_weight_int_t> worker_node(argc, argv,world_size, 
										self_rank, &algo);
		
		worker_node.start();
		
	}
	MPI_Finalize();
	return 0;
}