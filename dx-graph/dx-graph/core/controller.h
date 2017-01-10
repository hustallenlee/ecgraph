#ifndef _CONTROLLER_H_
#define _CONTROLLER_H_
//控制节点代码，控制节点是整个图计算的中枢，控制节点主要负责
//1.第一阶段图数据的分发，将edge list 根据图的一些相关信息进行分发
//2.图处理迭代的进行，图迭代完成之后要将自身迭代的消息发送给控制节点
//	由控制节点进行判断是否进行分裂，控制节点要维护一个所有计算节点的信息


#include <vector>
#include <mpi.h>
#include <exception>
#include <stdexcept>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/date_time.hpp>

#include <thread>
#include <mutex>
#include <cstdlib>
#include <cassert>
#include <algorithm>

#include "utils/types.h"
#include "utils/buffer.h"
#include "utils/consistent_hash.h"
#include "utils/mylock.h"
#include "core/graph_define.h"
#include "packet/messages.h"
#include "packet/message_factory.h"


using namespace boost::property_tree;
using namespace boost::gregorian;
using namespace boost;

//提供底层网络通信抽象，封装了底层数据传输，控制信息


class controller {
public:
	controller(int argc,
		char **argv,
		int world_size, //当前集群中参与图处理节点的总数
		int self_rank,
		ecgraph::consistent_hash *ring/*,
		ecgraph::buffer<ecgraph::edge_t> *graphfile_buffer*/
		);
	controller() = delete;

	//typedef format::vertex_t mpi_transport_datatype_t;

	//同步ring环的信息，将ring环的元数据同步给所有的计算节点
	void sync_ring_info();
	void send_to_all_node(void * buf, int count, int tag);
	//分发分区配置信息
	void send_partition_info();

	//分发图数据
	void distributing_graph();

	//在迭代中发送更新数据
	//void send_update();

	//向某个计算节点发送控制信息
	void send_controll_msg(std::string json_msg, int rank);
	void send_controll_msg(base_message *msg, int rank);
	//处理控制信息
	void handle_graph_controll_data(ecgraph::byte_t * buf, int len);
	
	//发送结束信息
	void end_all();
	void controller_start();
	void go();
	void set_all_compute_node_state(NODE_STATE state);
	//消息以json的形式发过来
	void recv_msg_from_compute_node();

	void broadcast_msg_to_all(base_message * msg);

	void start() {
		//auto f_send = std::bind(&controller::send, this);
		//auto f_recv = std::bind(&controller::send, this);
		//send_thrd = new std::thread(f_send);
		//recv_thrd = new std::thread(f_recv);
		//controller_start();
		m_graphfile_buffer->start_write(m_graph_filename);
		go();
	}
	void process_compute_node_info();
	bool check_compute_node_run_info();

	//=============消息处理相关=======================================
	int get_message_id(ecgraph::byte_t *buf, int len);
	void handle_message(compute_node_runtime_info_msg &msg);
	void handle_message(compute_node_stop_send_update_msg &msg);
	//===============================================================
	~controller() {
		/*if (send_thrd) {
			if (send_thrd->joinable()) {
				send_thrd->join();
			}
			delete send_thrd;
			send_thrd = NULL;
		}

		if (recv_thrd) {
			if (recv_thrd->joinable()) {
				recv_thrd->join();
			}
			delete recv_thrd;
			recv_thrd = NULL;
		}*/
		delete m_state_mutex;
		delete m_graphfile_buffer;
	}

	//=============状态相关函数======================================
public:
	NODE_STATE get_current_state() {
		return m_node_state;
	}

	void go_to_next_state() {
		ecgraph::scoped_lock lock(*m_state_mutex);
		if (m_node_state != NODE_STATE::FINISH_ALL) {		//迭代未完成
			m_node_state = static_cast<NODE_STATE>(m_node_state + 1);
		}
	}
	void set_current_state(NODE_STATE state) {
		ecgraph::scoped_lock lock(*m_state_mutex);	//加锁
		m_node_state = state;

	}
	//=============状态相关函数======================================
private:
	int m_argc;
	char** m_argv;
	int m_rank;
	int m_size;		//当前参与的总大小
	int m_world_size;
	NODE_TYPE m_node_type;
	NODE_STATE m_node_state;
	ecgraph::consistent_hash *m_ring;
	ecgraph::buffer<ecgraph::edge_t> *m_graphfile_buffer;

	std::thread *send_thrd;
	std::thread *recv_thrd;
	std::mutex *m_state_mutex;
	//ecgraph::config m_partition_config;
	std::string m_graph_filename;
	std::vector<ecgraph::vertex_t> m_machines;
	message_factory m_message_factory;

	//记录每轮迭代各计算节点的运行信息
	std::map<ecgraph::vertex_t, compute_node_run_info_t> m_run_info;
	int m_max_loop;
	int m_current_loop;
	std::map<ecgraph::vertex_t, bool> m_machine_is_stop;
};

controller::controller(int argc,
	char **argv,
	int world_size, //当前集群中参与图处理节点的总数
	int self_rank,
	ecgraph::consistent_hash *ring/*,
	ecgraph::buffer<ecgraph::edge_t> *graphfile_buffer*/
	) {
	m_argc = argc;
	m_argv = argv;
	m_world_size = world_size;
	m_rank = self_rank;
	m_node_type = NODE_TYPE::CONTROLL_NODE;
	m_node_state = NODE_STATE::BEFORE_START;	//初始化状态
	m_ring = ring;
	m_graphfile_buffer = new ecgraph::buffer<ecgraph::edge_t>(GRAPH_DATA_BUFFER_SIZE);

	m_size = m_ring->compute_size() + 1;
	m_state_mutex = new std::mutex();
	
	m_ring->get_machines(m_machines);//得到所有的计算节点
	m_max_loop = std::stoi(argv[2]);
	m_current_loop = 0;
	m_graph_filename = argv[1];
}



void controller::sync_ring_info() {

	//只能在一下几个状态中调用
	assert(m_node_state == NODE_STATE::BEFORE_START
		|| m_node_state == NODE_STATE::DISTRIBUTING_GRAHP
		|| m_node_state == NODE_STATE::BETWEEN_TWO_ITERATION);

	std::string hash_info = m_ring->save();
	send_to_all_node((void *)hash_info.c_str(), hash_info.size(), HASH_INFO_TAG);

}


void controller::send_controll_msg(std::string json_msg, int rank)
{
	//判断rank的合法性
	//存计算节点的rank值
	auto iter = std::find(m_machines.begin(), m_machines.end(), rank);
	if (iter == m_machines.end()) {
		LOG_TRIVIAL(warn) << "the controll message destination does not exist";
		return;
	}
	MPI_Send((void *)json_msg.c_str(), json_msg.size(), 
				MPI_BYTE, rank, 
				GRAPH_CONTROLL_TAG, MPI_COMM_WORLD);
	
}

void controller::send_controll_msg(base_message * msg, int rank)
{
	std::string json_msg = msg->serialize();
	send_controll_msg(json_msg, rank);
}


void controller::send_to_all_node(void * buf, int count, int tag) {

	//获得计算节点的数量
	const int COMPUTE_SIZE = m_machines.size();

	//断言，整个集群中有多台计算节点和一台控制节点
	assert((COMPUTE_SIZE + 1) == m_size);

	//存机器rank值到数组下标的映射，方便查找
	//以及数组下标到rank值得映射，方便查找
	std::map<ecgraph::vertex_t, int> machine_to_index;
	std::map<int, ecgraph::vertex_t> index_to_machine;
	for (int i = 0; i < COMPUTE_SIZE; i++) {//映射
		machine_to_index[m_machines[i]] = i;
		index_to_machine[i] = m_machines[i];
	}

	//发送所有缓冲区中的数据
	MPI_Request *reqs = new MPI_Request[COMPUTE_SIZE];
	//MPI_Status status[COMPUTE_SIZE];

	//发送数据
	for (int i = 0; i < COMPUTE_SIZE; i++) {
		MPI_Isend((void *)(buf),
			count,
			MPI_BYTE,
			index_to_machine[i],
			tag,
			MPI_COMM_WORLD,
			&reqs[i]);
	}
	//等待所有发送都完成
	MPI_Waitall(COMPUTE_SIZE, reqs, MPI_STATUS_IGNORE);
	delete [] reqs;

}
void controller::end_all()
{
	//只能在一下几个状态中调用
	//assert(m_node_state == NODE_STATE::BEFORE_START
	//|| m_node_state == NODE_STATE::DISTRIBUTING_GRAHP
	//|| m_node_state == NODE_STATE::BETWEEN_TWO_ITERATION);

	char buf[] = "end";
	send_to_all_node((void *)buf, sizeof(buf), END_TAG);
}

void controller::send_partition_info() {
	//只能在一下几个状态中调用
	assert(m_node_state == NODE_STATE::BEFORE_START
		|| m_node_state == NODE_STATE::DISTRIBUTING_GRAHP
		|| m_node_state == NODE_STATE::BETWEEN_TWO_ITERATION);


	//获得计算节点的数量
	const int COMPUTE_SIZE = m_machines.size();

	//断言，整个集群中有多台计算节点和一台控制节点
	assert((COMPUTE_SIZE + 1) == m_size);

	//以及数组下标到rank值得映射，方便查找
	std::map<ecgraph::vertex_t, int> machine_to_index;
	std::map<int, ecgraph::vertex_t> index_to_machine;
	for (int i = 0; i < COMPUTE_SIZE; i++) {//映射
		machine_to_index[m_machines[i]] = i;
		index_to_machine[i] = m_machines[i];
	}

	//发送所有缓冲区中的数据
	MPI_Request *reqs = new MPI_Request[COMPUTE_SIZE];
	//MPI_Status status[COMPUTE_SIZE];


	std::string partition_info = m_ring->get_graph_info();
	std::stringstream ss;
	ptree pt;
	ss << partition_info;
	read_json(ss, pt);		//读取json

	for (int i = 0; i < COMPUTE_SIZE; i++) {
		ptree send_pt = pt;
		try {
			std::pair<ecgraph::vertex_t, ecgraph::vertex_t> vertices_range
				= m_ring->get_vertices_on_node(index_to_machine[i]);

			//分区名字，如sw_1表示rank值为1的计算节点的所拥有的图分区
			send_pt.put("graph_info.partition_id", std::to_string(index_to_machine[i]));
			send_pt.put("graph_info.name", pt.get<std::string>("graph_info.name")
				+ "_"
				+ std::to_string(index_to_machine[i]));

			send_pt.put("graph_info.partition_end_vid",
				std::to_string(vertices_range.second));

			send_pt.put("graph_info.partition_start_vid",
				std::to_string(vertices_range.first));

			send_pt.put("graph_info.partition_mid_vid",
				std::to_string(-1));
		}
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[controller][sync_partition_info] bad path";
			return;
		}
		std::stringstream ss2;
		write_json(ss2, send_pt);

		MPI_Isend((void *)(ss2.str().c_str()),
			ss2.str().size(),
			MPI_BYTE,
			index_to_machine[i],
			GRAPH_INFO_TAG,
			MPI_COMM_WORLD,
			&reqs[i]);
	}
	//等待所有发送都完成
	MPI_Waitall(COMPUTE_SIZE, reqs, MPI_STATUS_IGNORE);
	delete reqs;
}



void controller::distributing_graph() {

	//断言，当前状态只能为 DISTRIBUTING_GRAHP，且为控制节点
	assert(m_node_state == NODE_STATE::DISTRIBUTING_GRAHP
		&& m_node_type == NODE_TYPE::CONTROLL_NODE);

	//获得计算节点的数量
	const int COMPUTE_SIZE = m_machines.size();

	//断言，整个集群中有多台计算节点和一台控制节点
	assert((COMPUTE_SIZE + 1) == m_size);

	//存机器rank值到数组下标的映射，方便查找
	//以及数组下标到rank值得映射，方便查找
	std::map<ecgraph::vertex_t, int> machine_to_index;
	std::map<int, ecgraph::vertex_t> index_to_machine;
	for (int i = 0; i < COMPUTE_SIZE; i++) {//映射
		machine_to_index[m_machines[i]] = i;
		index_to_machine[i] = m_machines[i];
	}
	#ifdef MY_DEBUG
	LOG_TRIVIAL(info) <<"controller(" <<m_rank << ") distributing graph data";
	#endif

	//申请内存作为本机发送缓存
	//为什么要用new，是因为程序栈空间有限，另，暂存edge_t必须是按字节对齐的。
	ecgraph::edge_t * read_buf;
	ecgraph::edge_t ** send_buf;
	try {
		read_buf = new ecgraph::edge_t[READ_GRAPH_DATA_ONCE];
	}
	catch (std::bad_alloc) {
		LOG_TRIVIAL(error) << "[controller][distributing_graph] alloc memory for read buffer failed";
		exit(0);
	}
	
	try {
		send_buf = new ecgraph::edge_t *[COMPUTE_SIZE];
	}
	catch (std::bad_alloc) {
		LOG_TRIVIAL(error) << "[controller][distributing_graph] alloc memory for send buffer failed";
		exit(0);
	}

	//保存每个缓冲区的数据存入的长度，初始值为0，
	for (int i = 0; i < COMPUTE_SIZE; i++) {	//为所有的计算节点申请发送缓存空间
		try {
			send_buf[i] = new ecgraph::edge_t[GRAPH_DATA_BUFFER_SIZE];
		}
		catch (std::bad_alloc){
			LOG_TRIVIAL(error) << "[controller][distributing_graph] alloc memory for send buffer["<<i<<"] failed";
			exit(0);
		}
	}
	//开始从图数据缓存中不断的读取数据

	while (!m_graphfile_buffer->is_over()) {

		std::vector<int> length(COMPUTE_SIZE, 0);
		int readed_num = m_graphfile_buffer->read(read_buf, READ_GRAPH_DATA_ONCE);
		
		/*#ifdef MY_DEBUG
		LOG_TRIVIAL(info) << "controller(" << m_rank << ") readed "<<readed_num<<" edges";
		#endif*/

		//将的数据放到对应的发送缓存中,根据边的src进行划分
		int buf_index = 0;
		ecgraph::vertex_t machine_id;
		for (int i = 0; i < readed_num; i++) {
			machine_id = (*m_ring)(read_buf[i].src);
			buf_index = machine_to_index[machine_id];

			//if (i == 0) { LOG_TRIVIAL(info) << "i index " << buf_index; }
			//复制到对应的发送缓冲区
			/*memcpy((void *)(send_buf[buf_index] + length[buf_index]),
			(void *)(read_buf + i),
			sizeof(edge_t));*/

			//assert(buf_index < COMPUTE_SIZE && buf_index >= 0);

			//if (!(buf_index < COMPUTE_SIZE && buf_index >= 0)) {
				/*LOG_TRIVIAL(info) << "index "<<i 
								<< " buffer index "<<buf_index
								<<" len "<<length[buf_index]
								<<" machine id "<< machine_id;*/
			//}
			try {
				send_buf[buf_index][length[buf_index]] = read_buf[i];
			
				length[buf_index] ++;	//记录发送缓冲区的长度（以edge_t的大小为单位）
			}
			catch (std::exception &eobj) {
				LOG_TRIVIAL(error) << eobj.what();
			}
			//LOG_TRIVIAL(info) << "index " << i;
		}

		//发送所有缓冲区中的数据
		MPI_Request *reqs = new MPI_Request[COMPUTE_SIZE];
		//MPI_Status status[COMPUTE_SIZE];
		//发送数据
		
		for (int i = 0; i < COMPUTE_SIZE; i++) {
			MPI_Isend((void *)(send_buf[i]),
				length[i] * sizeof(ecgraph::edge_t),
				MPI_BYTE,
				index_to_machine[i],
				GRAPH_DATA_TAG,
				MPI_COMM_WORLD,
				&reqs[i]);
		}
		//等待所有发送都完成
		MPI_Waitall(COMPUTE_SIZE, reqs, MPI_STATUS_IGNORE);
		//发送所有缓冲区中的数据end
		delete[] reqs;
	}
	//释放申请的内存
	delete[] read_buf;
	for (int i = 0; i < COMPUTE_SIZE; i++) {	//为其他计算节点申请缓存空间
		delete[] send_buf[i];
	}
	delete[] send_buf;
	//释放申请的内存end
}



void controller::handle_graph_controll_data(ecgraph::byte_t * buf, int len)
{
/*LOG_TRIVIAL(info) << "controller(" << m_rank << ") TEST";
		while (1) {}*/
	int msg_id = get_message_id(buf, len);
	if (msg_id < 0 || !(msg_id >= 0 && msg_id < 1000)) {
		LOG_TRIVIAL(warn) << "bad message while dealing with the message from compute node";
		return;
	}
	
	if (msg_id == COMPUTE_NODE_RUNTIME_INFO_MSGID) {

		//只能在这个状态接收该消息
		assert(m_node_state == NODE_STATE::BETWEEN_TWO_ITERATION);
		compute_node_runtime_info_msg msg;
		msg.load(std::string((char *)buf, len));
		handle_message(msg);
	}
	else if (msg_id == COMPUTE_NODE_STOP_SEND_UPDATE_MSGID) {
		//只能在这个状态接收该消息
		//LOG_TRIVIAL(info) << "controller(" << m_rank << ") received stop send update msg";
		assert(m_node_state == NODE_STATE::IN_ITERATION);
		compute_node_stop_send_update_msg msg;
		msg.load(std::string((char *)buf, len));
		
		handle_message(msg);

	}

}



void controller::controller_start() {
	bool go_on = true;
	while (go_on) { //状态不为未完成，则继续
		switch (get_current_state()) {

			//在开始之前要发送图元数据分区信息，图元数据包括分区的一切信息
			case NODE_STATE::BEFORE_START:
			{
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") in BEFORE_START";
				#endif

				//同步ring环信息
				sync_ring_info();

				//发送图分区信息
				send_partition_info();

				//改变状态
				set_current_state(NODE_STATE::DISTRIBUTING_GRAHP);
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") out BEFORE_START";
				#endif
				break;
			}


				//分发图数据中
			case NODE_STATE::DISTRIBUTING_GRAHP:
			{
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") in DISTRIBUTING_GRAHP";
				#endif
				distributing_graph();
				set_current_state(NODE_STATE::FINISH_DISTRIBUTED_GRAPH);
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") out DISTRIBUTING_GRAHP";
				#endif
				break;
			}

				//完成图数据的分发
			case NODE_STATE::FINISH_DISTRIBUTED_GRAPH:
			{	
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") in FINISH_DISTRIBUTED_GRAPH";
				#endif

				
				
				//进入IN_ITERATION
				set_current_state(NODE_STATE::IN_ITERATION);
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") out FINISH_DISTRIBUTED_GRAPH";
				#endif
				break;
			}
				
				//在迭代中
			case NODE_STATE::IN_ITERATION:
			{	
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") in IN_ITERATION";
				#endif
				//发送给所有的计算节点一个控制信息，使其开始进行迭代, 
				//计算节点收到该消息后进入IN_ITERATION状态，并开始迭代
				controller_permit_start_msg *msg = new controller_permit_start_msg();
				msg->set_controller_id(m_rank);
				broadcast_msg_to_all(msg);
				delete msg;

				//进入BETWEEN_TWO_ITERATION状态
				set_current_state(NODE_STATE::BETWEEN_TWO_ITERATION);
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") out IN_ITERATION";
				#endif
				break;
			}
			
				//两轮迭代之间
			case NODE_STATE::BETWEEN_TWO_ITERATION:
			{
				
				//等待计算节点将自己一轮迭代的一些运行消息发送过来。
				recv_msg_from_compute_node();
				break;
			}
				//完成迭代
			case NODE_STATE::FINISH_ITERATION:
			{
				set_current_state(NODE_STATE::FINISH_ALL);
				break;
			}
			case NODE_STATE::FINISH_ALL:
			{	
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") in FINISH_ALL";
				#endif
				end_all();
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") end all";
				#endif
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") out FINISH_ALL";
				#endif
				go_on = false;
				break;
			}
		}
	}
}

void controller::go()
{
	bool go_on = true;
	while (go_on) { //状态不为未完成，则继续
		switch (get_current_state()) {

			//在开始之前要发送图元数据分区信息，图元数据包括分区的一切信息
			case NODE_STATE::BEFORE_START:
			{
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") in BEFORE_START";
				#endif

				//同步ring环信息
				sync_ring_info();

				//发送图分区信息
				send_partition_info();

				//改变状态
				set_current_state(NODE_STATE::DISTRIBUTING_GRAHP);

				//设置设置计算节点状态
				set_all_compute_node_state(NODE_STATE::DISTRIBUTING_GRAHP);

				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") out BEFORE_START";
				#endif
				break;
			}


			//分发图数据中
			case NODE_STATE::DISTRIBUTING_GRAHP:
			{
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") in DISTRIBUTING_GRAHP";
				#endif
				distributing_graph();
				set_current_state(NODE_STATE::FINISH_DISTRIBUTED_GRAPH);
				//设置设置计算节点状态
				set_all_compute_node_state(NODE_STATE::FINISH_DISTRIBUTED_GRAPH);
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") out DISTRIBUTING_GRAHP";
				#endif
				break;
			}

			//完成图数据的分发
			case NODE_STATE::FINISH_DISTRIBUTED_GRAPH:
			{
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") in FINISH_DISTRIBUTED_GRAPH";
				#endif
				//发送一条消息，表示最大迭代次数
				/*controller_send_max_loop_msg *msg = new controller_send_max_loop_msg();
				msg->set_controller_id(m_rank);
				msg->set_max_loop(m_max_loop);
				broadcast_msg_to_all(msg);
				delete msg;*/

				//进入IN_ITERATION
				set_all_compute_node_state(NODE_STATE::IN_ITERATION);
				set_current_state(NODE_STATE::IN_ITERATION);
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") out FINISH_DISTRIBUTED_GRAPH";
				#endif
				break;
			}

			//在迭代中
			case NODE_STATE::IN_ITERATION:
			{
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") in IN_ITERATION";
				#endif
				//发送给所有的计算节点一个控制信息，使其开始进行迭代, 
				//计算节点收到该消息后进入IN_ITERATION状态，并开始迭代
				controller_permit_start_msg *msg = new controller_permit_start_msg();
				msg->set_controller_id(m_rank);
				broadcast_msg_to_all(msg);
				delete msg;

				//从所有的计算节点收消息，这条消息代表所有的计算节点不会向外发update了
				m_machine_is_stop.clear();
			
				//LOG_TRIVIAL(info) << "controller(" << m_rank << ") recving stop msg";
				recv_msg_from_compute_node();
				LOG_TRIVIAL(info) << "controller(" << m_rank 
					<< ") received all stop send update msg";
				//while (1) {}

				process_compute_node_info();
				m_current_loop++;


				//进入BETWEEN_TWO_ITERATION状态
				set_all_compute_node_state(NODE_STATE::BETWEEN_TWO_ITERATION);
				//recv_msg_from_compute_node();
				set_current_state(NODE_STATE::BETWEEN_TWO_ITERATION);
			
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") out IN_ITERATION";
				#endif
				break;
			}

			//两轮迭代之间
			case NODE_STATE::BETWEEN_TWO_ITERATION:
			{
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") in BETWEEN_TWO_ITERATION";
				#endif
			
				//等待计算节点将自己一轮迭代的一些运行消息发送过来。
				m_run_info.clear();
				//从计算节点接收消息，存在m_run_info中
				recv_msg_from_compute_node();
				LOG_TRIVIAL(info) << "controller(" << m_rank
					<< ") received all run info msg";


				//对m_run_info的信息进行处理
				//TODO

				if (check_compute_node_run_info()) { //check ok
					//看是否达到最大迭代次数
					//LOG_TRIVIAL(info) << "controller(" << m_rank << ")"<< ;
					if (m_max_loop <= m_current_loop) {
					
						set_all_compute_node_state(NODE_STATE::FINISH_ITERATION);
						set_current_state(NODE_STATE::FINISH_ITERATION);
					}
					else {
						//继续迭代
						set_all_compute_node_state(NODE_STATE::IN_ITERATION);
						set_current_state(NODE_STATE::IN_ITERATION);
					}
				}
				else {
					LOG_TRIVIAL(info) << "controller(" << m_rank
									<< ")[BETWEEN_TWO_ITERATION]"
									<<"check runtime infomation failed";
					exit(0);
				}
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") out BETWEEN_TWO_ITERATION";
				#endif
				break;
			}
			//完成迭代
			case NODE_STATE::FINISH_ITERATION:
			{
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") in FINISH_ITERATION";
				#endif
				set_current_state(NODE_STATE::FINISH_ALL);
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") out FINISH_ITERATION";
				#endif
				break;
			}
			case NODE_STATE::FINISH_ALL:
			{
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") in FINISH_ALL";
				#endif
				//end_all();
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") end all";
				#endif
				#ifdef MY_DEBUG
				LOG_TRIVIAL(info) << "controller(" << m_rank << ") out FINISH_ALL";
				#endif
				go_on = false;
				break;
			}
		}
	}
}

inline void controller::set_all_compute_node_state(NODE_STATE state)
{
	controller_change_compute_node_state_msg *msg
		= new controller_change_compute_node_state_msg();
	msg->set_controller_id(m_rank);
	msg->set_state_index((int)state);
	broadcast_msg_to_all(msg);
	delete msg;
}


//必须接收到所有计算节点的消息
void controller::recv_msg_from_compute_node()
{
	const int COMPUTE_SIZE = m_machines.size();
	ecgraph::byte_t **recvmsg_buf;
	
	try {
		recvmsg_buf = new ecgraph::byte_t *[COMPUTE_SIZE];
	}
	catch(std::bad_alloc){
		LOG_TRIVIAL(error) << "can not alloc memory for receiving msg from compute node";
		exit(0);
	}

	for (int i = 0; i < COMPUTE_SIZE; i++) {
		try {
			recvmsg_buf[i] = new ecgraph::byte_t[MAX_NONDATA_SIZE];
		}
		catch (std::bad_alloc) {
			LOG_TRIVIAL(error) << "can not alloc memory for receiving msg["
								<<i<<"] from compute node";
			exit(0);
		}
	}
	
	std::map<int, ecgraph::vertex_t> index_to_machine;
	for (int i = 0; i < COMPUTE_SIZE; i++) {//映射
		index_to_machine[i] = m_machines[i];
	}

	MPI_Request *reqs = new MPI_Request[COMPUTE_SIZE];
	MPI_Status  *status = new MPI_Status[COMPUTE_SIZE];

	for (int i = 0; i < COMPUTE_SIZE; i++) {
		MPI_Irecv((void *)(recvmsg_buf[i]), MAX_NONDATA_SIZE, 
					MPI_BYTE, index_to_machine[i],
					GRAPH_CONTROLL_TAG, MPI_COMM_WORLD, &reqs[i]);
	}

	//等待所有计算节点的消息
	MPI_Waitall(COMPUTE_SIZE, reqs, status);

	/*LOG_TRIVIAL(info) << "controller(" << m_rank << ") TEST";
	while (1) {}*/
	//处理所有消息
	int count = 0;
	for (int i = 0; i < COMPUTE_SIZE; i++) {
		MPI_Get_count(&status[i], MPI_BYTE, &count);
		LOG_TRIVIAL(info) << "controller(" << m_rank 
							<< ") received msg from "<<index_to_machine[i];

		handle_graph_controll_data((ecgraph::byte_t *)(recvmsg_buf[i]), count);
	}

	//处理完计算节点发过来的控制消息之后，做进一步处理
	//如使计算节点分裂等等,让计算节点继续等等
	//process_compute_node_info();


	
	//释放内存
	for (int i = 0; i < COMPUTE_SIZE; i++) {
		delete[] recvmsg_buf[i];
	}
	delete[] recvmsg_buf;
	delete[] reqs;
	delete[] status;

}

void controller::broadcast_msg_to_all(base_message * msg)
{
	std::string json_msg = msg->serialize();
	send_to_all_node((void *)json_msg.c_str(), json_msg.size(), GRAPH_CONTROLL_TAG);
}

inline void controller::process_compute_node_info()
{
	//assert(m_run_info.size() == m_machines.size());
	//查看是否到达最大迭代次数
	if (m_run_info.size() == 0) { return; }
	auto first = m_run_info.begin();
	
	if (!check_compute_node_run_info()) {
		return;
	}
	
	/*if (m_max_loop <= m_current_loop) {
		controller_end_all_msg *msg = new controller_end_all_msg();
		msg->set_controller_id(m_rank);
		broadcast_msg_to_all(msg);
		set_all_compute_node_state(NODE_STATE::FINISH_ITERATION);
		return;
	}*/
	//根据计算节点的运行信息，决定下一步的工作，如计算节点的分等；
	//以下代码为控制计算节点是否分裂以及如何分裂的代码

	//代码暂无TODO

	//使计算节点继续迭代
	/*controller_permit_start_msg *msg = new controller_permit_start_msg();
	msg->set_controller_id(m_rank);
	broadcast_msg_to_all(msg);*/
}

inline bool controller::check_compute_node_run_info()
{
	for (auto &item : m_run_info) {
		if (m_max_loop >0) {
			if (m_max_loop < item.second.current_loop) {
				LOG_TRIVIAL(error) << "[check_compute_node_run_info] check failed "
									<<"because the compute nodes current loop is bigger than max loop";
				return false;
			}
		}
		else {
			//m_max_loop = item.second.max_loop;
			//m_current_loop = item.second.current_loop;
			LOG_TRIVIAL(error) << "max loop of the iteration is not set";
		}
	}
	return true;
}


inline int controller::get_message_id(ecgraph::byte_t * buf, int len)
{
	ptree pt;
	std::stringstream ss;
	int msg_id = -1;
	ss << std::string((char *)buf, len);
	//std::cout << ss.str() <<" "<<ss.str().size()<< std::endl;
	read_json(ss, pt);
	try {
		msg_id = pt.get<int>("msg_id");
	}
	catch (boost::property_tree::ptree_bad_path) {
		LOG_TRIVIAL(error) << "bad message because no field named msg_id";
	}
	return msg_id;
}

inline void controller::handle_message(compute_node_runtime_info_msg &msg)
{
	//收到所有计算节点发送的这条消息就代表一轮的迭代结束，
	//控制节点知道了计算节点迭代的一些消息
	int compute_node_id = msg.get_compute_node_id();
	auto iter = m_run_info.find(compute_node_id);
	
	if (iter == m_run_info.end()) {
		compute_node_run_info_t run;
		run.compute_id = compute_node_id;
		run.runtime = msg.get_runtime();
		run.current_loop = msg.get_current_loop();
		m_run_info[compute_node_id] = run;
	}
}

inline void controller::handle_message(compute_node_stop_send_update_msg & msg)
{
	ecgraph::vertex_t compute_node_id = msg.get_compute_node_id();
	auto iter = m_machine_is_stop.find(compute_node_id);
	if (iter == m_machine_is_stop.end()) {
		m_machine_is_stop[compute_node_id] = true;
	}
}

#endif