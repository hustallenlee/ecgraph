#ifndef _WORKER_H_
#define _WORKER_H_

#include <mpi.h>

#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/date_time.hpp>

#include <thread>
#include <mutex>
#include <cstdlib>
#include <cassert>
#include <ctime>
#include <set>
#include <cstdio>
#include <algorithm>

#include "utils/types.h"
#include "utils/buffer.h"
#include "utils/consistent_hash.h"
#include "utils/mylock.h"
#include "core/graph_define.h"
#include "packet/messages.h"
#include "packet/message_factory.h"
#include "core/engine.h"
#include "utils/graph_data_sort.h"

using namespace boost::property_tree;
using namespace boost::gregorian;
using namespace boost;

//提供底层网络通信抽象，封装了底层数据传输，控制信息


/*所有的传递的信息都是json*/


template <typename update_type>	//update_type 结构体必须含有id成员变量
class worker {
public:
	worker(int argc,
		char **argv,
		int world_size, //当前集群中参与图处理节点的总数
		int self_rank,
		/*ecgraph::consistent_hash *ring,
		ecgraph::buffer<update_type> *out_buffer,
		ecgraph::buffer<update_type> *in_buffer*/
		engine<update_type> *algorithm
		);
	worker() = delete;

	//typedef format::vertex_t mpi_transport_datatype_t;

	//同步ring环的信息，将ring环的元数据同步给所有的计算节点
	//void sync_ring_info();
	//void send_to_all_node(void * buf, int count, int tag);
	//分发分区配置信息
	//void sync_partition_info();

	//分发图数据
	//void distributing_graph(ecgraph::buffer<ecgraph::edge_t> *graph_data_buffer);

	//在迭代中发送更新数据
	void send_update();
	void recv_update();
	//计算节点不断的接收所有的数据，不管是什么类型的数据
	//数据类型有：图数据，update，
	//ring环信息，图数据信息
	//控制信息
	void handle_graph_data(ecgraph::byte_t * buf, int len);
	void handle_update_data(ecgraph::byte_t * buf, int len);
	void handle_hash_info_data(ecgraph::byte_t * buf, int len);
	void handle_graph_info_data(ecgraph::byte_t * buf, int len);
	void handle_graph_controll_data(ecgraph::byte_t * buf, int len);

	void send_message_to_master(base_message * msg,int tag, int master_rank);
	void send_message_to_master(std::string json_msg, int tag, int master_rank);
	
	//向某一个worker发送消息
	void send_msg_to_one_worker(base_message *msg, int tag, int worker_rank);
	void send_msg_to_one_worker(std::string json_msg, int tag, int worker_rank);

	//二分自己
	void binay_partition_myself();
	void sync_data_to_worker(int worker_rank, int start);
	void set_one_worker_state(NODE_STATE state, int worker_rank);
	void handle_data_sync(ecgraph::byte_t * buf, int len);
	void disk_sort();
	ecgraph::vertex_t get_graph_mid();
	//接收数据，可以是各种数据
	void recv();

	void start() {
		//auto f_send = std::bind(&worker::send, this);
		//auto f_recv = std::bind(&worker::send, this);
		//send_thrd = new std::thread(f_send);
		//recv_thrd = new std::thread(f_recv);
		//auto f_send = std::bind(&worker<update_type>::send, this);
		//send_thrd = new std::thread(f_send);
		//std::function<void(this)> f_send = std::bind(&worker<update_type>::send_update, this);
		recv();
	}

	~worker() {
		/*if (send_thrd) {
			if (send_thrd->joinable()) {
				send_thrd->join();
			}
			delete send_thrd;
			send_thrd = NULL;
		}*/

		/*if (recv_thrd) {
			if (recv_thrd->joinable()) {
				recv_thrd->join();
			}
			delete recv_thrd;
			recv_thrd = NULL;
		}*/
		delete m_state_mutex;
		delete m_ring_mutex;
		delete m_graph_partition;
		delete m_ring;
		delete m_partition_config;
	}


	//=============处理具体控制消息===================================
	int get_message_id(ecgraph::byte_t *buf, int len);
	void handle_message(master_permit_start_msg &msg);
	void handle_message(master_end_all_msg &msg);
	//void handle_message();

	//=============处理具体控制消息end================================



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
	int m_size;		//当前参与的总大小，  更新集群信息时要更新这个
	int m_world_size;
	NODE_TYPE m_node_type;
	NODE_STATE m_node_state;

	//本集群的ring环信息, 更新集群信息时要更新这个
	ecgraph::consistent_hash *m_ring;

	//本分区的发送和接收缓存，用于和上层通信
	ecgraph::buffer<update_type> *m_out_buffer;
	ecgraph::buffer<update_type> *m_in_buffer;

	std::thread *send_thrd;
	std::thread *recv_thrd;
	std::thread *graph_thrd;
	//MPI_Datatype m_update;

	//状态锁
	std::mutex *m_state_mutex;
	std::mutex *m_ring_mutex;//ring 的互斥体

	//本节点的分区配置信息 更新集群信息时要更新这个
	ecgraph::config *m_partition_config;

	//本节点的分区名字
	std::string m_partition_filename;

	//存计算节点的rank值， 更新集群信息时要更新这个
	std::vector<ecgraph::vertex_t> m_machines;

	long long m_partition_edges_num;
	long long m_partition_mid_vid;

	//图的一部分，即分区。改图文件句柄会在接收图元数据的时候被打开，未打开之前为NULL
	std::fstream *m_graph_partition;

	engine<update_type> *m_algorithm;

	//m_in_buffer 关闭标志
	int m_in_buffer_close_flag;
	clock_t m_start_time;
	clock_t m_end_time;
	std::function<void(void)> m_f_send;
	std::function<void(void)> m_f_algorithm;
};

template <typename update_type>
worker<update_type>::worker(int argc,
	char **argv,
	int world_size, //当前集群中参与图处理节点的总数
	int self_rank,
	/*ecgraph::consistent_hash *ring,
	ecgraph::buffer<update_type> *out_buffer,
	ecgraph::buffer<update_type> *in_buffer,*/
	engine<update_type> *algorithm
	) {
	m_argc = argc;
	m_argv = argv;
	m_world_size = world_size;
	m_rank = self_rank;
	m_node_type = NODE_TYPE::WORKER_NODE;
	m_node_state = NODE_STATE::BEFORE_START;	//初始化状态
	m_ring = new ecgraph::consistent_hash();
	//m_out_buffer = out_buffer;
	//m_in_buffer = in_buffer;
	m_algorithm = algorithm;
	m_out_buffer = m_algorithm->get_out_buffer();
	m_in_buffer = m_algorithm->get_in_buffer();

	
	m_state_mutex = new std::mutex();
	m_ring_mutex = new std::mutex();
	//存计算节点的rank值

	

	m_partition_edges_num = 0;
	m_partition_mid_vid = -1;

	m_graph_partition = new std::fstream();
	m_partition_config = new ecgraph::config() ;
	m_size = 0;

	//本计算节点接收到控制节点发的结束message时，将其置为1
	m_in_buffer_close_flag = -1;

	m_start_time = -1; //无效值
	m_end_time = -1;	//无效值
	m_f_send = std::bind(&worker<update_type>::send_update, this);
	m_f_algorithm = std::bind(&engine<update_type>::iterate_once, m_algorithm);
}


template <typename update_type>
void worker<update_type>::send_update() {

	//断言，本函数只能运行在
	assert(m_node_type == NODE_TYPE::WORKER_NODE
		&& m_node_state == NODE_STATE::IN_ITERATION);


	//获得计算节点的数量
	const int WORKER_SIZE = m_machines.size();

	//断言，整个集群中有多台计算节点和一台控制节点
	assert((WORKER_SIZE + 1) == m_size);

	//存机器rank值到数组下标的映射，方便查找
	std::map<ecgraph::vertex_t, int> machine_to_index;
	std::map<int, ecgraph::vertex_t> index_to_machine;
	for (int i = 0; i < WORKER_SIZE; i++) {//映射
		machine_to_index[m_machines[i]] = i;
		index_to_machine[i] = m_machines[i];
	}

	//申请内存作为本机缓存
	//为什么要用new，是因为程序栈空间有限，另，暂存update_type必须是按字节对齐的。
	//申请SEND_BUFFER_SIZE大小的读缓冲区
	update_type* read_buf = new update_type[SEND_BUFFER_SIZE];
	update_type** send_buf = new update_type*[WORKER_SIZE];

	for (int i = 0; i < WORKER_SIZE; i++) {	//为其他计算节点申请缓存空间
		send_buf[i] = new update_type[SEND_BUFFER_SIZE];
	}
	//申请内存作为本机缓存end

	//从缓冲区中读取update到发送缓存中
	bool go_on = true;
	int readed_num;					//读取到的update的数量
	MPI_Request *reqs = new MPI_Request[WORKER_SIZE];
	MPI_Status *status = new MPI_Status[WORKER_SIZE];
	//std::ofstream update_out("update_"+std::to_string(m_rank));
	while (go_on) {
		std::vector<int> length(WORKER_SIZE, 0);;	//保存每个缓冲区的数据存入的长度，初始值为0
		
		if (!m_out_buffer->is_over()) { //未结束
			readed_num = m_out_buffer->read(read_buf, SEND_BUFFER_SIZE);
			//update_out.write((char *)read_buf, readed_num*sizeof(update_type));
			//LOG_TRIVIAL(info) << "worker(" << m_rank << ") generates " 
				//<< readed_num << " updates";
			
			//没有读到任何update
			if (readed_num == 0) {
				break;
			}
			int buf_index;
			ecgraph::vertex_t node_id;
			std::map<ecgraph::vertex_t, int>::iterator iter;
			for (int i = 0; i < readed_num; i++) {  //放入各自对应的缓冲区中
				node_id = (*m_ring)(read_buf[i].id);

				//能找到
				if ((iter = machine_to_index.find(node_id)) != machine_to_index.end()) {
					buf_index = iter->second;
					send_buf[buf_index][length[buf_index]] = read_buf[i];
					length[buf_index] ++;//缓冲区长度++
				}
				else {
					LOG_TRIVIAL(warn)<< "this update "<<read_buf[i].id
									<<" can not find the destination";
				}
			}

			//发送所有缓冲区中的数据
			
			
			//发送数据
			//my_flag用来去除发往本计算节点的update
			int my_flag = 0;
			//本计算节点的发送缓存索引
			if (machine_to_index.find(m_rank) == machine_to_index.end()) {
				LOG_TRIVIAL(error) << "some bad thing happened,"
									<<" because the self rank does not belong to this cluster";
				return;
			}
			int self_index = machine_to_index[m_rank];
			for (int i = 0; i < WORKER_SIZE; i++) {

				//若为要发往本机的数据，则跳过
				if (i == self_index) { my_flag = 1; continue; }

				MPI_Isend((void *)(send_buf[i]),
					length[i] * sizeof(update_type),
					MPI_BYTE,
					index_to_machine[i],
					UPDATE_DATA_TAG,
					MPI_COMM_WORLD,
					&reqs[i - my_flag]);
			}
			/*if (m_rank == 2) {
				LOG_TRIVIAL(info) << "worker(" << m_rank << ") m_in_buffer send update";
				m_in_buffer->show();
			}*/
			//将发往本计算节点的缓冲区中的数据直接写入本机接收缓存
			if (!m_in_buffer->push(send_buf[self_index], length[self_index])) {
				LOG_TRIVIAL(error) << "worker(" << m_rank << ") m_in_buffer should be reset";
				exit(0);
			}
			/*LOG_TRIVIAL(info) << "worker(" << m_rank 
				<< ") m_in_buffer local updates: "<< length[self_index];
			if (m_rank == 2) {
				LOG_TRIVIAL(info) << "worker(" << m_rank << ") m_in_buffer send update";
				m_in_buffer->show();
			}*/
			//等待所有发送都完成
			MPI_Waitall(WORKER_SIZE - my_flag, reqs, status);
			for (int i = 0; i < (WORKER_SIZE - my_flag); i++) {
				if (status[i].MPI_ERROR != MPI_SUCCESS) {
					LOG_TRIVIAL(error) << "send update error, rank:"
						<< index_to_machine[i] << ", error code "
						<< status[i].MPI_ERROR;
				}
			}
			//发送所有缓冲区中的数据end


			//MPI_Send((void *)buf, readed_size*sizeof(update_type), MPI_BYTE, );
		}
		else {	//已结束
			go_on = false;
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") m_out_buffer is over";
			//发送一条消息给控制节点知晓
			/*worker_stop_send_update_msg *msg
				= new worker_stop_send_update_msg();
			msg->set_worker_id(m_rank);
			send_message_to_master(msg, master_RANK);
			delete msg;*/
		}
	}
	//从缓冲区中读取update到发送缓存中end

	
	worker_stop_send_update_msg *msg
		= new worker_stop_send_update_msg();
	msg->set_worker_id(m_rank);
	send_message_to_master(msg, GRAPH_CONTROLL_TAG ,MASTER_RANK);
	delete msg;
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") send stop send update msg ok";

	//设置结束时间
	m_end_time = clock();
	//释放缓存
	delete [] read_buf;
	for (int i = 0; i < WORKER_SIZE; i++) {	//循环释放所有的空间
		delete[] send_buf[i];
	}
	delete[] send_buf;
	delete[] reqs;
	delete[] status;
	//释放缓存end
}

//接收update的线程
template<typename update_type>
inline void worker<update_type>::recv_update()
{
	/*update_type *recv_buf = new update_type[RECV_BUFFER_SIZE];
	MPI_Status  status;
	bool go_on = true;
	while (go_on) {
		MPI_Recv((void *)recv_buf,
			sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
			MPI_ANY_SOURCE, UPDATE_DATA_TAG,
			MPI_COMM_WORLD, &status);
		//处理数据
		int count;  //接收的数据量
		MPI_Get_count(&status, MPI_BYTE, &count);
	}*/
}


template<typename update_type>
void worker<update_type>::handle_graph_data(ecgraph::byte_t * buf, int len)
{
	//接收的长度必须为sizeof(update_type)的整数倍
	assert(len%sizeof(ecgraph::edge_t) == 0);
	assert(m_node_type == NODE_TYPE::WORKER_NODE &&
			m_node_state == NODE_STATE::DISTRIBUTING_GRAHP);

	//放入in_buffer中
	/*#ifdef MY_DEBUG
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") receiving";
	#endif*/
	ecgraph::edge_t *edge_buf = (ecgraph::edge_t *)(buf);
	int edge_len = len / sizeof(ecgraph::edge_t);

	//记录边的数量
	m_partition_edges_num += edge_len;

	//写入文件
	if (m_graph_partition->is_open()) {
		
		//将收到的图数据写入
		m_graph_partition->write((char *)buf, len);
		/*#ifdef MY_DEBUG
		LOG_TRIVIAL(info) <<"worker("<<m_rank 
							<< ") writing "<< m_partition_edges_num<< " edges";
		#endif*/
	}
	else {
		LOG_TRIVIAL(error) <<"worker ("<<m_rank<< ") the file "
			<< m_partition_filename << " is not opened";
		return;
	}
	(*m_partition_config)["partition_edges_num"] = std::to_string(m_partition_edges_num);
}

//处理接收到的更新
template<typename update_type>
void worker<update_type>::handle_update_data(ecgraph::byte_t * buf, int len)
{
	//接收的长度必须为sizeof(update_type)的整数倍
	assert(len % sizeof(update_type) == 0);
	assert(m_node_type == NODE_TYPE::WORKER_NODE
			&& m_node_state == NODE_STATE::IN_ITERATION);

	//放入in_buffer中
	
	update_type *update_buf = (update_type *)(buf);
	int update_len = len / sizeof(update_type);
	/*LOG_TRIVIAL(info) << "worker(" << m_rank << ") received updates " << update_len;
	if (m_rank == 2) {
		LOG_TRIVIAL(info) << "worker(" << m_rank << ") m_in_buffer info handle update data";
		m_in_buffer->show();
	}*/
	if (!m_in_buffer->push(update_buf, update_len)) {
		LOG_TRIVIAL(error) << "worker(" << m_rank << ") m_in_buffer should be reset";
		exit(0);
	}
	/*if (m_rank == 2) {
		LOG_TRIVIAL(info) << "worker(" << m_rank << ") m_in_buffer info handle update data";
		m_in_buffer->show();
	}*/
}


template<typename update_type>
void worker<update_type>::handle_hash_info_data(ecgraph::byte_t * buf, int len)
{
	assert(m_node_type == NODE_TYPE::WORKER_NODE 
			&& (m_node_state == NODE_STATE::BEFORE_START
				|| m_node_state == NODE_STATE::BETWEEN_TWO_ITERATION));
	//更新本机ring环所有信息
	//TODO
	#ifdef MY_DEBUG
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") recv ring meta data: "
		<< std::string((char *)buf, len);
	#endif

	//更新本机的元数据信息
	m_ring->load_from_json(std::string((char *)buf, len));
	m_size = m_ring->worker_size() + 1;
	m_ring->get_workers(m_machines);
}


//必须在发完图数据前接收该信息
template<typename update_type>
void worker<update_type>::handle_graph_info_data(ecgraph::byte_t * buf, int len)
{
	assert(m_node_type == NODE_TYPE::WORKER_NODE 
			&& m_node_state == NODE_STATE::BEFORE_START);
	
	#ifdef MY_DEBUG
	LOG_TRIVIAL(info) <<"worker("<< m_rank << ") recv graph meta data: "
						<<std::string((char *)buf, len);
	#endif
	
	//从字符串中加载配置信息
	m_partition_config->load_from_string(std::string((char *)buf, len));

	//格式检查
	//检查该配置文件
	if (!m_partition_config->check()) {
		LOG_TRIVIAL(error) << "rank " << m_rank << " check failed";
		exit(0);
	}

	//设置文件名
	m_partition_filename = (*m_partition_config)["name"];

	#ifdef MY_DEBUG
	LOG_TRIVIAL(info) <<"worker(" << m_rank<< ") my partition name " << m_partition_filename;
	#endif
	
	if (!m_graph_partition->is_open()) {
		
		//以二进制和输出模式打开文件
		m_graph_partition->open(m_partition_filename, 
								std::ios::binary | std::ios::out);
		
		if (!m_graph_partition->is_open()) {
			LOG_TRIVIAL(info) << m_partition_filename<< " can not be opened" ;
			exit(0);
		}
		
	}
	

	(*m_partition_config)["partition_edges_num"] = std::to_string(m_partition_edges_num);

	long long partition_start_vid = std::stoll((*m_partition_config)["partition_start_vid"]);
	long long partition_end_vid = std::stoll((*m_partition_config)["partition_end_vid"]);
	long long vertices_num = std::stoll((*m_partition_config)["vertices"]);

	if (partition_start_vid > partition_end_vid){
		m_partition_mid_vid = ((vertices_num +
			partition_start_vid +
			partition_end_vid) / 2) % vertices_num;
	}
	else {
		m_partition_mid_vid = (partition_end_vid + partition_start_vid) / 2;
	}
	(*m_partition_config)["partition_mid_vid"] = std::to_string(m_partition_mid_vid);
	//(*m_partition_config).dump(m_partition_filename+".json");
	
}

template<typename update_type>
void worker<update_type>::handle_graph_controll_data(ecgraph::byte_t * buf, int len)
{
	assert(m_node_type == NODE_TYPE::WORKER_NODE);
	//处理控制节点发过来的控制消息

	//首先获取msg_id
	
	int msg_id = get_message_id(buf, len);
	if (msg_id < 0) {
		LOG_TRIVIAL(warn) << "bad message";
		return;
	}
	switch (msg_id) {
		case MASTER_PERMIT_START_MSGID:
		{
			master_permit_start_msg msg;
			msg.load(std::string(buf, len));
			handle_message(msg);
			break;
		}
		case MASTER_END_ALL_MSGID:
		{
			master_end_all_msg msg;
			msg.load(std::string(buf, len));
			handle_message(msg);
		}
		default:
			LOG_TRIVIAL(info) << "this message can not be identified";
			break;
	}
}

template<typename update_type>
inline void 
worker<update_type>::send_message_to_master(base_message * msg, int tag, int master_rank)
{
	std::string json_msg = msg->serialize();
	send_message_to_master(json_msg, tag, master_rank);
}

template<typename update_type>
inline void 
worker<update_type>::send_message_to_master(std::string json_msg, int tag, int master_rank)
{
	//判断rank的合法性
	//存计算节点的rank值
	//auto iter = std::find(m_machines.begin(), m_machines.end(), master_rank);
	//if (iter == m_machines.end()) {
		//LOG_TRIVIAL(warn) << "[send_message_to_master]the controll message destination does not exist";
		//return;
	//}
	MPI_Send((void *)json_msg.c_str(), json_msg.size(),
		MPI_BYTE, master_rank,
		tag, MPI_COMM_WORLD);
}

template<typename update_type>
inline void worker<update_type>::send_msg_to_one_worker(base_message * msg, 
	int tag, int worker_rank)
{
	std::string json_msg = msg->serialize();
	send_msg_to_one_worker(json_msg, tag, worker_rank);
}

template<typename update_type>
inline void worker<update_type>::send_msg_to_one_worker(std::string json_msg, 
	int tag, int worker_rank)
{
	MPI_Send((void *)json_msg.c_str(), json_msg.size(),
		MPI_BYTE, worker_rank, tag, MPI_COMM_WORLD);
}

template<typename update_type>
inline void worker<update_type>::binay_partition_myself()
{
	//在分裂过程中，要分裂的的worker
	//将分区数据及分区配置给另外一个新的worker
	//并将新的ring信息给另一个新的worker,
	//这个新的worker在FINISH_DISTRIBUTED_GRAPH状态后直接进入
	//BETWEEN_TWO_ITERATION状态，这样所有worker又处于同一状态了。
	long long mid = std::stoll((*m_partition_config)["partition_mid_vid"]);
	long long start = std::stoll((*m_partition_config)["partition_start_vid"]);
	long long end = std::stoll((*m_partition_config)["partition_end_vid"]);
	
	//首先获取一个新的worker
	std::vector<ecgraph::vertex_t> machines;
	m_ring->get_workers(machines);
	std::set<ecgraph::vertex_t> worker_set(machines.begin(), machines.end());
	
	//在资源池找一个空闲的worker, 0默认是master，不可用
	ecgraph::vertex_t new_worker = MASTER_RANK; //无效值
	for (ecgraph::vertex_t i = 1; i < m_world_size; i++) {
		if (worker_set.find(i) == worker_set.end()) {//不存在，则找到了
			new_worker = i;
			break;
		}
	}
	if (new_worker == MASTER_RANK) { return; }
	//更新自身ring环信息
	m_ring->split(std::make_pair((ecgraph::vertex_t)mid, new_worker));

	//序列化
	std::string ring_info = m_ring->save();

	//发送给新worker
	send_msg_to_one_worker(ring_info, HASH_INFO_TAG, new_worker);

	//发送分区配置信息===============================
	std::fstream my_partition_config(m_partition_filename + ".json", std::ios::in);
	ptree send_pt, pt;
	read_json(my_partition_config, pt);
	send_pt = pt;
	//填充值
	send_pt.put("graph_info.partition_id", std::to_string(new_worker));
	std::string new_worker_partition_name = pt.get<std::string>("graph_info.name");
	int pos = new_worker_partition_name.find_last_of('_');
	new_worker_partition_name.erase(pos);
	send_pt.put("graph_info.name", new_worker_partition_name
		+ "_" + std::to_string(new_worker));
	send_pt.put("graph_info.partition_end_vid",	std::to_string(mid));
	send_pt.put("graph_info.partition_start_vid", std::to_string(start));
	send_pt.put("graph_info.partition_mid_vid",	std::to_string(-1));
	//发送
	std::stringstream ss;
	write_json(ss,send_pt);
	send_msg_to_one_worker(ss.str(), GRAPH_INFO_TAG, new_worker);
	ss.clear();

	//发送完毕后，改变其状态为DISTRIBUTING_GRAHP，以接收图数据
	set_one_worker_state(NODE_STATE::DISTRIBUTING_GRAHP, new_worker);
	
	//=====================================================================


	//开始发送图分区数据====================================================
	//
	ecgraph::buffer<ecgraph::edge_t> graphdata_buffer(GRAPH_DATA_BUFFER_SIZE);
	ecgraph::edge_t * edge_buffer = new ecgraph::edge_t[READ_GRAPH_DATA_ONCE];
	//首先为图分区改名
	std::string new_partition_name = m_partition_filename + "_tmp";
	if (rename(m_partition_filename.c_str(), 
			new_partition_name.c_str()) != 0) {
		LOG_TRIVIAL(error) << "worker(" << m_rank << ") rename "
			<< m_partition_filename << " failed";
		perror(m_partition_filename.c_str());
		exit(0);
	}
	if (rename((m_partition_filename+".json").c_str(),
		(m_partition_filename + ".json_tmp").c_str()) != 0) {
		LOG_TRIVIAL(error) << "worker(" << m_rank << ") rename "
			<< (m_partition_filename + ".json_tmp") << " failed";
		perror((m_partition_filename + ".json_tmp").c_str());
		exit(0);
	}
	std::ofstream my_partition(m_partition_filename, std::ios::out
													| std::ios::app);
	//分配发送缓冲 0 为新worker 1为本机
	ecgraph::edge_t* send_buf[2];
	send_buf[0] = new ecgraph::edge_t[SEND_BUFFER_SIZE];
	send_buf[1] = new ecgraph::edge_t[SEND_BUFFER_SIZE];

	graphdata_buffer.start_write(new_partition_name);
	ecgraph::vertex_t dst_rank;
	int my_edges_num = 0; //再次分完区后本机剩下的边数
	while (!graphdata_buffer.is_over()) {
		int length[2] = {0};
		int readed_num = graphdata_buffer.read(edge_buffer, READ_GRAPH_DATA_ONCE);
		for (int i = 0; i < readed_num; i++) {
			dst_rank = (*m_ring)(edge_buffer[i].src);
			if (dst_rank == new_worker) {
				//放到send_buf[1]中
				send_buf[0][length[0]++] = edge_buffer[i];
			}
			else if(dst_rank == m_rank){
				send_buf[1][length[1]++] = edge_buffer[i];
			}
			else {
				LOG_TRIVIAL(error) << "worker(" << m_rank << ") "
					<< "error happened in sending graph data";
				exit(0);
			}
		}
		//发送和写入
		my_partition.write((char *)(send_buf[1]), 
							sizeof(ecgraph::edge_t)*length[1]);
		my_edges_num += length[1];
		MPI_Send((void *)(send_buf[0]),
							length[0]*sizeof(ecgraph::edge_t),
							MPI_BYTE,
							new_worker,
							GRAPH_DATA_TAG,
							MPI_COMM_WORLD);
	}
	//发送完图数据之后，再设置其状态为FINISH_DISTRIBUTED_GRAPH
	set_one_worker_state(NODE_STATE::FINISH_DISTRIBUTED_GRAPH, new_worker);

	//再发送一个同步迭代次数的消息，是新的worker的迭代次数和其他worker同步
	worker_sync_state_msg *msg = new worker_sync_state_msg();
	msg->set_worker_id(m_rank);
	msg->set_current_loop(m_algorithm->super_step());
	send_msg_to_one_worker(msg, GRAPH_CONTROLL_TAG, new_worker);
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") sync step to worker("
		<<new_worker<<") ok";

	sync_data_to_worker(new_worker, 
		m_algorithm->get_local_graph_vertices_offset(mid));
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") mid "<<mid
			<< " local "<< m_algorithm->get_local_graph_vertices_offset(mid);


	//然后再发一个改变状态的消息，使新的worker的状态和其他worker同步
	set_one_worker_state(NODE_STATE::BETWEEN_TWO_ITERATION, new_worker);
	//发完以上消息后，所有的所有的worker就处于BETWEEN_TWO_ITERATION
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") set new worker("
		<<new_worker<<") BETWEEN_TWO_ITERATION";

	//更新自己的分区配置信息
	my_partition_config.close();
	my_partition_config.open(m_partition_filename + ".json", std::ios::out);


	//以下等一会再填
	long long global_graph_vertices_num
		= std::stoll((*m_partition_config)["vertices"]);

	long long my_mid;
	if (end >= (mid + 1)) {
		my_mid = ((end + mid + 1) / 2) % global_graph_vertices_num;
	}
	else {
		my_mid = ((global_graph_vertices_num + end + mid + 1) / 2)
			% global_graph_vertices_num;
	}
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") my_mid "<<my_mid
								<<" end "<<end<<" start "<<mid+1;

	//更新m_partition_config
	(*m_partition_config)["partition_start_vid"] = std::to_string(mid+1);	
	(*m_partition_config)["partition_mid_vid"] = std::to_string(my_mid);
	(*m_partition_config)["partition_end_vid"] = std::to_string(end);
	(*m_partition_config)["partition_edges_num"] = std::to_string(my_edges_num);
	//保存本worker的图分区配置文件
	m_partition_config->dump(m_partition_filename+".json");

	//处理本机的degree和result
	int *degree = m_algorithm->get_degree().data();
	decltype(m_algorithm->get_result_value_type()) 
		*result = m_algorithm->get_result().data();

	
	//本地数组的开始和结束
	int local_start = m_algorithm->get_local_graph_vertices_offset(mid + 1);
	int local_end = m_algorithm->get_local_graph_vertices_offset(end);

	for (int i = local_start; i <= local_end; i++) {
		degree[i - local_start] = degree[i];
		result[i - local_start] = result[i];
	}
	m_algorithm->get_degree().resize(local_end - local_start +1);
	m_algorithm->get_result().resize(local_end - local_start + 1);
	/*LOG_TRIVIAL(info) << "degree[0] " << degree[0] << " result[0] " << result[0]
		<<"size "<< m_algorithm->get_degree().size();*/
	delete[] send_buf[0];
	delete[] send_buf[1];
	my_sleep(1);
	//========================================================================
}


//start表示从数组的什么位置开始同步
template<typename update_type>
inline void worker<update_type>::sync_data_to_worker(int worker_rank, int end)
{
	//开始同步degree和result
	decltype(m_algorithm->get_result_value_type()) 
		*result = m_algorithm->get_result().data();
	int *degree = m_algorithm->get_degree().data();
	worker_sync_data_t *datas = new worker_sync_data_t[SEND_BUFFER_SIZE];
	int length = 0;
	
	for (int i = 0; i <= end; i++) {
		datas[length].index = i;
		datas[length].degree = degree[i];
		datas[length].result = result[i];
		++length;
		if (length == SEND_BUFFER_SIZE || i == end) {
			MPI_Send((void*)datas, length*sizeof(worker_sync_data_t),
				MPI_BYTE, worker_rank, DATA_SYNC_TAG, MPI_COMM_WORLD);
			length = 0;
		}
	}

}


template<typename update_type>
inline void worker<update_type>::set_one_worker_state(NODE_STATE state, int worker_rank)
{
	//此时worker作为一个伪master改变一个新worker的状态
	master_change_worker_state_msg *msg
		= new master_change_worker_state_msg();
	msg->set_master_id(m_rank);
	msg->set_state_index((int)state);
	send_msg_to_one_worker(msg, GRAPH_CONTROLL_TAG, worker_rank);
	delete msg;
}

template<typename update_type>
inline void worker<update_type>::handle_data_sync(ecgraph::byte_t * buf, int len)
{
	assert(len % (sizeof(worker_sync_data_t)) == 0);
	worker_sync_data_t *sync_data = (worker_sync_data_t *)buf;
	int sync_data_len = len / sizeof(worker_sync_data_t);
	int index = 0;
	int *degree = m_algorithm->get_degree().data();
	decltype(m_algorithm->get_result_value_type()) 
		*result = m_algorithm->get_result().data();
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") handle_data_sync"
					<<" len "<<sync_data_len;
	for (int i = 0; i < sync_data_len; i++) {
		index = sync_data[i].index;
		degree[index] = sync_data[i].degree;
		result[index] = sync_data[i].result;
	}
}

template<typename update_type>
inline void worker<update_type>::disk_sort()
{
	//对图数据进行外部排序
	graph_data_sort(m_algorithm, m_partition_filename);
}


//a*|v| + |E|
//a为图的平均度
template<typename update_type>
inline ecgraph::vertex_t worker<update_type>::get_graph_mid()
{
	//对排好序的图数据取中点


	ecgraph::edge_t  *edge_buf = new ecgraph::edge_t[READ_GRAPH_DATA_ONCE];

	//打开文件读
	std::ifstream graphdata_file(m_partition_filename);
	
	//ecgraph::graph_data gd(argv[1]);

	//获得全图的边数
	long long edges_num = m_algorithm->get_edges_num();

	//std::cout << "edges_num " << edges_num << std::endl;

	//获得全图的结点数
	ecgraph::vertex_t vertices_num = m_algorithm->get_gobal_graph_vertices_num();

	LOG_TRIVIAL(info) << "worker(" << m_rank << ") vertices_num " 
					<< vertices_num;

	//获得图分区的开始结点
	ecgraph::vertex_t partition_start_vid = m_algorithm->get_start_vid();

	LOG_TRIVIAL(info) << "worker(" << m_rank << ") partition_start_vid " 
					<< partition_start_vid;

	ecgraph::vertex_t partition_end_vid = m_algorithm->get_end_vid();

	LOG_TRIVIAL(info) << "worker(" << m_rank << ") partition_end_vid "
					<< partition_end_vid;

	//获得分区的结点数
	ecgraph::vertex_t partition_vertices_num 
		= m_algorithm->get_graph_vertices_num();

	LOG_TRIVIAL(info) << "worker(" << m_rank << ") partition_vertices_num " 
						<< partition_vertices_num;
	
	//获得图分区的边数
	long long partition_edges_num = m_algorithm->get_partition_edges_num();
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") partition_edges_num "
					<< partition_edges_num;


	//全图的平均度
	long long average_degree = edges_num / vertices_num;
	
	//外存模式时，设置为1看看
	average_degree = 1;


	//全分区的COST
	long long partition_total_cost
		= average_degree * partition_vertices_num + partition_edges_num;
	//以中点为段的期望COST
	//std::cout << "here" << std::endl;
	long long expect_cost = partition_total_cost / 2;


	/*std::cout << "avg degree " << average_degree
		<< " partition_total_cost " << partition_total_cost
		<< " expect_cost " << expect_cost << std::endl;*/

	//开始读排序后的图，找到my_mid
	ecgraph::vertex_t my_mid = partition_start_vid;

	long long edges_num_tmp = 0;
	long long cost_tmp = 0;
	int read_num = 0;
	bool stop = false;
	int yu = 0;

	
	while (!graphdata_file.eof()) {

		read_num = graphdata_file.read((char *)edge_buf + yu,
			READ_GRAPH_DATA_ONCE*sizeof(ecgraph::edge_t) - yu).gcount();
		read_num += yu;
		yu = read_num % sizeof(ecgraph::edge_t);
		read_num = read_num / sizeof(ecgraph::edge_t);

		for (int i = 0; i < read_num; i++) {
			edges_num_tmp++;
			my_mid = edge_buf[i].src;
			cost_tmp = ((
				(long long)my_mid +
				(long long)vertices_num -
				(long long)partition_start_vid
				) % (long long)vertices_num)* average_degree + edges_num_tmp;
			if (cost_tmp >= expect_cost) {
				stop = true;
				break;
			}
		}
		if (stop) break;
	}

	return my_mid;
}

template<typename update_type>
void worker<update_type>::recv()
{
	//断言，需要为计算节点
	assert(m_node_type == NODE_TYPE::WORKER_NODE);

	bool go_on = true;
	ecgraph::byte_t *recv_buf = new ecgraph::byte_t[RECV_BUFFER_SIZE];
	MPI_Status status;
	bool skip_interation = true;
	while (go_on) { //状态不为未完成，则继续
		
		switch (get_current_state()) {

			//在开始之前要发送图元数据分区信息，图元数据包括分区的一切信息
		case NODE_STATE::BEFORE_START:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in BEFORE_START";
			#endif

			bool next = true;
			while (next) {
				//接收消息
				MPI_Recv((void *)recv_buf,
					sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
					MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
					//处理数据
				int count;  //接收的数据量
				MPI_Get_count(&status, MPI_BYTE, &count);

				//assert(status.MPI_SOURCE == MASTER_RANK);
				switch (status.MPI_TAG)
				{
				case HASH_INFO_TAG:
					
					//处理接收的ring环信息
					handle_hash_info_data(recv_buf, count);
					break;

				case GRAPH_INFO_TAG:
					
					//处理接收的图元数据信息
					handle_graph_info_data(recv_buf, count);
					
					break;
					
				case GRAPH_CONTROLL_TAG: //设置状态消息
				{
					if (get_message_id(recv_buf, count)
						!= MASTER_CHANGE_WORKER_STATE_MSGID) {
						LOG_TRIVIAL(warn) << "expect a change state message";
						continue;
					}
					/*LOG_TRIVIAL(info) << "worker(" << m_rank
						<< ") recv change state msg source " << status.MPI_SOURCE;*/
					master_change_worker_state_msg msg;
					msg.load(std::string((char *)recv_buf, count));
					set_current_state((NODE_STATE)msg.get_state_index());
					next = false;
					break;
				}
				case END_TAG:
				{
					set_current_state(NODE_STATE::FINISH_ALL);
					LOG_TRIVIAL(info) << "worker(" << m_rank << ") received end";
					next = false;
					break;
				}
				default:
					LOG_TRIVIAL(warn) <<"worker("<<m_rank<< ") [BEFORE_START]not an expected message";
					break;
				}
			}
			
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") out BEFORE_START";
			#endif
			break;
		}


		//分发图数据中
		case NODE_STATE::DISTRIBUTING_GRAHP:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in DISTRIBUTING_GRAHP";
			#endif

			bool next = true;
			while (next) {
				//接收消息
				MPI_Recv((void *)recv_buf,
					sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
					MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
				//处理数据
				int count;  //接收的数据量
				MPI_Get_count(&status, MPI_BYTE, &count);
				//assert(status.MPI_SOURCE == MASTER_RANK);//控制节点
				switch (status.MPI_TAG)
				{
				case GRAPH_DATA_TAG:

					handle_graph_data(recv_buf, count);
					break;

				case GRAPH_CONTROLL_TAG:
				{
					if (get_message_id(recv_buf, count)
						!= MASTER_CHANGE_WORKER_STATE_MSGID) {
						LOG_TRIVIAL(warn) << "worker(" << m_rank 
							<< ") expect a change state message";
						continue;
					}
					//LOG_TRIVAIL
					/*LOG_TRIVIAL(info) << "worker(" << m_rank
						<< ") recv change state msg source " << status.MPI_SOURCE;*/
					master_change_worker_state_msg msg;
					msg.load(std::string((char *)recv_buf, count));
					set_current_state((NODE_STATE)msg.get_state_index());

					//将分区配置信息写入磁盘
					(*m_partition_config).dump(m_partition_filename + ".json");
					next = false;
					break;
				}
				
				default:
					LOG_TRIVIAL(warn) << "worker(" << m_rank 
						<< ") [DISTRIBUTING_GRAHP]not an expected message"
						<<" message TAG "<< status.MPI_TAG
						<< " source " << status.MPI_SOURCE;
					break;
				}
			}
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") out DISTRIBUTING_GRAHP";
			#endif
			break;
		}

		//完成图数据的分发
		case NODE_STATE::FINISH_DISTRIBUTED_GRAPH:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in FINISH_DISTRIBUTED_GRAPH";
			#endif
			//首先关掉m_graph_partition
			m_graph_partition->close();
			
			//使算法加载图数据
			m_algorithm->load_graph(m_partition_filename);

			//初始化
			m_algorithm->init();

			//求图进行均衡划分的中值
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") sorting";
			disk_sort();
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") sorted";


			ecgraph::vertex_t mid_after_calculation = get_graph_mid();
			(*m_partition_config)["partition_mid_vid"] 
				= std::to_string(mid_after_calculation);
			m_algorithm->set_partition_mid_vid(mid_after_calculation);

			(*m_partition_config).dump(m_partition_filename + ".json");
			
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") mid vid "
				<< (*m_partition_config)["partition_mid_vid"];

			

			bool next = true;
			while (next) {
				//接收消息
				MPI_Recv((void *)recv_buf,
					sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
					MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
				//处理数据
				int count;  //接收的数据量
				MPI_Get_count(&status, MPI_BYTE, &count);
				//assert(status.MPI_SOURCE == MASTER_RANK);//控制节点
				switch (status.MPI_TAG)
				{
				case GRAPH_CONTROLL_TAG:
				{	
					int msg_id = get_message_id(recv_buf, count);

					if (msg_id == MASTER_CHANGE_WORKER_STATE_MSGID) {
						master_change_worker_state_msg msg;
						//装载消息
						LOG_TRIVIAL(info) << "worker(" << m_rank
							<< ") recv change state msg source " << status.MPI_SOURCE;

						msg.load(std::string((char *)recv_buf, count));
						//设置状态
						set_current_state((NODE_STATE)msg.get_state_index());
						next = false;//结束
						
					}
					else if (msg_id == WORKER_SYNC_STATE_MSGID) {
						worker_sync_state_msg msg;
						msg.load(std::string((char *)recv_buf, count));
						m_algorithm->set_current_step(msg.get_current_loop());
					}
					else{
						LOG_TRIVIAL(warn)<< "worker(" << m_rank
							<< ") expect a change state message";
						continue;
					}

					break;
				}
				case DATA_SYNC_TAG:
				{
					handle_data_sync(recv_buf, count);
					break;
				}
				default:
					LOG_TRIVIAL(warn) <<"worker("<<m_rank
						<< ") [FINISH_DISTRIBUTED_GRAPH]not an expected message"
						<< " message TAG " << status.MPI_TAG
						<< " source "<<status.MPI_SOURCE
						<<std::string((char *)recv_buf, count);
					break;
				}
			}
			
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") out FINISH_DISTRIBUTED_GRAPH";
			#endif
			break;
		}

		//在迭代中
		case NODE_STATE::IN_ITERATION:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in IN_ITERATION";
			#endif
			skip_interation = false;
			bool next = true;
			while (next) {
				//接收消息
				MPI_Recv((void *)recv_buf,
					sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
					MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
				//处理数据
				int count;  //接收的数据量
				MPI_Get_count(&status, MPI_BYTE, &count);
				switch (status.MPI_TAG)
				{
					case UPDATE_DATA_TAG:
					{	
						//接收到的是update，处理收到的update 扔到m_in_buffer中

						handle_update_data(recv_buf, count);
						break;
					}

					//收到控制节点发的消息，分别控制迭代的开始和结束
					case GRAPH_CONTROLL_TAG:
					{
						
						assert(status.MPI_SOURCE == MASTER_RANK);//控制节点
						int msg_id = get_message_id(recv_buf, count);
						/*#ifdef MY_DEBUG
						LOG_TRIVIAL(info) << "worker(" << m_rank
							<< ") received controll msg, msg id: "<<msg_id;
						#endif */

						if (msg_id < 0 || (msg_id >= 0 && msg_id <1000)) {
							break;
						}
						
						//可以作为一轮迭代开始标志
						if (msg_id == MASTER_PERMIT_START_MSGID) {
							//LOG_TRIVIAL(warn) << "expect a permit start message";
							//开始进行一轮迭代,迭代完成后向控制节点发消息
							//TODO
							#ifdef MY_DEBUG
							LOG_TRIVIAL(info) << "worker(" << m_rank 
								<< ") is permitted to start";
							#endif 

							m_start_time = clock();
							
							//开一个线程，不断的往外发update
							//当m_out_buffer over的时候，先计算时间，再向控制节点报告
							//然后控制节点就会发一个改变计算节点状态的消息

							//迭代一次
							
							//下面几行程序顺序不要换
							m_algorithm->reset_all();
							graph_thrd = new std::thread(m_f_algorithm);
							send_thrd = new std::thread(m_f_send);
							//LOG_TRIVIAL(info) << "worker(" << m_rank << ") algorithm info";
							//m_algorithm->show_graph_info();
							
							
							
							//LOG_TRIVIAL(info) << "worker(" << m_rank << ") end iteration";
							//while (1) {}
						}
						else if (msg_id == MASTER_CHANGE_WORKER_STATE_MSGID) {
						//可以作为一轮迭代的结束标志
							//等待结束，释放new出来的变量
							LOG_TRIVIAL(info) << "worker("
								<< m_rank << ")[IN_ITERATION] recv change state msg";
							if (send_thrd->joinable()) {
								send_thrd->join();
								delete send_thrd;
								send_thrd = NULL;
							}
							if (send_thrd != NULL) {
								delete send_thrd;
							}

							//使得图计算程序能结束
							m_algorithm->setover_in_buffer();

	

							if (graph_thrd->joinable()) {
								graph_thrd->join();
								delete graph_thrd;
								graph_thrd = NULL;
							}
							
							if (graph_thrd != NULL) {
								delete graph_thrd;
							}

							m_algorithm->reset_all();
							//LOG_TRIVIAL(info) << "worker(" 
								//<< m_rank << ")[IN_ITERATION] recv change state msg";
							
							master_change_worker_state_msg msg;
							msg.load(std::string((char *)recv_buf, count));
							//设置状态
							set_current_state((NODE_STATE)msg.get_state_index());
							
							//等结束
							
							next = false;
						}
						break;
					}

					default:
					{
						LOG_TRIVIAL(warn) <<"worker("<<m_rank
							<<")[IN_ITERATION]not an expected message."
							<<" TAG "<< status.MPI_TAG;
						break;
					}
				}
			}
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") out IN_ITERATION";
			#endif
			break;
		}

		//两轮迭代之间
		case NODE_STATE::BETWEEN_TWO_ITERATION:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in BETWEEN_TWO_ITERATION";
			#endif

			if (skip_interation == false) {
				//将运行信息发给master节点。
				worker_runtime_info_msg *msg
					= new worker_runtime_info_msg();
				msg->set_worker_id(m_rank);
				msg->set_current_loop(m_algorithm->get_current_step());
				//msg->set_current_loop(5);
				//msg->set_max_loop(10);

				msg->set_run_time(((double)(m_end_time - m_start_time)) / CLOCKS_PER_SEC);

				send_message_to_master(msg, GRAPH_CONTROLL_TAG, MASTER_RANK);
				m_start_time = -1;
				m_end_time = -1;
			}
			bool next = true;
			while (next) {
				//接收消息
				MPI_Recv((void *)recv_buf,
					sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
					MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
				//处理数据
				int count;  //接收的数据量
				MPI_Get_count(&status, MPI_BYTE, &count);

				switch (status.MPI_TAG)
				{
					//迭代之间接收的控制消息，控制计算节点的分裂等等
					//计算节点分裂时，计算节点接收控制节点发过来的分裂消息
					//计算节点完成分裂后就告知控制节点，分裂期间，计算节点只与要分裂的节点
					//通信，等分裂完了就告知控制节点，控制节点更新全局ring环信息
					case GRAPH_CONTROLL_TAG:
					{
						assert(status.MPI_SOURCE == MASTER_RANK);//控制节点
						int msg_id = get_message_id(recv_buf, count);

						if (msg_id < 0 || (msg_id >= 0 && msg_id <= 999)) {
							continue;
						}

						if (msg_id == MASTER_BINARY_PARTITION_WORKER_MSGID) {
							//收到master发过来的二分消息
							binay_partition_myself();
							//LOG_TRIVIAL(info) << "worker("
							//	<< m_rank << ") binay_partition_myself ok";

							//序列化
							std::string ring_info = m_ring->save();
							worker_send_ring_info_msg *msg
								= new worker_send_ring_info_msg();
							msg->set_worker_id(m_rank);
							msg->set_ring_info(ring_info);
							send_message_to_master(msg, HASH_INFO_TAG, MASTER_RANK);
							//LOG_TRIVIAL(info) << "worker(" 
							//	<< m_rank << ") sent ring info to master";
						}
						else if (msg_id == MASTER_CHANGE_WORKER_STATE_MSGID) {
							//可以作为一轮迭代开始标志
							master_change_worker_state_msg msg;
							msg.load(std::string((char *)recv_buf, count));
							//设置状态
							set_current_state((NODE_STATE)msg.get_state_index());
							next = false;
						}
						break;
					}
					case HASH_INFO_TAG:
						//更新本机信息
						/*if(m_rank == 2){
							LOG_TRIVIAL(info) << "result[0]:"
								<< m_algorithm->get_result()[0]
								<< " result[1]:"
								<< m_algorithm->get_result()[1]
								<<  "result[2]:"
								<< m_algorithm->get_result()[2]
								<< " degree[0]:"
								<< m_algorithm->get_degree()[0]
								<< " degree[1]:"
								<< m_algorithm->get_degree()[1]
								<< " degree[2]:"
								<< m_algorithm->get_degree()[2];
						}*/
						handle_hash_info_data(recv_buf, count);
						m_algorithm->load_graph(m_partition_filename);
						/*if (m_rank == 2) {
							LOG_TRIVIAL(info) << "result[0]:"
								<< m_algorithm->get_result()[0]
								<< " result[1]:"
								<< m_algorithm->get_result()[1]
								<< "result[2]:"
								<< m_algorithm->get_result()[2]
								<< " degree[0]:"
								<< m_algorithm->get_degree()[0]
								<< " degree[1]:"
								<< m_algorithm->get_degree()[1]
								<< " degree[2]:"
								<< m_algorithm->get_degree()[2];
						}*/
						//m_algorithm->init();
						//m_algorithm->show_graph_info();
						break;
					default:
					{
						LOG_TRIVIAL(warn) << "[BETWEEN_TWO_ITERATION]not an expected message";
						break;
					}
				}
				}
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") out BETWEEN_TWO_ITERATION";
			#endif
			break;
		}
		//完成迭代
		case NODE_STATE::FINISH_ITERATION:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in FINISH_ITERATION";
			#endif
			m_algorithm->output();
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") output ok";
			bool next = true;
			while (next) {
				//接收消息
				MPI_Recv((void *)recv_buf,
					sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
					MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
				//处理数据
				int count;  //接收的数据量
				MPI_Get_count(&status, MPI_BYTE, &count);

				assert(status.MPI_SOURCE == MASTER_RANK);
				switch (status.MPI_TAG)
				{

				case END_TAG:
				{
					set_current_state(NODE_STATE::FINISH_ALL);
					LOG_TRIVIAL(info) << "worker(" << m_rank << ") received end";
					m_algorithm->clear();
					next = false;
					break;
				}
				default:
					LOG_TRIVIAL(warn) << "[BEFORE_START]not an expected message";
					break;
				}
			}

			//set_current_state(NODE_STATE::FINISH_ALL);
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") out FINISH_ITERATION";
			#endif
			break;
		}
		case NODE_STATE::FINISH_ALL:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in FINISH_ALL";
			#endif
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") out FINISH_ALL";
			#endif
			go_on = false;
			break;
		}
		}
	}
	delete[] recv_buf;
}

template<typename update_type>
inline int worker<update_type>::get_message_id(ecgraph::byte_t * buf, int len)
{
	ptree pt;
	std::stringstream ss;
	int msg_id = -1;
	ss << std::string((char *)buf, len);
	read_json(ss, pt);
	try {
		msg_id = pt.get<int>("msg_id");
	}
	catch (boost::property_tree::ptree_bad_path) {
		LOG_TRIVIAL(error) << "bad message because no field named msg_id";
	}
	return msg_id;
}


//收到这条消息之后开始进行迭代
template<typename update_type>
void worker<update_type>::handle_message(master_permit_start_msg & msg)
{
	#ifdef MY_DEBUG
	LOG_TRIVIAL(info) << "received message master_permit_start_msg";
	#endif
	set_current_state(NODE_STATE::IN_ITERATION);

	//返回循环，开始接收从其他计算节点发来的update;
}

//暂时还没有任何屁用
template<typename update_type>
inline void worker<update_type>::handle_message(master_end_all_msg & msg)
{
	#ifdef MY_DEBUG
	LOG_TRIVIAL(info) << "received message master_end_all_msg";
	#endif
	
	set_current_state(NODE_STATE::FINISH_ALL);
}

#endif