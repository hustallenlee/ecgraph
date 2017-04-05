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

//�ṩ�ײ�����ͨ�ų��󣬷�װ�˵ײ����ݴ��䣬������Ϣ


/*���еĴ��ݵ���Ϣ����json*/


template <typename update_type>	//update_type �ṹ����뺬��id��Ա����
class worker {
public:
	worker(int argc,
		char **argv,
		int world_size, //��ǰ��Ⱥ�в���ͼ����ڵ������
		int self_rank,
		/*ecgraph::consistent_hash *ring,
		ecgraph::buffer<update_type> *out_buffer,
		ecgraph::buffer<update_type> *in_buffer*/
		engine<update_type> *algorithm
		);
	worker() = delete;

	//typedef format::vertex_t mpi_transport_datatype_t;

	//ͬ��ring������Ϣ����ring����Ԫ����ͬ�������еļ���ڵ�
	//void sync_ring_info();
	//void send_to_all_node(void * buf, int count, int tag);
	//�ַ�����������Ϣ
	//void sync_partition_info();

	//�ַ�ͼ����
	//void distributing_graph(ecgraph::buffer<ecgraph::edge_t> *graph_data_buffer);

	//�ڵ����з��͸�������
	void send_update();
	void recv_update();
	//����ڵ㲻�ϵĽ������е����ݣ�������ʲô���͵�����
	//���������У�ͼ���ݣ�update��
	//ring����Ϣ��ͼ������Ϣ
	//������Ϣ
	void handle_graph_data(ecgraph::byte_t * buf, int len);
	void handle_update_data(ecgraph::byte_t * buf, int len);
	void handle_hash_info_data(ecgraph::byte_t * buf, int len);
	void handle_graph_info_data(ecgraph::byte_t * buf, int len);
	void handle_graph_controll_data(ecgraph::byte_t * buf, int len);

	void send_message_to_master(base_message * msg,int tag, int master_rank);
	void send_message_to_master(std::string json_msg, int tag, int master_rank);
	
	//��ĳһ��worker������Ϣ
	void send_msg_to_one_worker(base_message *msg, int tag, int worker_rank);
	void send_msg_to_one_worker(std::string json_msg, int tag, int worker_rank);

	//�����Լ�
	void binay_partition_myself();
	void sync_data_to_worker(int worker_rank, int start);
	void set_one_worker_state(NODE_STATE state, int worker_rank);
	void handle_data_sync(ecgraph::byte_t * buf, int len);
	void disk_sort();
	ecgraph::vertex_t get_graph_mid();
	//�������ݣ������Ǹ�������
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


	//=============������������Ϣ===================================
	int get_message_id(ecgraph::byte_t *buf, int len);
	void handle_message(master_permit_start_msg &msg);
	void handle_message(master_end_all_msg &msg);
	//void handle_message();

	//=============������������Ϣend================================



	//=============״̬��غ���======================================
public:
	NODE_STATE get_current_state() {
		return m_node_state;
	}

	void go_to_next_state() {
		ecgraph::scoped_lock lock(*m_state_mutex);
		if (m_node_state != NODE_STATE::FINISH_ALL) {		//����δ���
			m_node_state = static_cast<NODE_STATE>(m_node_state + 1);
		}
	}
	void set_current_state(NODE_STATE state) {
		ecgraph::scoped_lock lock(*m_state_mutex);	//����
		m_node_state = state;

	}
	//=============״̬��غ���======================================
private:
	int m_argc;
	char** m_argv;
	int m_rank;
	int m_size;		//��ǰ������ܴ�С��  ���¼�Ⱥ��ϢʱҪ�������
	int m_world_size;
	NODE_TYPE m_node_type;
	NODE_STATE m_node_state;

	//����Ⱥ��ring����Ϣ, ���¼�Ⱥ��ϢʱҪ�������
	ecgraph::consistent_hash *m_ring;

	//�������ķ��ͺͽ��ջ��棬���ں��ϲ�ͨ��
	ecgraph::buffer<update_type> *m_out_buffer;
	ecgraph::buffer<update_type> *m_in_buffer;

	std::thread *send_thrd;
	std::thread *recv_thrd;
	std::thread *graph_thrd;
	//MPI_Datatype m_update;

	//״̬��
	std::mutex *m_state_mutex;
	std::mutex *m_ring_mutex;//ring �Ļ�����

	//���ڵ�ķ���������Ϣ ���¼�Ⱥ��ϢʱҪ�������
	ecgraph::config *m_partition_config;

	//���ڵ�ķ�������
	std::string m_partition_filename;

	//�����ڵ��rankֵ�� ���¼�Ⱥ��ϢʱҪ�������
	std::vector<ecgraph::vertex_t> m_machines;

	long long m_partition_edges_num;
	long long m_partition_mid_vid;

	//ͼ��һ���֣�����������ͼ�ļ�������ڽ���ͼԪ���ݵ�ʱ�򱻴򿪣�δ��֮ǰΪNULL
	std::fstream *m_graph_partition;

	engine<update_type> *m_algorithm;

	//m_in_buffer �رձ�־
	int m_in_buffer_close_flag;
	clock_t m_start_time;
	clock_t m_end_time;
	std::function<void(void)> m_f_send;
	std::function<void(void)> m_f_algorithm;
};

template <typename update_type>
worker<update_type>::worker(int argc,
	char **argv,
	int world_size, //��ǰ��Ⱥ�в���ͼ����ڵ������
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
	m_node_state = NODE_STATE::BEFORE_START;	//��ʼ��״̬
	m_ring = new ecgraph::consistent_hash();
	//m_out_buffer = out_buffer;
	//m_in_buffer = in_buffer;
	m_algorithm = algorithm;
	m_out_buffer = m_algorithm->get_out_buffer();
	m_in_buffer = m_algorithm->get_in_buffer();

	
	m_state_mutex = new std::mutex();
	m_ring_mutex = new std::mutex();
	//�����ڵ��rankֵ

	

	m_partition_edges_num = 0;
	m_partition_mid_vid = -1;

	m_graph_partition = new std::fstream();
	m_partition_config = new ecgraph::config() ;
	m_size = 0;

	//������ڵ���յ����ƽڵ㷢�Ľ���messageʱ��������Ϊ1
	m_in_buffer_close_flag = -1;

	m_start_time = -1; //��Чֵ
	m_end_time = -1;	//��Чֵ
	m_f_send = std::bind(&worker<update_type>::send_update, this);
	m_f_algorithm = std::bind(&engine<update_type>::iterate_once, m_algorithm);
}


template <typename update_type>
void worker<update_type>::send_update() {

	//���ԣ�������ֻ��������
	assert(m_node_type == NODE_TYPE::WORKER_NODE
		&& m_node_state == NODE_STATE::IN_ITERATION);


	//��ü���ڵ������
	const int WORKER_SIZE = m_machines.size();

	//���ԣ�������Ⱥ���ж�̨����ڵ��һ̨���ƽڵ�
	assert((WORKER_SIZE + 1) == m_size);

	//�����rankֵ�������±��ӳ�䣬�������
	std::map<ecgraph::vertex_t, int> machine_to_index;
	std::map<int, ecgraph::vertex_t> index_to_machine;
	for (int i = 0; i < WORKER_SIZE; i++) {//ӳ��
		machine_to_index[m_machines[i]] = i;
		index_to_machine[i] = m_machines[i];
	}

	//�����ڴ���Ϊ��������
	//ΪʲôҪ��new������Ϊ����ջ�ռ����ޣ����ݴ�update_type�����ǰ��ֽڶ���ġ�
	//����SEND_BUFFER_SIZE��С�Ķ�������
	update_type* read_buf = new update_type[SEND_BUFFER_SIZE];
	update_type** send_buf = new update_type*[WORKER_SIZE];

	for (int i = 0; i < WORKER_SIZE; i++) {	//Ϊ��������ڵ����뻺��ռ�
		send_buf[i] = new update_type[SEND_BUFFER_SIZE];
	}
	//�����ڴ���Ϊ��������end

	//�ӻ������ж�ȡupdate�����ͻ�����
	bool go_on = true;
	int readed_num;					//��ȡ����update������
	MPI_Request *reqs = new MPI_Request[WORKER_SIZE];
	MPI_Status *status = new MPI_Status[WORKER_SIZE];
	//std::ofstream update_out("update_"+std::to_string(m_rank));
	while (go_on) {
		std::vector<int> length(WORKER_SIZE, 0);;	//����ÿ�������������ݴ���ĳ��ȣ���ʼֵΪ0
		
		if (!m_out_buffer->is_over()) { //δ����
			readed_num = m_out_buffer->read(read_buf, SEND_BUFFER_SIZE);
			//update_out.write((char *)read_buf, readed_num*sizeof(update_type));
			//LOG_TRIVIAL(info) << "worker(" << m_rank << ") generates " 
				//<< readed_num << " updates";
			
			//û�ж����κ�update
			if (readed_num == 0) {
				break;
			}
			int buf_index;
			ecgraph::vertex_t node_id;
			std::map<ecgraph::vertex_t, int>::iterator iter;
			for (int i = 0; i < readed_num; i++) {  //������Զ�Ӧ�Ļ�������
				node_id = (*m_ring)(read_buf[i].id);

				//���ҵ�
				if ((iter = machine_to_index.find(node_id)) != machine_to_index.end()) {
					buf_index = iter->second;
					send_buf[buf_index][length[buf_index]] = read_buf[i];
					length[buf_index] ++;//����������++
				}
				else {
					LOG_TRIVIAL(warn)<< "this update "<<read_buf[i].id
									<<" can not find the destination";
				}
			}

			//�������л������е�����
			
			
			//��������
			//my_flag����ȥ������������ڵ��update
			int my_flag = 0;
			//������ڵ�ķ��ͻ�������
			if (machine_to_index.find(m_rank) == machine_to_index.end()) {
				LOG_TRIVIAL(error) << "some bad thing happened,"
									<<" because the self rank does not belong to this cluster";
				return;
			}
			int self_index = machine_to_index[m_rank];
			for (int i = 0; i < WORKER_SIZE; i++) {

				//��ΪҪ�������������ݣ�������
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
			//������������ڵ�Ļ������е�����ֱ��д�뱾�����ջ���
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
			//�ȴ����з��Ͷ����
			MPI_Waitall(WORKER_SIZE - my_flag, reqs, status);
			for (int i = 0; i < (WORKER_SIZE - my_flag); i++) {
				if (status[i].MPI_ERROR != MPI_SUCCESS) {
					LOG_TRIVIAL(error) << "send update error, rank:"
						<< index_to_machine[i] << ", error code "
						<< status[i].MPI_ERROR;
				}
			}
			//�������л������е�����end


			//MPI_Send((void *)buf, readed_size*sizeof(update_type), MPI_BYTE, );
		}
		else {	//�ѽ���
			go_on = false;
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") m_out_buffer is over";
			//����һ����Ϣ�����ƽڵ�֪��
			/*worker_stop_send_update_msg *msg
				= new worker_stop_send_update_msg();
			msg->set_worker_id(m_rank);
			send_message_to_master(msg, master_RANK);
			delete msg;*/
		}
	}
	//�ӻ������ж�ȡupdate�����ͻ�����end

	
	worker_stop_send_update_msg *msg
		= new worker_stop_send_update_msg();
	msg->set_worker_id(m_rank);
	send_message_to_master(msg, GRAPH_CONTROLL_TAG ,MASTER_RANK);
	delete msg;
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") send stop send update msg ok";

	//���ý���ʱ��
	m_end_time = clock();
	//�ͷŻ���
	delete [] read_buf;
	for (int i = 0; i < WORKER_SIZE; i++) {	//ѭ���ͷ����еĿռ�
		delete[] send_buf[i];
	}
	delete[] send_buf;
	delete[] reqs;
	delete[] status;
	//�ͷŻ���end
}

//����update���߳�
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
		//��������
		int count;  //���յ�������
		MPI_Get_count(&status, MPI_BYTE, &count);
	}*/
}


template<typename update_type>
void worker<update_type>::handle_graph_data(ecgraph::byte_t * buf, int len)
{
	//���յĳ��ȱ���Ϊsizeof(update_type)��������
	assert(len%sizeof(ecgraph::edge_t) == 0);
	assert(m_node_type == NODE_TYPE::WORKER_NODE &&
			m_node_state == NODE_STATE::DISTRIBUTING_GRAHP);

	//����in_buffer��
	/*#ifdef MY_DEBUG
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") receiving";
	#endif*/
	ecgraph::edge_t *edge_buf = (ecgraph::edge_t *)(buf);
	int edge_len = len / sizeof(ecgraph::edge_t);

	//��¼�ߵ�����
	m_partition_edges_num += edge_len;

	//д���ļ�
	if (m_graph_partition->is_open()) {
		
		//���յ���ͼ����д��
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

//������յ��ĸ���
template<typename update_type>
void worker<update_type>::handle_update_data(ecgraph::byte_t * buf, int len)
{
	//���յĳ��ȱ���Ϊsizeof(update_type)��������
	assert(len % sizeof(update_type) == 0);
	assert(m_node_type == NODE_TYPE::WORKER_NODE
			&& m_node_state == NODE_STATE::IN_ITERATION);

	//����in_buffer��
	
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
	//���±���ring��������Ϣ
	//TODO
	#ifdef MY_DEBUG
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") recv ring meta data: "
		<< std::string((char *)buf, len);
	#endif

	//���±�����Ԫ������Ϣ
	m_ring->load_from_json(std::string((char *)buf, len));
	m_size = m_ring->worker_size() + 1;
	m_ring->get_workers(m_machines);
}


//�����ڷ���ͼ����ǰ���ո���Ϣ
template<typename update_type>
void worker<update_type>::handle_graph_info_data(ecgraph::byte_t * buf, int len)
{
	assert(m_node_type == NODE_TYPE::WORKER_NODE 
			&& m_node_state == NODE_STATE::BEFORE_START);
	
	#ifdef MY_DEBUG
	LOG_TRIVIAL(info) <<"worker("<< m_rank << ") recv graph meta data: "
						<<std::string((char *)buf, len);
	#endif
	
	//���ַ����м���������Ϣ
	m_partition_config->load_from_string(std::string((char *)buf, len));

	//��ʽ���
	//���������ļ�
	if (!m_partition_config->check()) {
		LOG_TRIVIAL(error) << "rank " << m_rank << " check failed";
		exit(0);
	}

	//�����ļ���
	m_partition_filename = (*m_partition_config)["name"];

	#ifdef MY_DEBUG
	LOG_TRIVIAL(info) <<"worker(" << m_rank<< ") my partition name " << m_partition_filename;
	#endif
	
	if (!m_graph_partition->is_open()) {
		
		//�Զ����ƺ����ģʽ���ļ�
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
	//������ƽڵ㷢�����Ŀ�����Ϣ

	//���Ȼ�ȡmsg_id
	
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
	//�ж�rank�ĺϷ���
	//�����ڵ��rankֵ
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
	//�ڷ��ѹ����У�Ҫ���ѵĵ�worker
	//���������ݼ��������ø�����һ���µ�worker
	//�����µ�ring��Ϣ����һ���µ�worker,
	//����µ�worker��FINISH_DISTRIBUTED_GRAPH״̬��ֱ�ӽ���
	//BETWEEN_TWO_ITERATION״̬����������worker�ִ���ͬһ״̬�ˡ�
	long long mid = std::stoll((*m_partition_config)["partition_mid_vid"]);
	long long start = std::stoll((*m_partition_config)["partition_start_vid"]);
	long long end = std::stoll((*m_partition_config)["partition_end_vid"]);
	
	//���Ȼ�ȡһ���µ�worker
	std::vector<ecgraph::vertex_t> machines;
	m_ring->get_workers(machines);
	std::set<ecgraph::vertex_t> worker_set(machines.begin(), machines.end());
	
	//����Դ����һ�����е�worker, 0Ĭ����master��������
	ecgraph::vertex_t new_worker = MASTER_RANK; //��Чֵ
	for (ecgraph::vertex_t i = 1; i < m_world_size; i++) {
		if (worker_set.find(i) == worker_set.end()) {//�����ڣ����ҵ���
			new_worker = i;
			break;
		}
	}
	if (new_worker == MASTER_RANK) { return; }
	//��������ring����Ϣ
	m_ring->split(std::make_pair((ecgraph::vertex_t)mid, new_worker));

	//���л�
	std::string ring_info = m_ring->save();

	//���͸���worker
	send_msg_to_one_worker(ring_info, HASH_INFO_TAG, new_worker);

	//���ͷ���������Ϣ===============================
	std::fstream my_partition_config(m_partition_filename + ".json", std::ios::in);
	ptree send_pt, pt;
	read_json(my_partition_config, pt);
	send_pt = pt;
	//���ֵ
	send_pt.put("graph_info.partition_id", std::to_string(new_worker));
	std::string new_worker_partition_name = pt.get<std::string>("graph_info.name");
	int pos = new_worker_partition_name.find_last_of('_');
	new_worker_partition_name.erase(pos);
	send_pt.put("graph_info.name", new_worker_partition_name
		+ "_" + std::to_string(new_worker));
	send_pt.put("graph_info.partition_end_vid",	std::to_string(mid));
	send_pt.put("graph_info.partition_start_vid", std::to_string(start));
	send_pt.put("graph_info.partition_mid_vid",	std::to_string(-1));
	//����
	std::stringstream ss;
	write_json(ss,send_pt);
	send_msg_to_one_worker(ss.str(), GRAPH_INFO_TAG, new_worker);
	ss.clear();

	//������Ϻ󣬸ı���״̬ΪDISTRIBUTING_GRAHP���Խ���ͼ����
	set_one_worker_state(NODE_STATE::DISTRIBUTING_GRAHP, new_worker);
	
	//=====================================================================


	//��ʼ����ͼ��������====================================================
	//
	ecgraph::buffer<ecgraph::edge_t> graphdata_buffer(GRAPH_DATA_BUFFER_SIZE);
	ecgraph::edge_t * edge_buffer = new ecgraph::edge_t[READ_GRAPH_DATA_ONCE];
	//����Ϊͼ��������
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
	//���䷢�ͻ��� 0 Ϊ��worker 1Ϊ����
	ecgraph::edge_t* send_buf[2];
	send_buf[0] = new ecgraph::edge_t[SEND_BUFFER_SIZE];
	send_buf[1] = new ecgraph::edge_t[SEND_BUFFER_SIZE];

	graphdata_buffer.start_write(new_partition_name);
	ecgraph::vertex_t dst_rank;
	int my_edges_num = 0; //�ٴη������󱾻�ʣ�µı���
	while (!graphdata_buffer.is_over()) {
		int length[2] = {0};
		int readed_num = graphdata_buffer.read(edge_buffer, READ_GRAPH_DATA_ONCE);
		for (int i = 0; i < readed_num; i++) {
			dst_rank = (*m_ring)(edge_buffer[i].src);
			if (dst_rank == new_worker) {
				//�ŵ�send_buf[1]��
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
		//���ͺ�д��
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
	//������ͼ����֮����������״̬ΪFINISH_DISTRIBUTED_GRAPH
	set_one_worker_state(NODE_STATE::FINISH_DISTRIBUTED_GRAPH, new_worker);

	//�ٷ���һ��ͬ��������������Ϣ�����µ�worker�ĵ�������������workerͬ��
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


	//Ȼ���ٷ�һ���ı�״̬����Ϣ��ʹ�µ�worker��״̬������workerͬ��
	set_one_worker_state(NODE_STATE::BETWEEN_TWO_ITERATION, new_worker);
	//����������Ϣ�����е����е�worker�ʹ���BETWEEN_TWO_ITERATION
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") set new worker("
		<<new_worker<<") BETWEEN_TWO_ITERATION";

	//�����Լ��ķ���������Ϣ
	my_partition_config.close();
	my_partition_config.open(m_partition_filename + ".json", std::ios::out);


	//���µ�һ������
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

	//����m_partition_config
	(*m_partition_config)["partition_start_vid"] = std::to_string(mid+1);	
	(*m_partition_config)["partition_mid_vid"] = std::to_string(my_mid);
	(*m_partition_config)["partition_end_vid"] = std::to_string(end);
	(*m_partition_config)["partition_edges_num"] = std::to_string(my_edges_num);
	//���汾worker��ͼ���������ļ�
	m_partition_config->dump(m_partition_filename+".json");

	//��������degree��result
	int *degree = m_algorithm->get_degree().data();
	decltype(m_algorithm->get_result_value_type()) 
		*result = m_algorithm->get_result().data();

	
	//��������Ŀ�ʼ�ͽ���
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


//start��ʾ�������ʲôλ�ÿ�ʼͬ��
template<typename update_type>
inline void worker<update_type>::sync_data_to_worker(int worker_rank, int end)
{
	//��ʼͬ��degree��result
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
	//��ʱworker��Ϊһ��αmaster�ı�һ����worker��״̬
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
	//��ͼ���ݽ����ⲿ����
	graph_data_sort(m_algorithm, m_partition_filename);
}


//a*|v| + |E|
//aΪͼ��ƽ����
template<typename update_type>
inline ecgraph::vertex_t worker<update_type>::get_graph_mid()
{
	//���ź����ͼ����ȡ�е�


	ecgraph::edge_t  *edge_buf = new ecgraph::edge_t[READ_GRAPH_DATA_ONCE];

	//���ļ���
	std::ifstream graphdata_file(m_partition_filename);
	
	//ecgraph::graph_data gd(argv[1]);

	//���ȫͼ�ı���
	long long edges_num = m_algorithm->get_edges_num();

	//std::cout << "edges_num " << edges_num << std::endl;

	//���ȫͼ�Ľ����
	ecgraph::vertex_t vertices_num = m_algorithm->get_gobal_graph_vertices_num();

	LOG_TRIVIAL(info) << "worker(" << m_rank << ") vertices_num " 
					<< vertices_num;

	//���ͼ�����Ŀ�ʼ���
	ecgraph::vertex_t partition_start_vid = m_algorithm->get_start_vid();

	LOG_TRIVIAL(info) << "worker(" << m_rank << ") partition_start_vid " 
					<< partition_start_vid;

	ecgraph::vertex_t partition_end_vid = m_algorithm->get_end_vid();

	LOG_TRIVIAL(info) << "worker(" << m_rank << ") partition_end_vid "
					<< partition_end_vid;

	//��÷����Ľ����
	ecgraph::vertex_t partition_vertices_num 
		= m_algorithm->get_graph_vertices_num();

	LOG_TRIVIAL(info) << "worker(" << m_rank << ") partition_vertices_num " 
						<< partition_vertices_num;
	
	//���ͼ�����ı���
	long long partition_edges_num = m_algorithm->get_partition_edges_num();
	LOG_TRIVIAL(info) << "worker(" << m_rank << ") partition_edges_num "
					<< partition_edges_num;


	//ȫͼ��ƽ����
	long long average_degree = edges_num / vertices_num;
	
	//���ģʽʱ������Ϊ1����
	average_degree = 1;


	//ȫ������COST
	long long partition_total_cost
		= average_degree * partition_vertices_num + partition_edges_num;
	//���е�Ϊ�ε�����COST
	//std::cout << "here" << std::endl;
	long long expect_cost = partition_total_cost / 2;


	/*std::cout << "avg degree " << average_degree
		<< " partition_total_cost " << partition_total_cost
		<< " expect_cost " << expect_cost << std::endl;*/

	//��ʼ��������ͼ���ҵ�my_mid
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
	//���ԣ���ҪΪ����ڵ�
	assert(m_node_type == NODE_TYPE::WORKER_NODE);

	bool go_on = true;
	ecgraph::byte_t *recv_buf = new ecgraph::byte_t[RECV_BUFFER_SIZE];
	MPI_Status status;
	bool skip_interation = true;
	while (go_on) { //״̬��Ϊδ��ɣ������
		
		switch (get_current_state()) {

			//�ڿ�ʼ֮ǰҪ����ͼԪ���ݷ�����Ϣ��ͼԪ���ݰ���������һ����Ϣ
		case NODE_STATE::BEFORE_START:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in BEFORE_START";
			#endif

			bool next = true;
			while (next) {
				//������Ϣ
				MPI_Recv((void *)recv_buf,
					sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
					MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
					//��������
				int count;  //���յ�������
				MPI_Get_count(&status, MPI_BYTE, &count);

				//assert(status.MPI_SOURCE == MASTER_RANK);
				switch (status.MPI_TAG)
				{
				case HASH_INFO_TAG:
					
					//������յ�ring����Ϣ
					handle_hash_info_data(recv_buf, count);
					break;

				case GRAPH_INFO_TAG:
					
					//������յ�ͼԪ������Ϣ
					handle_graph_info_data(recv_buf, count);
					
					break;
					
				case GRAPH_CONTROLL_TAG: //����״̬��Ϣ
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


		//�ַ�ͼ������
		case NODE_STATE::DISTRIBUTING_GRAHP:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in DISTRIBUTING_GRAHP";
			#endif

			bool next = true;
			while (next) {
				//������Ϣ
				MPI_Recv((void *)recv_buf,
					sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
					MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
				//��������
				int count;  //���յ�������
				MPI_Get_count(&status, MPI_BYTE, &count);
				//assert(status.MPI_SOURCE == MASTER_RANK);//���ƽڵ�
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

					//������������Ϣд�����
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

		//���ͼ���ݵķַ�
		case NODE_STATE::FINISH_DISTRIBUTED_GRAPH:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in FINISH_DISTRIBUTED_GRAPH";
			#endif
			//���ȹص�m_graph_partition
			m_graph_partition->close();
			
			//ʹ�㷨����ͼ����
			m_algorithm->load_graph(m_partition_filename);

			//��ʼ��
			m_algorithm->init();

			//��ͼ���о��⻮�ֵ���ֵ
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
				//������Ϣ
				MPI_Recv((void *)recv_buf,
					sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
					MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
				//��������
				int count;  //���յ�������
				MPI_Get_count(&status, MPI_BYTE, &count);
				//assert(status.MPI_SOURCE == MASTER_RANK);//���ƽڵ�
				switch (status.MPI_TAG)
				{
				case GRAPH_CONTROLL_TAG:
				{	
					int msg_id = get_message_id(recv_buf, count);

					if (msg_id == MASTER_CHANGE_WORKER_STATE_MSGID) {
						master_change_worker_state_msg msg;
						//װ����Ϣ
						LOG_TRIVIAL(info) << "worker(" << m_rank
							<< ") recv change state msg source " << status.MPI_SOURCE;

						msg.load(std::string((char *)recv_buf, count));
						//����״̬
						set_current_state((NODE_STATE)msg.get_state_index());
						next = false;//����
						
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

		//�ڵ�����
		case NODE_STATE::IN_ITERATION:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in IN_ITERATION";
			#endif
			skip_interation = false;
			bool next = true;
			while (next) {
				//������Ϣ
				MPI_Recv((void *)recv_buf,
					sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
					MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
				//��������
				int count;  //���յ�������
				MPI_Get_count(&status, MPI_BYTE, &count);
				switch (status.MPI_TAG)
				{
					case UPDATE_DATA_TAG:
					{	
						//���յ�����update�������յ���update �ӵ�m_in_buffer��

						handle_update_data(recv_buf, count);
						break;
					}

					//�յ����ƽڵ㷢����Ϣ���ֱ���Ƶ����Ŀ�ʼ�ͽ���
					case GRAPH_CONTROLL_TAG:
					{
						
						assert(status.MPI_SOURCE == MASTER_RANK);//���ƽڵ�
						int msg_id = get_message_id(recv_buf, count);
						/*#ifdef MY_DEBUG
						LOG_TRIVIAL(info) << "worker(" << m_rank
							<< ") received controll msg, msg id: "<<msg_id;
						#endif */

						if (msg_id < 0 || (msg_id >= 0 && msg_id <1000)) {
							break;
						}
						
						//������Ϊһ�ֵ�����ʼ��־
						if (msg_id == MASTER_PERMIT_START_MSGID) {
							//LOG_TRIVIAL(warn) << "expect a permit start message";
							//��ʼ����һ�ֵ���,������ɺ�����ƽڵ㷢��Ϣ
							//TODO
							#ifdef MY_DEBUG
							LOG_TRIVIAL(info) << "worker(" << m_rank 
								<< ") is permitted to start";
							#endif 

							m_start_time = clock();
							
							//��һ���̣߳����ϵ����ⷢupdate
							//��m_out_buffer over��ʱ���ȼ���ʱ�䣬������ƽڵ㱨��
							//Ȼ����ƽڵ�ͻᷢһ���ı����ڵ�״̬����Ϣ

							//����һ��
							
							//���漸�г���˳��Ҫ��
							m_algorithm->reset_all();
							graph_thrd = new std::thread(m_f_algorithm);
							send_thrd = new std::thread(m_f_send);
							//LOG_TRIVIAL(info) << "worker(" << m_rank << ") algorithm info";
							//m_algorithm->show_graph_info();
							
							
							
							//LOG_TRIVIAL(info) << "worker(" << m_rank << ") end iteration";
							//while (1) {}
						}
						else if (msg_id == MASTER_CHANGE_WORKER_STATE_MSGID) {
						//������Ϊһ�ֵ����Ľ�����־
							//�ȴ��������ͷ�new�����ı���
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

							//ʹ��ͼ��������ܽ���
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
							//����״̬
							set_current_state((NODE_STATE)msg.get_state_index());
							
							//�Ƚ���
							
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

		//���ֵ���֮��
		case NODE_STATE::BETWEEN_TWO_ITERATION:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in BETWEEN_TWO_ITERATION";
			#endif

			if (skip_interation == false) {
				//��������Ϣ����master�ڵ㡣
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
				//������Ϣ
				MPI_Recv((void *)recv_buf,
					sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
					MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
				//��������
				int count;  //���յ�������
				MPI_Get_count(&status, MPI_BYTE, &count);

				switch (status.MPI_TAG)
				{
					//����֮����յĿ�����Ϣ�����Ƽ���ڵ�ķ��ѵȵ�
					//����ڵ����ʱ������ڵ���տ��ƽڵ㷢�����ķ�����Ϣ
					//����ڵ���ɷ��Ѻ�͸�֪���ƽڵ㣬�����ڼ䣬����ڵ�ֻ��Ҫ���ѵĽڵ�
					//ͨ�ţ��ȷ������˾͸�֪���ƽڵ㣬���ƽڵ����ȫ��ring����Ϣ
					case GRAPH_CONTROLL_TAG:
					{
						assert(status.MPI_SOURCE == MASTER_RANK);//���ƽڵ�
						int msg_id = get_message_id(recv_buf, count);

						if (msg_id < 0 || (msg_id >= 0 && msg_id <= 999)) {
							continue;
						}

						if (msg_id == MASTER_BINARY_PARTITION_WORKER_MSGID) {
							//�յ�master�������Ķ�����Ϣ
							binay_partition_myself();
							//LOG_TRIVIAL(info) << "worker("
							//	<< m_rank << ") binay_partition_myself ok";

							//���л�
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
							//������Ϊһ�ֵ�����ʼ��־
							master_change_worker_state_msg msg;
							msg.load(std::string((char *)recv_buf, count));
							//����״̬
							set_current_state((NODE_STATE)msg.get_state_index());
							next = false;
						}
						break;
					}
					case HASH_INFO_TAG:
						//���±�����Ϣ
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
		//��ɵ���
		case NODE_STATE::FINISH_ITERATION:
		{
			#ifdef MY_DEBUG
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") in FINISH_ITERATION";
			#endif
			m_algorithm->output();
			LOG_TRIVIAL(info) << "worker(" << m_rank << ") output ok";
			bool next = true;
			while (next) {
				//������Ϣ
				MPI_Recv((void *)recv_buf,
					sizeof(ecgraph::byte_t)* RECV_BUFFER_SIZE, MPI_BYTE,
					MPI_ANY_SOURCE, MPI_ANY_TAG,
					MPI_COMM_WORLD, &status);
				//��������
				int count;  //���յ�������
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


//�յ�������Ϣ֮��ʼ���е���
template<typename update_type>
void worker<update_type>::handle_message(master_permit_start_msg & msg)
{
	#ifdef MY_DEBUG
	LOG_TRIVIAL(info) << "received message master_permit_start_msg";
	#endif
	set_current_state(NODE_STATE::IN_ITERATION);

	//����ѭ������ʼ���մ���������ڵ㷢����update;
}

//��ʱ��û���κ�ƨ��
template<typename update_type>
inline void worker<update_type>::handle_message(master_end_all_msg & msg)
{
	#ifdef MY_DEBUG
	LOG_TRIVIAL(info) << "received message master_end_all_msg";
	#endif
	
	set_current_state(NODE_STATE::FINISH_ALL);
}

#endif