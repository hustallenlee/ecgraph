#ifndef _ENGINE_
#define _ENGINE_
#include "utils/buffer.h"
#include "utils/config.h"
#include "utils/log_wrapper.h"
#include "utils/types.h"
#include "utils/type_utils.h"
#include "utils/mylock.h"
#include "utils/graph_data.h"
#include "core/graph_define.h"

#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <mpi.h>
#include <functional>
#define DISK_IO_SIZE 5000000   //5M
#define UPDATE_BUFFER_SIZE 10000 //10k

template <class update_type >
class update_array: public std::vector<update_type >{
private:

public:
	update_array(){}
	
	virtual void push_update( update_type update){

		this -> push_back(update);
	}
	
};

//inherit the class judgement
/*template <class update_type>
class contrete_judgement:public judgement<update_type>{
private:
	
public:
	contrete_judgement(std::string ini_filename):judgement<update_type>(ini_filename) {}
	virtual int test(update_type update ){
		if ((this -> min_id <= update.id) && (this -> max_id >= update.id)){
			return 1;
		}
		else{
			return 0;
		}
	}
};*/



//
//这个类是图计算的主要实现，用户必须继承它 
//并实现 scatter(), gather(), output()
//
template <class update_type>
class engine {
protected:
	
	//最大的迭代次数·
	//int m_max_loop;

	//记录当前的迭代次数
	int m_step_counter;

	//std::string config_file;
	
	//edge list 文件名
	std::string m_filename;

	//a disk buffer, that one thread can read the file and writes
	//the bytes into the buffer, another thread can read bytes
	//from the buffer 磁盘IO缓存
	ecgraph::buffer<ecgraph::edge_t> *m_disk_io;

	//图结点数
	ecgraph::vertex_t m_vertices_num;

	//====================图相关信息======
	//图边数
	long long m_edge_num;
	//图的类型
	int m_type;
	//图边的大小
	int m_edge_size;
	//图分区的起始结点id
	ecgraph::vertex_t m_partition_start_vid;
	//图分区结束结点id
	ecgraph::vertex_t m_partition_end_vid;
	//图分区中间节点
	ecgraph::vertex_t m_partition_mid_vid;
	//图分区结点数量
	ecgraph::vertex_t m_partition_vertices_num;
	//===================================
	
	ecgraph::edge_t *m_edge_buffer;
	int m_edge_buffer_offset;
	int m_edge_buffer_start;

	//update_array<update_type > ua;

	//update_buf is a buffer which update belongs to local machine 
	//ecgraph::update_stream<update_type > *update_buf;
	//ecgraph::buffer<update_type> *update_buf;
	//trans is used to tansport update from network or local to local or netowrk 
	//transportation<update_type > * trans;

	//judge a vertex belongs to local or not
	//contrete_judgement<update_type > *ju;
	//scatter thread
	std::thread *m_scatter_thrd;

	//gather thread
	std::thread *m_gather_thrd;

	bool m_gather_return;
	//unsigned long m_min_id;

	//update发送缓冲区
	ecgraph::buffer<update_type> *m_out_buffer;

	//update 接收缓冲区
	ecgraph::buffer<update_type> *m_in_buffer;
	ecgraph::graph_data *m_graphdata_file;
	std::function<void(void)> m_f_scatter;
	std::function<void(void)> m_f_gather;
	update_type *m_out_update_buffer;
	update_type *m_in_update_buffer;
	int m_in_update_buffer_offset;
	int m_in_update_buffer_start;

	int m_out_update_buffer_offset;
	//为true则表示不会有update了
	bool m_no_more_update;
	//long long m_updated_num;

public:
	
	engine() {
		//超步计数
		m_step_counter = 0;
		//边的大小
		m_edge_size = sizeof(ecgraph::edge_t);
		
		//offset为输入update缓冲区的offset,用来标记里面存储的update数量
		m_in_update_buffer_offset = 0;
		m_in_update_buffer_start = 0;


		//输出缓冲区的存的update的偏移
		m_out_update_buffer_offset = 0;

		//边缓冲区中位置的偏移
		m_edge_buffer_offset = 0;
		m_edge_buffer_start = 0;
		
		//disk buffer
		m_disk_io = new ecgraph::buffer<ecgraph::edge_t>(DISK_IO_SIZE);
		//接收缓冲区
		m_in_buffer = new ecgraph::buffer<update_type>(IN_BUFFER_SIZE);

		//发送缓冲区
		m_out_buffer = new ecgraph::buffer<update_type>(OUT_BUFFER_SIZE);
		//m_out_buffer = m_in_buffer;


		m_out_update_buffer = new update_type[UPDATE_BUFFER_SIZE];
		m_in_update_buffer = new update_type[UPDATE_BUFFER_SIZE];
		m_edge_buffer = new ecgraph::edge_t[READ_GRAPH_DATA_ONCE];

		m_f_scatter = std::bind(&engine<update_type>::scatter_wrapper, this);
		m_f_gather = std::bind(&engine<update_type>::gather_wrapper, this);
		m_scatter_thrd = NULL; 
		m_gather_thrd = NULL;
		m_gather_return = false;
		m_no_more_update = false;


		m_graphdata_file = NULL;

		//m_updated_num = 0;
		#ifdef MY_DEBUG
		//LOG_TRIVIAL(info)<<"offset "<<m_min_id;
		#endif
		//scatter_return = false;
	} 
	
	void load_graph(std::string fn) {


		//the path of the edge list file
		m_filename = fn;

		//从命令行读取的最大迭代次数
		//m_max_loop = mloop;

		
		
		
		//图数据文件============================================
		if (m_graphdata_file != NULL) {
			delete m_graphdata_file;
			m_graphdata_file = NULL;
		}
		m_graphdata_file = new ecgraph::graph_data(m_filename);
		m_type = m_graphdata_file->get_type();
		m_vertices_num = m_graphdata_file->get_vertices_count();
		m_edge_num = m_graphdata_file->get_edges_count();
		m_partition_start_vid = m_graphdata_file->get_partition_start_vid();
		m_partition_end_vid = m_graphdata_file->get_partition_end_vid();
		m_partition_mid_vid = m_graphdata_file->get_partition_mid_vid();

		if (m_partition_end_vid >= m_partition_start_vid) {
			m_partition_vertices_num = m_partition_end_vid - m_partition_start_vid + 1;
		}
		else {
			m_partition_vertices_num = m_vertices_num - m_partition_start_vid 
										+ m_partition_end_vid + 1;
		}
		//=====================================================


		
	}

	virtual ~ engine(){
		if (m_disk_io != NULL) {
			delete m_disk_io;
			m_disk_io = NULL;
		}

		if (m_in_buffer != NULL) {
			delete m_in_buffer;
			m_in_buffer = NULL;
		}
		if (m_out_buffer != NULL) {
			delete m_out_buffer;
			m_out_buffer = NULL;
		}

		if (m_graphdata_file != NULL) {
			delete m_graphdata_file;
			m_graphdata_file = NULL;
		}

		if (m_out_update_buffer != NULL) {
			delete m_out_update_buffer;
			m_out_update_buffer = NULL;
		}

		if (m_in_update_buffer != NULL) {
			delete m_in_update_buffer;
			m_in_update_buffer = NULL;
		}

		if (m_edge_buffer != NULL) {
			delete m_edge_buffer;
			m_edge_buffer = NULL;
		}

	}
	//在计算节点分裂时才会reload
	void reload() {
		//主要是图的元数据的更新
		m_in_update_buffer_offset = 0;
		m_out_update_buffer_offset = 0;
		m_graphdata_file->init(m_filename);
	}


	//图分区相关函数============================

	//这里是得到图分区的结点数
	ecgraph::vertex_t get_graph_vertices_num() {
		return m_partition_vertices_num;
	}
	//这里是得到全图的节点数
	ecgraph::vertex_t get_gobal_graph_vertices_num() {
		return m_vertices_num;
	}
	ecgraph::vertex_t get_local_graph_vertices_offset(ecgraph::vertex_t vid) {
		return (vid + m_vertices_num - m_partition_start_vid) % m_vertices_num;
	}
	ecgraph::vertex_t get_gobal_graph_vid(ecgraph::vertex_t local_id) {
		return (local_id + m_vertices_num + m_partition_start_vid) % m_vertices_num;
	}
	void show_graph_info() {
		LOG_TRIVIAL(info) << "m_type " << m_type;
		LOG_TRIVIAL(info) << "m_vertices_num " << m_vertices_num;
		LOG_TRIVIAL(info) << "m_edge_num " << m_edge_num;
		LOG_TRIVIAL(info) << "m_partition_start_vid " << m_partition_start_vid;
		LOG_TRIVIAL(info) << "m_partition_end_vid " << m_partition_end_vid;
		LOG_TRIVIAL(info) << "m_partition_mid_vid " << m_partition_mid_vid;
		LOG_TRIVIAL(info) << "m_partition_vertices_num " << m_partition_vertices_num;
	}
	//========================================

	bool no_more_update() {
		return m_no_more_update;
	}

	void set_no_more_update() {
		m_no_more_update = true;
	}

	void reset_no_more_update(){
		m_no_more_update = false;
	}

	//go to next super step
	virtual	void next_super_step() {
		m_step_counter ++;
	}
	
	//return current super step
	virtual int super_step(){
		return m_step_counter ;
	}	
	
	//return whether reach the max super step
	/*virtual bool reach_max_step (){
		if (m_step_counter >= m_max_loop){
			return true;
		}
		else{
			return false;
		}
	}
	virtual int get_max_step() {
		return m_max_loop;
	}*/
	virtual int get_current_step() {
		return m_step_counter;
	}
	//return: false means the file has been written fully
	//while true means disk read successful
	/*bool fill_in_buf(int start){
		if(!m_disk_io -> is_over()){
			m_readed_bytes = m_disk_io -> read(m_edge_buf + start, m_byte_num_once);
			m_edge_buf_offset = 0;
			return true;
		}
		else{
			m_readed_bytes = 0;
			m_edge_buf_offset = 0;
			return false;
		}
	}*/

	//return: false means that reach the end of the file 
	//true means that read ok
	virtual bool get_next_edge(ecgraph::edge_t & edge){
		if (m_edge_buffer_offset == m_edge_buffer_start) { //为空
			if (m_disk_io->is_over()) {
				return false;
			}
			else {
				int readed_num;
				//读到的数量为0时, 且缓冲区未结束，重新读
				while ((readed_num
					= m_disk_io->read(m_edge_buffer, READ_GRAPH_DATA_ONCE)) == 0
					&& !m_disk_io->is_over()) {
				}
				if (readed_num == 0) return false;
				m_edge_buffer_start = 0;
				m_edge_buffer_offset = readed_num;
				edge = m_edge_buffer[m_edge_buffer_start++];
			}
		}
		else {
			edge = m_edge_buffer[m_edge_buffer_start++];
		}
		return true;
	}


	void set_convergence(){
		m_gather_return = true;
	}
	
	bool is_convergence(){
		return m_gather_return;
	}
	

	//只能一个线程访问, 把update扔到m_out_buffer中
	virtual void add_update(const update_type &update, bool immediate){
		//update_buf -> add_update(update);

		//这里有一个本地的缓冲区，用来暂存scather线程的产生的update
		//等到一定数量之后或扫边完成之后一起放到m_out_buffer中
		//这样会减小锁的开销
		m_out_update_buffer[m_out_update_buffer_offset++] = update;
		
		//无边可以获取，且缓冲区中也没边，直接写入，不会再产生update
		/*if (m_disk_io->is_over() && m_edge_buffer_offset == m_edge_buffer_start) {
			if (!m_out_buffer->push(m_out_update_buffer, m_out_update_buffer_offset)) {
				LOG_TRIVIAL(error) << "engine m_out_buffer should be reset";
				exit(0);
			}
			m_out_update_buffer_offset = 0;  //重置
			m_out_buffer->set_over();	//m_out_buffer也不会新增边了
										//设置m_out_buffer为不会新增加边的状态
			return;
		}
		else {*/
			if (immediate) { //立即写入
				if (!m_out_buffer->push(m_out_update_buffer, m_out_update_buffer_offset)) {
					LOG_TRIVIAL(error) << "engine m_out_buffer should be reset";
					exit(0);
				}
				m_out_update_buffer_offset = 0;
			}
			else { 
				if (m_out_update_buffer_offset == UPDATE_BUFFER_SIZE) {
					if (!m_out_buffer->push(m_out_update_buffer, UPDATE_BUFFER_SIZE)) {
						LOG_TRIVIAL(error) << "engine m_out_buffer should be reset";
						exit(0);
					}
					m_out_update_buffer_offset = 0;
				}
			}
		//}
	}

	//将所有缓存的update推出
	void push_all_buffered_update() {
		if (!m_out_buffer->push(m_out_update_buffer, m_out_update_buffer_offset)) {
			LOG_TRIVIAL(error) << "engine m_out_buffer should be reset";
			exit(0);
		}

		LOG_TRIVIAL(info) << "compute("<< get_rank() <<") pushed all buffered update";
		m_out_update_buffer_offset = 0;
		m_out_buffer->set_over();
	}
	int get_rank() {
		int count = -1;
		MPI_Comm_rank(MPI_COMM_WORLD, &count);
		return count;
	}

	//只能一个线程访问, 从m_in_buffer中取
	//返回未true表示读取成功，false 则不会再有update了
	virtual bool get_update(update_type &update){
		

		if (m_in_update_buffer_offset == m_in_update_buffer_start) {//为空
			int readed_num;
			if (!m_in_buffer->is_over()) {
				readed_num =
					m_in_buffer->read(m_in_update_buffer, UPDATE_BUFFER_SIZE);
			}
			else {
				return false;
			}
			/*if (readed_num == 0) {
				return false;
			}*/
			m_in_update_buffer_start = 0;
			m_in_update_buffer_offset = readed_num;
		}
		update = m_in_update_buffer[m_in_update_buffer_start++];
		return true;
	}

	//让系统回到迭代初始状态
	void reset_all() {
		//reset m_out_buffer
		m_out_buffer->reset();
		m_in_buffer-> reset();
		m_disk_io->reset();

		m_edge_buffer_offset = 0;
		m_edge_buffer_start = 0;
		m_in_update_buffer_offset = 0;
		m_in_update_buffer_start = 0;
		m_out_update_buffer_offset = 0;

		//m_updated_num = 0;


	}
	void setover_in_buffer() {
		m_in_buffer->set_over();
	}
	ecgraph::buffer<update_type> *  get_out_buffer() {
		return m_out_buffer;
	}
	ecgraph::buffer<update_type> *  get_in_buffer() {
		return m_in_buffer;
	}
	//return means that a computing in a 
	//super step is convergence or not
	//
	virtual void init() = 0;
	virtual void scatter() = 0;
	virtual bool gather() = 0;	
	virtual void output() = 0;
	virtual void clear() = 0;
	virtual void scatter_wrapper() {
		scatter();
		push_all_buffered_update();
	}
	virtual void gather_wrapper() {
		gather();
	}
	//迭代一次
	virtual void iterate_once() {
		LOG_TRIVIAL(info) << "iterator " << super_step();
		//启动读文件线程
		reset_all();
		m_disk_io->start_write(m_filename);
		//LOG_TRIVIAL(info) << "reading file " << m_filename;
		
		//scatter线程
		m_scatter_thrd = new std::thread(m_f_scatter);

		//gather线程
		m_gather_thrd = new std::thread(m_f_gather);

		m_disk_io->write_join();

		if (m_scatter_thrd->joinable()) {
			m_scatter_thrd->join();
		}
		if (m_gather_thrd->joinable()) {
			m_gather_thrd->join();
		}
		next_super_step();
		delete m_scatter_thrd;
		delete m_gather_thrd;
		reset_all();
	}
	
	//run the super step 废弃
	/*virtual void run(){

		while ( !reach_max_step() ){
			LOG_TRIVIAL(info)<<"iterator "<<super_step();
			//启动读文件线程
			reset_all();
			m_disk_io -> start_write(m_filename);

			//scatter线程
			m_scatter_thrd = new std::thread(m_f_scatter);

			//gather线程
			m_gather_thrd = new std::thread(m_f_gather);


			m_disk_io -> write_join();		

			if (m_scatter_thrd->joinable()) {
				m_scatter_thrd->join();
			}
			if (m_gather_thrd->joinable()) {
				m_gather_thrd->join();
			}
			if ( is_convergence() ){
				break;
			}
			next_super_step();
			delete m_scatter_thrd;
			delete m_gather_thrd;
		}
		
		output();
	}*/
	
};
#endif

