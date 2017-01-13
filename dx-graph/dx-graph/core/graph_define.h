#ifndef _GRAPH_DEFINE_
#define _GRAPH_DEFINE_
#include "utils/consistent_hash.h"

#ifdef _MSC_VER
#include <Windows.h>
#elif defined(__GNUC__)
#if (__GNU__ >= 4 && __GNUC_MINOR__ >=7)
#include <unistd.h>
#endif
#endif

/*所有的传递的信息都是json*/

//表明这是Update
#define UPDATE_DATA_TAG 0
//表明这是数据
#define GRAPH_DATA_TAG 1
//表明这是hash同步信息，将同步后的ring信息同步到所有的计算节点
#define HASH_INFO_TAG 2
//表明这是图的元数据信息
#define GRAPH_INFO_TAG 3
//图计算过程中的控制信息
#define GRAPH_CONTROLL_TAG 100

//结束消息
#define	END_TAG 1000
#define MASTER_RANK 0 //控制节点



//============改变一下宏定义调节性能========================================================
#define RECV_BUFFER_SIZE 20000000   //每个计算节点接收数据的大小 20M 这个必须以字节为单位计数
#define SEND_BUFFER_SIZE 1000000	//发送缓存的大小 1M   以update大小为单位计数
#define MAX_NONDATA_SIZE 1000	//非数据消息的最大长度 1k  各种消息的最大长度，字节为单位
#define GRAPH_DATA_BUFFER_SIZE 5000000 //图数据缓冲区大小 0.5M  edge_t大小为单位计数
#define READ_GRAPH_DATA_ONCE 1000000 //一次读取的图数据量 0.1M  edge_t大小为单位计数

//worker节点的接收和发送还粗
#define IN_BUFFER_SIZE 5000000 //接收缓存大小 5M update大小为单位
#define OUT_BUFFER_SIZE 5000000 //发送缓冲区大小 5M update大小为单位
//============================================================================================
enum NODE_STATE {				//本机状态
	BEFORE_START,				//还未开始
	DISTRIBUTING_GRAHP,			//分发图数据中
	FINISH_DISTRIBUTED_GRAPH,	//分发完毕，还未开始执行图计算
	IN_ITERATION,				//迭代中
	BETWEEN_TWO_ITERATION,		//在两轮迭代中
	FINISH_ITERATION,			//完成迭代
	FINISH_ALL					//完成所有
};

enum NODE_TYPE {
	MASTER_NODE,
	WORKER_NODE
};
void my_sleep(unsigned long seconds) {
#ifdef _MSC_VER
	Sleep(seconds*1000);
#else
	sleep(seconds);
#endif
}

//定义计算节点运行的信息
typedef struct {
	ecgraph::vertex_t compute_id;
	double runtime;
	int max_loop;
	int current_loop;
} worker_run_info_t;



//控制节点收到消息的定义 json格式
/*
每条消息必须有一个消息标识msg_id
计算节点发往控制节点的msg_id范围为0~999
控制节点发往计算节点的msg_id范围为1000~1999
*/

//计算节点请求开始迭代 msg_id == 0
//内容为worker_id
//worker -> master 计算节点发给控制节点
std::string worker_runtime_info_str =
R"({
		"msg_id" : "0",
		"content":{
			"comute_node_id": "-1",
			"runtime":"-1"',
			"current_loop":"1"
		}
})";

//控制节点允许计算节点开始迭代的请求 msg_id == 1000
//内容为master_id
//master -> worker
std::string master_permit_start_str =
R"({
		"msg_id" : "1000",
		"content":{
			"master_id": "0"	
		}
})";

//msg_id == 1，是计算节点发过来的信息，这条是关于计算节点迭代的一些信息，如一次迭代的
//运行时间，其他的一些负载信息，发给控制节点供控制节点判断
std::string worker_iteration_str = //废弃
R"({
	"msg_id" : "1",
	"content":{
				
	}
})";

//msg_id == 1001，控制节点发送该消息使所有的计算节点结束
std::string master_end_all_str =
R"({
	"msg_id" : "1001",
	"content":{
		"master_id":"0"
	}
})";

//msg_id == 2, 计算节点发给控制节点的消息，该消息在计算节点扫完自己的边后发送
std::string worker_stop_send_update_str =
R"({
	"msg_id" : "2",
	"content":{
		"worker_id":"-1"
	}
})";

//msg_id == 1002, 控制节点发送给计算节点改变状态的消息
std::string master_change_worker_state_str =
R"({
	"msg_id" : "1002",
	"content":{
		"master_id":"-1",
		"state_index": "-1"
	}
})";
//msg_id == 1003， 控制节点发给结算节点，表示最大迭代次数
std::string master_send_max_loop_str =
R"({
	"msg_id" : "1003",
	"content":{
		"master_id":"-1",
		"max_loop": "-1"
	}
})";
//msg_id == 1004
std::string master_end_one_iteration_str =
R"({
	"msg_id" : "1004",
	"content":{
		"master_id":"-1"
	}
})";
#endif