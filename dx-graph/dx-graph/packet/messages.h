#ifndef _BASE_MESSAGE_H_
#define _BASE_MESSAGE_H_
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/date_time.hpp>


#include <string>
#include <cassert>

#include "utils/types.h"
#include "core/graph_define.h"
#include "utils/log_wrapper.h"

using namespace boost::property_tree;
using namespace boost::gregorian;
using namespace boost;

#define COMPUTE_NODE_RUNTIME_INFO_MSGID 0
#define CONTROLLER_PERMIT_START_MSGID 1000
#define CONTROLLER_END_ALL_MSGID 1001
#define COMPUTE_NODE_STOP_SEND_UPDATE_MSGID 2
#define CONTROLLER_CHANGE_COMPUTE_NODE_STATE_MSGID 1002
#define CONTROLLER_SEND_MAX_LOOP_MSGID 1003
#define CONTROLLER_END_ONE_ITERATION_MSGID 1004

class base_message {
protected:
	int m_msg_id;
	ptree pt;

public:
	base_message(){}
	virtual void load(std::string msg) {
		std::stringstream ss;
		ss << msg;
		read_json(ss, pt);
		try {
			m_msg_id = pt.get<int>("msg_id");
		}
		catch(boost::property_tree::ptree_bad_path){
			LOG_TRIVIAL(error) << "the message is illegal, "
								<< "no field named msg_id";
		}

		try {
			ptree content = pt.get_child("content");
		}
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "the message is illegal, "
								<< "no field named content";
		}
	}
	virtual std::string serialize() = 0;
};

//////////////////////////////////////////////////////////////////////
class compute_node_runtime_info_msg: public base_message{
private:
	ecgraph::vertex_t m_compute_node_id;
	double m_runtime;
	//int m_max_loop;
	int m_current_loop;
public:
	compute_node_runtime_info_msg() {
		m_msg_id = COMPUTE_NODE_RUNTIME_INFO_MSGID;
	}
	//从json 字符串中load
	void load(std::string msg){
		base_message::load(msg);
		assert(COMPUTE_NODE_RUNTIME_INFO_MSGID == m_msg_id);
		try {
			m_compute_node_id = pt.get<ecgraph::vertex_t>("content.compute_node_id");
			
		}
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[compute_node_runtime_info_msg] illegal message"
								<< "no field named content.compute_node_id";
		}
		try {
			m_runtime = pt.get<double>("content.runtime");

		}
		/*catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[compute_node_runtime_info_msg] illegal message"
				<< "no field named content.runtime";
		}
		try {
			m_max_loop = pt.get<int>("content.max_loop");

		}*/
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[compute_node_runtime_info_msg] illegal message"
				<< "no field named content.max_loop";
		}

		try {
			m_current_loop = pt.get<int>("content.current_loop");

		}
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[compute_node_runtime_info_msg] illegal message"
				<< "no field named content.current_loop";
		}
		
	}
	
	//get 和 set
	//=============================================================
	ecgraph::vertex_t get_compute_node_id() {
		return m_compute_node_id;
	}
	void set_compute_node_id(ecgraph::vertex_t compute_node_id) {
		m_compute_node_id = compute_node_id;
	}

	ecgraph::vertex_t get_msg_id() {
		return m_msg_id;
	}

	double get_runtime() {
		return m_runtime;
	}
	void set_run_time(double runtime) {
		m_runtime = runtime;
	}

	/*int get_max_loop() {
		return m_max_loop;
	}
	void set_max_loop(int max_loop) {
		m_max_loop = max_loop;
	}*/

	int get_current_loop() {
		return m_current_loop;
	}
	void set_current_loop(int current_loop) {
		m_current_loop = current_loop;
	}

	//==============================================================

	//序列化
	std::string serialize() {

		//=======填充
		pt.put("msg_id", std::to_string(m_msg_id));
		pt.put("content.compute_node_id", std::to_string(m_compute_node_id));
		pt.put("content.runtime", std::to_string(m_runtime));
		//pt.put("content.max_loop", std::to_string(m_max_loop));
		pt.put("content.current_loop", std::to_string(m_current_loop));
		//=======

		std::stringstream ss;
		write_json(ss, pt);
		return ss.str();
	}
};


////////////////////////////////////////////////////////////////////////////
class controller_permit_start_msg : public base_message {
private:
	ecgraph::vertex_t m_controller_id;
public:
	controller_permit_start_msg(){
		m_msg_id = CONTROLLER_PERMIT_START_MSGID;
	}
	//从json 字符串中load
	void load(std::string msg) {
		base_message::load(msg);
		assert(CONTROLLER_PERMIT_START_MSGID == m_msg_id);
		try {
			m_controller_id = pt.get<ecgraph::vertex_t>("content.controller_id");

		}
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[controller_permit_start_msg] illegal message, "
				<< "no field named content.controller_id";
		}
	}

	//get 和 set
	//=============================================================
	ecgraph::vertex_t get_controller_id() {
		return m_controller_id;
	}
	void set_controller_id(ecgraph::vertex_t controller_id) {
		m_controller_id = controller_id;
	}

	ecgraph::vertex_t get_msg_id() {
		return m_msg_id;
	}
	//==============================================================

	//序列化
	std::string serialize() {

		//=======填充
		pt.put("msg_id", std::to_string(m_msg_id));
		pt.put("content.controller_id", std::to_string(m_controller_id));
		//=======

		std::stringstream ss;
		write_json(ss, pt);
		return ss.str();
	}
};
///////////////////////////////////////////////////////////////////////////////
class controller_end_all_msg : public base_message {
private:
	ecgraph::vertex_t m_controller_id;
public:
	controller_end_all_msg() {
		m_msg_id = CONTROLLER_END_ALL_MSGID;
	}
	//从json 字符串中load
	void load(std::string msg) {
		base_message::load(msg);
		assert(CONTROLLER_END_ALL_MSGID == m_msg_id);
		try {
			m_controller_id = pt.get<ecgraph::vertex_t>("content.controller_id");

		}
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[controller_end_all_msg] illegal message, "
				<< "no field named content.controller_id";
		}
	}

	//get 和 set
	//=============================================================
	ecgraph::vertex_t get_controller_id() {
		return m_controller_id;
	}
	void set_controller_id(ecgraph::vertex_t controller_id) {
		m_controller_id = controller_id;
	}

	ecgraph::vertex_t get_msg_id() {
		return m_msg_id;
	}
	//==============================================================

	//序列化
	std::string serialize() {

		//=======填充
		pt.put("msg_id", std::to_string(m_msg_id));
		pt.put("content.controller_id", std::to_string(m_controller_id));
		//=======

		std::stringstream ss;
		write_json(ss, pt);
		return ss.str();
	}
};
///////////////////////////////////////////////////////////////////////////////
class compute_node_stop_send_update_msg : public base_message {
private:
	ecgraph::vertex_t m_compute_node_id;
public:
	compute_node_stop_send_update_msg() {
		m_msg_id = COMPUTE_NODE_STOP_SEND_UPDATE_MSGID;
	}
	//从json 字符串中load
	void load(std::string msg) {
		base_message::load(msg);
		assert(COMPUTE_NODE_STOP_SEND_UPDATE_MSGID == m_msg_id);
		try {
			m_compute_node_id = pt.get<ecgraph::vertex_t>("content.compute_node_id");

		}
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[compute_node_stop_send_update_msg] illegal message, "
				<< "no field named content.compute_node_id";
		}
	}

	//get 和 set
	//=============================================================
	ecgraph::vertex_t get_compute_node_id() {
		return m_compute_node_id;
	}
	void set_compute_node_id(ecgraph::vertex_t compute_node_id) {
		m_compute_node_id = compute_node_id;
	}

	ecgraph::vertex_t get_msg_id() {
		return m_msg_id;
	}
	//==============================================================

	//序列化
	std::string serialize() {

		//=======填充
		pt.put("msg_id", std::to_string(m_msg_id));
		pt.put("content.compute_node_id", std::to_string(m_compute_node_id));
		//=======

		std::stringstream ss;
		write_json(ss, pt);
		return ss.str();
	}
};
///////////////////////////////////////////////////////////////////////////////
class controller_change_compute_node_state_msg : public base_message {
private:
	ecgraph::vertex_t m_controller_id;
	int m_state_index;
public:
	controller_change_compute_node_state_msg() {
		m_msg_id = CONTROLLER_CHANGE_COMPUTE_NODE_STATE_MSGID;
	}
	//从json 字符串中load
	void load(std::string msg) {
		base_message::load(msg);
		assert(CONTROLLER_CHANGE_COMPUTE_NODE_STATE_MSGID == m_msg_id);
		try {
			m_controller_id = pt.get<ecgraph::vertex_t>("content.controller_id");

		}
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[controller_end_all_msg] illegal message, "
				<< "no field named content.controller_id";
		}
		try {
			m_state_index = pt.get<ecgraph::vertex_t>("content.state_index");

		}
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[controller_end_all_msg] illegal message, "
				<< "no field named content.state_index";
		}
	}

	//get 和 set
	//=============================================================
	ecgraph::vertex_t get_controller_id() {
		return m_controller_id;
	}
	void set_controller_id(ecgraph::vertex_t controller_id) {
		m_controller_id = controller_id;
	}

	ecgraph::vertex_t get_msg_id() {
		return m_msg_id;
	}

	int get_state_index() {
		return m_state_index;
	}

	void set_state_index(int state_index) {
		m_state_index = state_index;
	}

	//==============================================================

	//序列化
	std::string serialize() {

		//=======填充
		pt.put("msg_id", std::to_string(m_msg_id));
		pt.put("content.controller_id", std::to_string(m_controller_id));
		pt.put("content.state_index", std::to_string(m_state_index));
		//=======

		std::stringstream ss;
		write_json(ss, pt);
		return ss.str();
	}
};
///////////////////////////////////////////////////////////////////////////////

class controller_send_max_loop_msg : public base_message {
private:
	ecgraph::vertex_t m_controller_id;
	int m_max_loop;
public:
	controller_send_max_loop_msg() {
		m_msg_id = CONTROLLER_SEND_MAX_LOOP_MSGID;
	}
	//从json 字符串中load
	void load(std::string msg) {
		base_message::load(msg);
		assert(CONTROLLER_SEND_MAX_LOOP_MSGID == m_msg_id);
		try {
			m_controller_id = pt.get<ecgraph::vertex_t>("content.controller_id");

		}
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[controller_end_all_msg] illegal message, "
				<< "no field named content.controller_id";
		}
		try {
			m_max_loop = pt.get<int>("content.max_loop");

		}
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[controller_end_all_msg] illegal message, "
				<< "no field named content.max_loop";
		}
	}

	//get 和 set
	//=============================================================
	ecgraph::vertex_t get_controller_id() {
		return m_controller_id;
	}
	void set_controller_id(ecgraph::vertex_t controller_id) {
		m_controller_id = controller_id;
	}

	ecgraph::vertex_t get_msg_id() {
		return m_msg_id;
	}

	int get_max_loop() {
		return m_max_loop;
	}

	void set_max_loop(int state_index) {
		m_max_loop = state_index;
	}

	//==============================================================

	//序列化
	std::string serialize() {

		//=======填充
		pt.put("msg_id", std::to_string(m_msg_id));
		pt.put("content.controller_id", std::to_string(m_controller_id));
		pt.put("content.max_loop", std::to_string(m_max_loop));
		//=======

		std::stringstream ss;
		write_json(ss, pt);
		return ss.str();
	}
};
///////////////////////////////////////////////////////////////////////////////

class controller_end_one_iteration_msg : public base_message {
private:
	ecgraph::vertex_t m_controller_id;
public:
	controller_end_one_iteration_msg() {
		m_msg_id = CONTROLLER_END_ONE_ITERATION_MSGID;
	}
	//从json 字符串中load
	void load(std::string msg) {
		base_message::load(msg);
		assert(CONTROLLER_END_ONE_ITERATION_MSGID == m_msg_id);
		try {
			m_controller_id = pt.get<ecgraph::vertex_t>("content.controller_id");

		}
		catch (boost::property_tree::ptree_bad_path) {
			LOG_TRIVIAL(error) << "[controller_end_all_msg] illegal message, "
				<< "no field named content.controller_id";
		}
	}

	//get 和 set
	//=============================================================
	ecgraph::vertex_t get_controller_id() {
		return m_controller_id;
	}
	void set_controller_id(ecgraph::vertex_t controller_id) {
		m_controller_id = controller_id;
	}

	ecgraph::vertex_t get_msg_id() {
		return m_msg_id;
	}
	//==============================================================

	//序列化
	std::string serialize() {

		//=======填充
		pt.put("msg_id", std::to_string(m_msg_id));
		pt.put("content.controller_id", std::to_string(m_controller_id));
		//=======

		std::stringstream ss;
		write_json(ss, pt);
		return ss.str();
	}
};
///////////////////////////////////////////////////////////////////////////////
#endif