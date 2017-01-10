/*
 * dist-xs
 *
 * Copyright 2015 Key Laboratory of Data Storage System, Ministry of Education WNLO HUST
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _LOG_WRAPPER_
#define _LOG_WRAPPER_
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <thread>
#include <chrono>
#include <ctime>

enum log_state {
	fatal = 0,
	error = 1,
	warn = 2,
	info = 3,
	total = 4,
};
static const char * log_header[total]=
{
	"FATAL", "ERROR", "WARNING", "INFO"
};

class LOG_TRIVIAL {
	enum log_state level;
public:
	std::stringstream state;
	LOG_TRIVIAL(enum log_state level_in)
		:level(level_in){}
	
	template<typename T>
	LOG_TRIVIAL & operator<< (T value){
		state<< value;
		return *this;
	}
	const std::string get_current_time(){
		auto now = std::chrono::system_clock::now();
		std::time_t t = std::chrono::system_clock::to_time_t(now);
		std::string ts = std::ctime(&t);
		ts.resize(ts.size()-1);
		return ts;
	}	
	~LOG_TRIVIAL(){
		std::stringstream final;
		final << get_current_time();
		final << "<" <<log_header[level] <<"> ";
		final << state.str() <<std::endl;
		#ifdef LOG
		std::ofstream log_file("./dx.log", std::ios::out | std::ios::app );
		log_file<<final.str();
		#endif
		std::cerr <<final.str();
	}
	

};
#endif
