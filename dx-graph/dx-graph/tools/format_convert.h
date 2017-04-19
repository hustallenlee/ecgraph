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
#ifndef _FORMAT_CONVERT_
#define _FORMAT_CONVERT_
#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <map>
#include <utility>
#include <algorithm>
#include "utils/types.h"
#include "utils/log_wrapper.h"
#include "utils/type_utils.h"

namespace ecgraph {
	class format_convert {
	private:
		std::string filename;
		std::ifstream snap_file;
		std::ofstream type_file;
		std::ofstream map_file;
		std::ofstream config_file;
		vertex_t vertex_num;
        //vertex_t min_vertex_id;
        //vertex_t max_vertex_id;
		long long edge_num;
	public:
		format_convert(std::string fn):filename(fn){
			snap_file.open(filename, std::ios::in);
			#ifdef COMPACT_GRAPH
			type_file.open(filename + ".bin", 
							std::ios::out | std::ios::binary);

			config_file.open(filename + ".bin.ini", std::ios::out);
			#else
			type_file.open(filename + "_noncompact.bin", 
							std::ios::out | std::ios::binary);

			config_file.open(filename + "_noncompact.bin.ini", std::ios::out);
			#endif	
			map_file.open(filename + ".map",std::ios::out );
			map_file << "#after before" << std::endl;
			vertex_num = 0;
			edge_num = 0;
            //min_vertex_id = ecgraph::VERTEX_MAX;
            //max_vertex_id = ecgraph::VERTEX_MIN;

		}
		~format_convert(){
			
			snap_file.close();
			type_file.close();
			map_file.close();
			config_file.close();
		}
		
		void write_config(){
			config_file << "#type\tname\tvertices\tedges\tmin_vertex_id\tmax_vertex_id\n";
			#ifdef COMPACT_GRAPH
			config_file << "1" <<"\t"
						<<filename+".bin"<<"\t"
						<<vertex_num<<"\t"
						<<edge_num<<"\t"
                        //<<min_vertex_id<<"\t"
                        //<<max_vertex_id<<"\t"
                        <<std::endl;
			#else
			config_file << "2" <<"\t"
						<<filename+"_noncompact.bin"<<"\t"
						<<vertex_num<<"\t"
						<<edge_num<<"\t"
                        //<<min_vertex_id<<"\t"
                        //<<max_vertex_id<<"\t"
                        <<std::endl;
			#endif
		}

		void to_type() {
			char line[1024] = {0};
			if (! snap_file){
				LOG_TRIVIAL(error) << "no file is opened";
			}
			ecgraph::edge_t edge;
			std::map<vertex_t, vertex_t> mapper;
			vertex_t i=0;
			std::cout << sizeof(edge) <<std::endl;
			while(snap_file.getline(line,sizeof(line))){
				std::istringstream record(line);
				record >> edge.src;
				record >> edge.dst;
				#ifdef WEIGHT_EDGE
				edge.value = generate_weight();
				#endif
				
				auto map_ins = mapper.insert( std::pair <vertex_t, vertex_t>(edge.src, i));//转换
				if (map_ins.second == true){
					map_file << i <<" " << edge.src << std::endl;
					edge.src = i;
					i++;
				}
				else{
					edge.src = map_ins.first->second;//map_ins.first代表这个关键字的迭代器
				}
				
				map_ins = mapper.insert( std::pair <vertex_t, vertex_t>(edge.dst, i));
				if (map_ins.second == true){
					map_file << i <<" " << edge.dst << std::endl;
					edge.dst = i;
					i++;
			    }	
				else{
					edge.dst = map_ins.first->second;
				}
				type_file.write( (char *) (&edge), sizeof(ecgraph::edge_t));//write(const unsigned char *buf,int num); write() 从buf 指向的缓存写 num 个字符到文件中，值得注意的是缓存的类型是 unsigned char *，有时可能需要类型转换
				edge_num ++;
			}
			vertex_num = i;
			#ifdef COMPACT_GRAPH
			LOG_TRIVIAL(info) << "snap to compact type successfull";
			#else
			LOG_TRIVIAL(info) << "snap to non-compact type successfull";
			#endif
			write_config();
		}
		weight_t generate_weight(){
			return 1.0;
		}

	};
}
#endif
