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
#include "../utils/types.h"
#include "../utils/log_wrapper.h"
#include "../utils/type_utils.h"
namespace format {
	class format_convert {
	private:
		std::string filename;
		std::ifstream snap_file;
		std::ofstream type_file;
		std::ofstream map_file;
		std::ofstream config_file;
		int vertex_num;
		long edge_num;
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

		}
		~format_convert(){
			
			snap_file.close();
			type_file.close();
			map_file.close();
			config_file.close();
		}
		
		void write_config(){
			config_file << "#type\tname\tvertices\tedges\n";
			#ifdef COMPACT_GRAPH
			config_file << "1" <<" "
						<<filename+".bin"<<" "
						<<vertex_num<<" "
						<<edge_num<<std::endl;
			#else
			config_file << "2" <<" "
						<<filename+"_noncompact.bin"<<" "
						<<vertex_num<<" "
						<<edge_num<<std::endl;
			#endif
		}

		void to_type() {
			char line[1024] = {0};
			if (! snap_file){
				LOG_TRIVIAL(error) << "no file is opened";
			}
			format::edge_t edge;
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
				
				auto map_ins = mapper.insert( std::pair <vertex_t, vertex_t>(edge.src, i));
				if (map_ins.second == true){
					map_file << i <<" " << edge.src << std::endl;
					edge.src = i;
					i++;
				}
				else{
					edge.src = map_ins.first->second;
				}
				
				map_ins = mapper.insert( std::pair <vertex_t, vertex_t>(edge.dst, i));
				if (map_ins.second == true){
					map_file << i <<" " << edge.dst << std::endl;
					edge.dst = i;
					i++;
				
				else{
					edge.dst = map_ins.first->second;
				}
				type_file.write( (char *) (&edge), sizeof(format::edge_t));
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
