/*
 * author:      allen lee(Junhao Li) allen_lee922@foxmail.com
 * address:     WNLO, Huazhong University of Science & Technology
 * Time:        2016.11
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

/*
 *  This namespace dx_lib includes somes utils for our project.
 *  Class graph_data is a graph data abstract class
 *
 *
 */
#ifndef _GRAPH_DATA_H_
#define _GRAPH_DATA_H_

#include <sstream>
#include <string>

#include "utils/types.h"
#include "utils/type_utils.h"
#include "log_wrapper.h"
#include "config.h"
namespace ecgraph{
    class graph_data{
		private:

			config *conf;
			std::string m_graphdata_name;
			int m_ntype;
			vertex_t m_nvertices;
			unsigned long long m_nedges;
			vertex_t m_partition_start_vid;
			vertex_t m_partition_end_vid;

			vertex_t m_partition_edges_num;
			vertex_t m_partition_mid_vid;

			vertex_t m_partition_id;
        public:

            typedef vertex_t graph_size_t;
            //构造函数
            explicit graph_data(std::string graphfile_name)
				:m_graphdata_name(graphfile_name){            
				init(m_graphdata_name);
            }
			graph_data(){};

			~graph_data() {
				delete conf;
			}
            //初始化
			void init(std::string graphdata_name) {
				conf = new config(graphdata_name + ".json");
                std::stringstream ss;
                //图类型
                //===========================================
                ss << (*conf)["type"];            //graph type
                ss >> m_ntype;
                ss.clear();
                //===========================================

                //图结点数量
                //================================================
                ss << (*conf)["vertices"];  //graph vertices numbers
                ss >> m_nvertices;
                ss.clear();
                //=================================================
                
                //图中边的数量
                //=================================================
                ss << (*conf)["edges"];
                ss >> m_nedges;        //graph edges numbers
				ss.clear();
                //=================================================

				//分区中中起始结点id
				//=================================================
				ss << (*conf)["partition_start_vid"];
				ss >> m_partition_start_vid;
				ss.clear();
				//=================================================

				//分区中中结束结点id
				//=================================================
				ss << (*conf)["partition_end_vid"];
				ss >> m_partition_end_vid;
				ss.clear();
				//=================================================

				//分区边的数量
				//=================================================
				ss << (*conf)["partition_edges_num"];
				ss >> m_partition_edges_num;
				ss.clear();
				//=================================================

				//分区中将边对半划分的图结点id
				//=================================================
				ss << (*conf)["partition_mid_vid"];
				ss >> m_partition_mid_vid;
				ss.clear();
				//=================================================

				//分区id
				//=================================================
				ss << (*conf)["partition_id"];
				ss >> m_partition_id;
				ss.clear();
				//=================================================

            }

            //get graph type
            int get_type(){
                return m_ntype;
            }
            //get numbers of the graph vertices
			graph_size_t get_vertices_count(){
                return m_nvertices;
            }

            //get numbers of the graph edges
			unsigned long long get_edges_count(){
                return m_nedges;
            }

			vertex_t get_partition_start_vid() {
				return m_partition_start_vid;
			}

			vertex_t get_partition_end_vid() {
				return m_partition_end_vid;
			}

			unsigned long get_partition_edges_num() {
				return m_partition_edges_num;
			}

			vertex_t get_partition_mid_vid() {
				return m_partition_mid_vid;
			}

			int get_partition_id() {
				return m_partition_id;
			}

			std::string save() {
				return conf->save();
			}

			std::string get_graphdata_name() {
				return m_graphdata_name;
			}

			ecgraph::config *get_config_ptr() {
				return conf;
			}
    };    
}
#endif
