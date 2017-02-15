#include "utils/consistent_hash.h"
#include "utils/graph_data.h"
#include "utils/types.h"
#include "utils/buffer.h"
#include "algorithms/pagerank.h"
#include "core/engine.h"
#include <vector>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/date_time.hpp>
#include <sstream>
#include <thread>
#include <algorithm>
template <typename update_type>
void graph_data_sort(engine<update_type> *algorithm, std::string partition_filename) {

	long long edges_num = algorithm->get_edges_num();
	long long vertices_num = algorithm->get_gobal_graph_vertices_num();
	ecgraph::vertex_t start = algorithm->get_start_vid();
	ecgraph::vertex_t end = algorithm->get_end_vid();

	std::string sort_name = partition_filename + ".part";
	ecgraph::buffer<ecgraph::edge_t> file_buf(GRAPH_DATA_BUFFER_SIZE);

	int edge_num_each_part = READ_GRAPH_DATA_ONCE * 100;
	ecgraph::edge_t  *edge_buf = new ecgraph::edge_t[edge_num_each_part];
	int index = 0;
	file_buf.start_write(partition_filename);
	auto is_smaller = [&start, &end, &vertices_num](const ecgraph::edge_t &a, const ecgraph::edge_t &b) ->bool {
		if (start > end) {
			//当同处于一段的时候
			if (start <= a.src && a.src < vertices_num
				&& start <= b.src && b.src < vertices_num
				|| (0 <= a.src && a.src <= end
					&& 0 <= b.src && b.src <= end)) {
				return a.src < b.src;
			}
			else if (a.src >= start) { return true; }
			else return false;
		}
		else {
			return a.src < b.src;
		}
	};
	while (!file_buf.is_over()) {
		int read_num = file_buf.read(edge_buf, edge_num_each_part);
		std::sort(edge_buf, edge_buf + read_num, is_smaller);
		//std::cout << "read num " << read_num << std::endl;
		std::ofstream part(sort_name + "_" + std::to_string(index++), std::ios::binary);
		part.write((char *)edge_buf, read_num*sizeof(ecgraph::edge_t));
	}
	file_buf.write_join();

	ecgraph::edge_t **read_buf = new ecgraph::edge_t*[3];
	for (int i = 0; i < 2; i++) {
		read_buf[i] = new ecgraph::edge_t[READ_GRAPH_DATA_ONCE];
	}
	read_buf[2] = new ecgraph::edge_t[READ_GRAPH_DATA_ONCE * 2];

	for (int i = index; i != 1; i = (i / 2 + i % 2)) {
		//LOG_TRIVIAL(info) << "i " << i;
		for (int j = 0; j < i; j += 2) {
			//LOG_TRIVIAL(info) << "j " << j<< " i "<<i;
			std::pair<long long, long long> first_flag, second_flag, third_flag;
			first_flag.first = 0;
			first_flag.second = 0;
			second_flag.first = 0;
			second_flag.second = 0;
			third_flag.first = 0;
			third_flag.second = 0;

			std::ofstream out(sort_name + "_tmp");
			std::ifstream in_first, in_second;
			if ((j + 1) == i) {
				//改名就行
				if (rename((sort_name + "_" + std::to_string(j)).c_str(),
					(sort_name + "_" + std::to_string(j / 2)).c_str()) == 0) {
					continue;
				}
				else {
					LOG_TRIVIAL(error) << "error happened while rename file "
						<< sort_name + "_" + std::to_string(j);
				}
			}
			else {
				in_first.open(sort_name + "_" + std::to_string(j));
				if (!in_first) {
					LOG_TRIVIAL(error) << sort_name + "_" + std::to_string(j)
						<< " can not opened for read";
					exit(0);
				}
				in_second.open(sort_name + "_" + std::to_string(j + 1));
				if (!in_second) {
					LOG_TRIVIAL(error) << sort_name + "_" + std::to_string(j + 1)
						<< " can not opened for read";
					exit(0);
				}
			}
			int yu0 = 0, yu1 = 0;
			while (1) {
				//文件读完
				/*if (in_first.eof() && in_second.eof()) {
				break;
				}*/
				if (first_flag.first == first_flag.second) {
					first_flag.first = 0;
					//将余下的复制到read_buf[0]头,只能用memmove
					memmove((char *)(read_buf[0]),
						(char *)(read_buf[0]) + first_flag.second, yu0);

					int read_count = in_first.read((char*)(read_buf[0]) + yu0,
						READ_GRAPH_DATA_ONCE*sizeof(ecgraph::edge_t) - yu0).gcount();

					//std::cout << "0 read count " << read_count << std::endl;
					read_count += yu0;
					first_flag.second = read_count / sizeof(ecgraph::edge_t);
					yu0 = read_count % sizeof(ecgraph::edge_t);
				}

				if (second_flag.first == second_flag.second) {

					second_flag.first = 0;
					//将余下的复制到read_buf[0]头,只能用memmove
					memmove((char *)(read_buf[1]),
						(char *)(read_buf[1]) + second_flag.second, yu1);

					int read_count = in_second.read((char*)(read_buf[1]) + yu1,
						READ_GRAPH_DATA_ONCE*sizeof(ecgraph::edge_t) - yu1).gcount();
					//std::cout << "1 read count " << read_count << std::endl;
					read_count += yu1;
					second_flag.second = read_count / sizeof(ecgraph::edge_t);
					yu1 = read_count % sizeof(ecgraph::edge_t);
				}

				//没有读到任何数据，结束
				if (first_flag.first == first_flag.second
					&& second_flag.first == second_flag.second) {
					break;
				}

				if (first_flag.first == first_flag.second
					&& in_first.eof()) {
					//说明第二个数组还有剩余,将剩下的写入文件
					out.write((char*)(read_buf[1] + second_flag.first),
						sizeof(ecgraph::edge_t)*(second_flag.second - second_flag.first));
					second_flag.first = 0;
					second_flag.second = 0;
					continue;
				}

				if (second_flag.first == second_flag.second
					&&in_second.eof()) {
					//说明第一个数组还有剩余，将剩下的写入文件
					out.write((char*)(read_buf[0] + first_flag.first),
						sizeof(ecgraph::edge_t)*(first_flag.second - first_flag.first));
					first_flag.first = 0;
					first_flag.second = 0;
					continue;
				}



				while (first_flag.first != first_flag.second &&
					second_flag.first != second_flag.second) {

					if (is_smaller(read_buf[0][first_flag.first],
						read_buf[1][second_flag.first])) {
						//std::cout << "smaller" << std::endl;
						read_buf[2][third_flag.second] = read_buf[0][first_flag.first];
						++third_flag.second;
						++first_flag.first;
					}
					else {
						//std::cout << "bigger" << std::endl;
						read_buf[2][third_flag.second] = read_buf[1][second_flag.first];
						++third_flag.second;
						++second_flag.first;
					}
				}
				out.write((char*)(read_buf[2]), sizeof(ecgraph::edge_t)*third_flag.second);
				third_flag.second = 0;
			}
			assert(yu0 == 0 && yu1 == 0);
			in_first.close();
			in_second.close();
			out.close();



			if (remove((sort_name + "_" + std::to_string(j)).c_str()) != 0) {
				LOG_TRIVIAL(error) << "error happened while remove file "
					<< sort_name + "_" + std::to_string(j);
			}
			if (remove((sort_name + "_" + std::to_string(j + 1)).c_str()) != 0) {
				LOG_TRIVIAL(error) << "error happened while remove file "
					<< sort_name + "_" + std::to_string(j + 1);
			}
			if (rename((sort_name + "_tmp").c_str(),
				(sort_name + "_" + std::to_string(j / 2)).c_str()) != 0) {
				LOG_TRIVIAL(error) << "error happened while rename file "
					<< sort_name + "_" + std::to_string(j + 1);
			}
		}
	}
	/*file_buf.reset();
	file_buf.start_write(sort_name + "_0");
	while(!file_buf.is_over()){
	int read_num= file_buf.read(edge_buf, edge_num_each_part);
	if (std::is_sorted(edge_buf, edge_buf + read_num, is_smaller)
	== false) {
	std::cout << "not sorted" << std::endl;
	}
	}
	std::cout << "sorted" << std::endl;*/
	if (rename((sort_name + "_" + std::to_string(0)).c_str(),
		partition_filename.c_str()) != 0) {
		LOG_TRIVIAL(error) << "rename failded while rename "
			<< (sort_name + "_" + std::to_string(0))
			<< " to " << partition_filename;
	}

	delete[] edge_buf;
	for (int i = 0; i < 3; i++) {
		delete[] read_buf[i];
	}
	delete[] read_buf;

}