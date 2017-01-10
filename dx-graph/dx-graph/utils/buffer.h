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
 *  Class buffer is a class that buffers graph
 *
 *
 */
#ifndef _BUFFER_
#define _BUFFER_
#include <iostream>
#include <fstream>
#include <string>
//#include <boost/thread/thread.hpp>
//#include <boost/thread/mutex.hpp>
//#include <boost/thread/condition.hpp>
#include <thread>
#include <mutex>
#include <string.h>
#include "log_wrapper.h"
#include "mylock.h"
#define BUFFERSIZE 1000*1000*100

//typedef char BYTE;

namespace ecgraph {
    /*template<class elem_type, 
                unsigned int BUFFERSIZE, 
                unsigned int INPUTSIZE, 
                unsigned int OUTPUTSIZE>
    class buffer_c{
    public:
        typedef char BYTE;
        
    private:
        std::vector<elem_type> data;
        std::vector< std::vector<elem_type> > input;//作为一个整体放入buffer中
        std::vector< std::vector<elem_type> > out;//从buffer中作为一个整体取出
        
    public:
        buffer_c(){
            data.resize(BUFFERSIZE);
        }

    }；*/

	//缓冲区类
    //std::mutex buffer_mutex;
	template <class elem_type>
	class buffer {
    public:
	    //boost::mutex buffer_mutex;

	protected:
		elem_type *m_data;    //存放数据数组
		int m_front;          //有效数据头
		int m_rear;           //有效数据尾
		const int m_len;            //缓冲区长度
		int m_over;
		unsigned long m_read_count;
		unsigned long m_write_count;
		//boost::thread *thrd;
        std::thread *m_thrd;
		std::mutex *buffer_mutex;
		//boost::mutex over_flag_mutex;
		int m_elem_size;

	public:
		buffer() :m_len(BUFFERSIZE),
                    m_front(0),
                    m_rear(0),
                    m_over(0),
                    m_read_count(0),
                    m_write_count(0),
                    m_thrd(NULL){
			//std::cout<<"gou 1"<<std::endl;
			m_data = new elem_type[BUFFERSIZE]();
			m_elem_size = sizeof(elem_type);
			buffer_mutex = new std::mutex();
		}

		buffer(unsigned int size):m_len(size),
                                    m_front(0),
                                    m_rear(0),
                                    m_over(0),
                                    m_read_count(0),
                                    m_write_count(0),
                                    m_thrd(NULL){
			m_data = new elem_type[size]();
			m_elem_size = sizeof(elem_type);
			buffer_mutex = new std::mutex();
		}
	
		~buffer(){
			delete [] m_data;
			delete buffer_mutex;
			//delete thrd;
		}

		/*void clear(){
			
			front = 0;
			rear = 0;
			over = 0;
			read_count=0;
			
			delete [] data;
		}*/
		
	private:
		//full states
		bool is_full(){
			
			return ((m_rear + 1) % m_len == m_front);
		}

		/*friend std::ostream &operator<< (std::ostream &os, const buffer & buf){
			boost::mutex::scoped_lock lock(buffer_mutex);
			std::cout<<buf.front<<" "<<buf.rear<<std::endl;
			int i=0;
			for (i = buf.front; i != buf.rear; i = (i+1)%(buf.len))
				std::cout<<buf.data[i]<<" ";
			std::cout<<std::endl;
		}*/
		
		//empty states
		bool is_empty(){
			return (m_front == m_rear);
		}		
			
		//enqueue the buffer
		int enqueue(elem_type *buf, int n){
			int in_num = n;
			
			if ( is_full() ){
				return 0;
			} 
			if (is_empty()){
				in_num = ( (m_len - 1) < n ) ? (m_len - 1) : n;
			}
			else if ( n > (m_len + m_front - m_rear - 1 )%m_len ){ //n is bigger than empty space
				in_num = (m_len + m_front -m_rear - 1)%m_len;
			}
			
			if ( (m_rear + in_num ) <= m_len){
				memcpy((void * )(m_data + m_rear),
                        (void *)buf ,
					in_num * m_elem_size);
			}
			else{
				int temp_num = m_len - m_rear;
				memcpy((void *) (m_data + m_rear),
                        (void *)buf, 
                        temp_num * m_elem_size );
				memcpy((void *)m_data,
                        (void *)(buf + temp_num), 
                        (in_num - temp_num) * m_elem_size );
			}
			
			m_rear = (m_rear + in_num )% m_len;
			return in_num;
		}

		//dequeue the buffer
		int dequeue(elem_type * buf, int n){
			int out_num = n ;
			if(is_empty()){
				return 0;
			}
			if (is_full()){
				out_num = ( (m_len-1) < n )? (m_len -1) : n;
			}
			else if ( n > (m_len + m_rear - m_front )% m_len ){
                //n is bigger than it already has
				out_num = (m_len + m_rear - m_front )% m_len;
			}
			
			if ( (m_front + out_num) <= m_len){
				memcpy((void *)buf,
                        (void *)(m_data + m_front),
                        out_num * m_elem_size);
			}
			else{
				int temp_num = m_len - m_front;
				memcpy((void *)buf, 
                        (void *) (m_data + m_front),
                        temp_num * m_elem_size);

				memcpy((void *)(buf + temp_num), 
                        (void * )m_data,
                        (out_num - temp_num) * m_elem_size);
			}
			m_front = (m_front + out_num) % m_len;
			return out_num;
		}
		
	public:
		bool push(elem_type *block, int num) {
			if (m_over == 1) {
				return false;
			}
			int temp = num;
			int total = num;
			while (temp) {
				ecgraph::scoped_lock lock(*buffer_mutex);	//lock
				int write_num = enqueue(block + total - temp, temp);
				temp = temp - write_num;
				m_write_count += write_num;
			}
			return true;
		}
		//write the file into the buffer 
		//can be used in non-BYTE buffer
		int write( std::string filename ){
			reset_over();
			std::ifstream infile( filename,std::ios::in | std::ios::binary);
            if(!infile){
                LOG_TRIVIAL(error)<<"The file can not be opened";
                return 0;
            }
			const int size = 100;
			elem_type buf[size];
			int total;
			int yu = 0;
			while((total = infile.read((char *)buf + yu, m_elem_size * size).gcount())){
				//std::cout<<"total "<<total<<std::endl;
				//std::cout<<"write "<<buf<<std::endl;
				total += yu;
				yu = total % m_elem_size;
				total = total / m_elem_size;
				int temp = total;
				
				while(temp){
					//boost::mutex::scoped_lock lock(buffer_mutex);
                    //buffer_mutex.lock();
                    
                    //scope lock
                    ecgraph::scoped_lock lock(*buffer_mutex);
					
                    
					int write_num = enqueue(buf + total - temp, temp);
					temp = temp - write_num;
					m_write_count += write_num;
					//buffer_mutex.unlock();
				}
				if(infile.eof()){
					//LOG_TRIVIAL(info)<<"file "<<filename << " has being written into the buffer";
					//boost::mutex::scoped_lock lock(buffer_mutex);
                    //scope lock
					set_over();
                    //buffer_mutex.unlock();
				}
				
			}
			set_over();
			//LOG_TRIVIAL(info)<< "write thread has finished total write "<< write_count << " bytes";
			return m_write_count;
		}


		int read(elem_type * buf, int n){
			int total = n;
			int temp = n;
			while(temp){
				//boost::mutex::scoped_lock lock(buffer_mutex);
                //buffer_mutex.lock();
				ecgraph::scoped_lock lock(*buffer_mutex);
				int read_num = dequeue(buf + total - temp, temp);
				temp = temp - read_num; 
				m_read_count += read_num;
				if (m_over == 1 && is_empty())
					break;
                //buffer_mutex.unlock();
			}
			return total - temp ;
		}
		
		void start_write(std::string filename){
			reset();
			//auto f = boost::bind(&buffer::write, this, filename);
            auto f = std::bind(&buffer::write, this, filename);
			//LOG_TRIVIAL(info) << "starting write the buffer using file "<< filename;
			//thrd = new boost::thread(f);
			m_thrd = new std::thread(f);
		}


		void write_join(){
			if (m_thrd){
				if (m_thrd->joinable() ){
					m_thrd->join();
				}
				delete m_thrd;
				m_thrd = NULL;
			}
		}

		bool is_over(){
			//boost::mutex::scoped_lock lock(buffer_mutex);
			//std::cout<<"in buffer is_over"<<std::endl;	
			ecgraph::scoped_lock lock(*buffer_mutex);
			return (m_over == 1) && (is_empty()) ;
		}
		void set_over() { //使结束
			ecgraph::scoped_lock lock(*buffer_mutex);
			m_over = 1;
		}
		void reset_over() {
			ecgraph::scoped_lock lock(*buffer_mutex);
			m_over = 0;
		}
		void reset(){
			m_over = 0;
			m_read_count = 0;
			m_write_count = 0;
			m_front = 0;
			m_rear = 0;
			write_join();
		}
		void show() {
			LOG_TRIVIAL(info) << "m_over: " << m_over
				<< " m_front: " << m_front
				<< " m_rear: " << m_rear;
		}
	};
}	
#endif

