/*
 *author:      allen lee(Junhao Li) allen_lee922@foxmail.com
 *address:     WNLO, Huazhong University of Science & Technology
 *Time:        2016.11
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
 *  his namespace dx_lib includes somes utils for our project.
 *  class myclock is a scoped lock
 *  
 *  
 */
#ifndef _MYLOCK_
#define _MYLOCK_
#include <mutex>
namespace ecgraph{
    class scoped_lock{
    private:
        std::mutex *mymutex;
    public:
        scoped_lock(std::mutex &mu){
           mymutex = & mu;
           mymutex->lock(); 
        }
        ~scoped_lock(){
            mymutex->unlock();
        }
    };

}
#endif
