
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
#ifndef _TYPES_
#define _TYPES_
#include <climits>
namespace ecgraph{
	typedef char byte_t;
	#ifdef COMPACT_GRAPH
	typedef unsigned int vertex_t;
	typedef double weight_t;
	const vertex_t VERTEX_MIN = 0;
	const vertex_t VERTEX_MAX = UINT_MAX;
	#else
	typedef unsigned long vertex_t;
	typedef double weight_t;
	const vertex_t VERTEX_MIN = 0;
	const vertex_t VERTEX_MAX = ULONG_MAX;
	#endif
	
	#ifdef WEIGHT_EDGE	

	#pragma pack(1)
	typedef struct {
		vertex_t src;
		vertex_t dst;
		weight_t value;
	}edge_t;
	#pragma pack()

	#else
	#pragma pack(1)
	typedef struct {
		vertex_t src;
		vertex_t dst;
	}edge_t;
	#pragma pack()
	#endif
}
#endif
