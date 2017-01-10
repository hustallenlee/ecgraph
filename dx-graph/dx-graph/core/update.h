#ifndef _UPDATE_H_
#define _UPDATE_H_
//#include "core/engine.h"
#include "core/controller.h"
#include "core/computation.h"
#include "core/graph_define.h"


#include "utils/log_wrapper.h"
#include "utils/buffer.h"
#include "utils/consistent_hash.h"
#include "utils/types.h"
#include "utils/type_utils.h"
#include "utils/mylock.h"
#include "utils/config.h"
//==============================================================
//			update的类型要从以下类型中选
//==============================================================
//按字节对齐
#pragma pack(1)

//权值为double的update
typedef struct {
	ecgraph::vertex_t id;
	ecgraph::weight_t update_value;
}update_weight_double_t;

//权值为int的update
typedef struct {
	ecgraph::vertex_t id;
	int update_value;
}update_weight_int_t;

//权值为long long 的update
typedef struct {
	ecgraph::vertex_t id;
	long long update_value;
}update_weight_long_long_t;

#pragma pack()
#endif