#ifndef _MESSAGE_FACTORY_H_
#define _MESSAGE_FACTORY_H_

#include "packet/messages.h"
#include "utils/log_wrapper.h"

class message_factory {

public:
	base_message * create_message(int message_id) {
		base_message *msg = NULL;
		switch (message_id) {
			//case WORKER_ASK_FOR_START_MSGID:
				//msg = new worker_runtime_info_msg();
				//break;

			case MASTER_PERMIT_START_MSGID:
				msg = new master_permit_start_msg();
				break;

			default:
				#ifdef MY_DEBUG
				LOG_TRIVIAL(warn) << "no such message";
				#endif
				break;
		}
		return msg;
	}

};
#endif