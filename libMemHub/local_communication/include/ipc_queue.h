/**
 * Inter-Process Communication (IPC) Queue
 */

#ifndef IPC_QUEUE_H
#define IPC_QUEUE_H

#include "msg_info.h"
#include "util.h"
class CIPCQueue
{
public:
    CIPCQueue(int job_id, bool server, int train);
    bool MsgSend(const void *msg, int msg_size, int msg_flg);
    bool MsgRecv(void *msg, int msg_size, int msg_type, int msg_flg);
    bool Destory();

private:
    key_t m_msg_key;
    int m_msg_queue_id;
    bool MsgGet();
    bool MsgCreate();
};

#endif
