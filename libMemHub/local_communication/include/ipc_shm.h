/**
 * Inter-Process Communication (IPC) Shared Memory
 */

#ifndef IPC_SHM_H
#define IPC_SHM_H

#include "msg_info.h"
#include "util.h"
class CIPCShm
{
public:
    CIPCShm(int job_id, bool server, int train);
    bool ShmWrite(char *msg, int msg_size);
    bool ShmRead(char *msg, int msg_size);
    bool Destory();

private:
    key_t m_msg_key;
    int m_msg_shm_id;
    char *m_shm_buffer;
    int m_train;

    bool ShmGet();
    bool ShmCreate();
};

#endif