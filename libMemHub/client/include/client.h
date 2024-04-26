#ifndef CLIENT_H
#define CLIENT_H

#include "../../local_communication/include/ipc_queue.h"
#include "../../local_communication/include/ipc_shm.h"
#include "util.h"

class CClient
{
public:
    CClient(int worker_id, int train);
    TrainDataInfo *get_train_data_info(int request_idx);

private:
    int m_worker_id;
    /**
     * m_msg_queue: Queue for exchanging messages between Client and Server. Specifically:
     *   client → server: request index from worker
     *   server → client: returned training data information, including data index, label index,
     *                    data size, and so on.
     */
    CIPCQueue *m_msg_queue;
    /**
     * m_msg_shm: Shared memory used by the Server to transmit training data to the Client.
     */
    CIPCShm *m_msg_shm;
    int m_train;

    void InitMsgQueue();
    void InitMsgShm();
};

#endif