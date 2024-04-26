// The interface for the Server of MemHub to communicate with one Client on this node.

#ifndef CS_PORT_H
#define CS_PORT_H

#include "../../../local_communication/include/ipc_queue.h"
#include "../../../local_communication/include/ipc_shm.h"
#include "../manager/data_manager.h"
#include "util.h"

class CClientServerPort{
public:
    CClientServerPort(int worker_id, CDataManager* data_manager,int train);
    ~CClientServerPort();
    ClientMsg RecvFromClient();
    void SendToClient(ServerMsg& server_msg,char* file_content);
    void request_file(RequestDataInfo& request_info,FileInfo* file_info);
    int get_worker_id();
private:
    int m_worker_id;
    CDataManager* m_data_manager;
    std::thread m_run_thread;
    CIPCQueue* m_ipc_queue;
    CIPCShm* m_ipc_shm;
    int m_train;
    void InitIPCQueue();
    void InitIPCShm();
    void InitRun();
};

void Run(CClientServerPort* cs_port);


#endif