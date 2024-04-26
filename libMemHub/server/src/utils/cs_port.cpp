#include "../../include/utils/cs_port.h"

CClientServerPort::CClientServerPort(int worker_id, CDataManager *data_manager, int train)
{
    this->m_worker_id = worker_id;
    this->m_data_manager = data_manager;
    this->m_train = train;
    this->InitIPCQueue();
    this->InitIPCShm();
    this->InitRun();
}

CClientServerPort::~CClientServerPort()
{
    this->m_run_thread.join();
    this->m_ipc_queue->Destory();
    this->m_run_thread.join();
}

void CClientServerPort::InitIPCQueue()
{
    this->m_ipc_queue = new CIPCQueue(this->m_worker_id, true, this->m_train);
}

void CClientServerPort::InitIPCShm()
{
    this->m_ipc_shm = new CIPCShm(this->m_worker_id, true, this->m_train);
}

/**
 * Creates a separate thread for communication with each connected Client in the Server of MemHub.
 */
void CClientServerPort::InitRun()
{
    this->m_run_thread = std::thread(Run, this);
    this->m_run_thread.detach();
}

ClientMsg CClientServerPort::RecvFromClient()
{
    ClientMsg client_msg;
    while (true)
    {
        bool result = this->m_ipc_queue->MsgRecv(&client_msg, CLIENT_MSG_SIZE, MSG_CLIENT_TYPE, IPC_WAIT);
        if (result == false)
        {
            std::cout << "cs port" + std::to_string(this->m_worker_id) + " recieve request msg from client failed" << std::endl;
        }
        else
        {
            break;
        }
    }
    return client_msg;
}

void CClientServerPort::SendToClient(ServerMsg &server_msg, char *file_content)
{
    this->m_ipc_shm->ShmWrite(file_content, server_msg.file_size);
    bool queue_result = this->m_ipc_queue->MsgSend(&server_msg, Server_MSG_SIZE, IPC_WAIT);
    if (queue_result == false)
    {
        std::cout << "cs port" + std::to_string(this->m_worker_id) + " send reply msg to client failed" << std::endl;
        exit(0);
    }
}

void CClientServerPort::request_file(RequestDataInfo &request_info, FileInfo *file_info)
{
    this->m_data_manager->request_file(request_info, file_info);
}

int CClientServerPort::get_worker_id()
{
    return this->m_worker_id;
}

void Run(CClientServerPort *cs_port)
{
    int worker_id = cs_port->get_worker_id();
    while (true)
    {
        ClientMsg client_msg = cs_port->RecvFromClient();
        int request_idx = client_msg.request_idx;

        RequestDataInfo request_info;
        request_info.worker_id = worker_id;
        request_info.request_idx = request_idx;

        FileInfo *file_info = new FileInfo();
        cs_port->request_file(request_info, file_info);
        ServerMsg server_msg;
        unsigned long file_size = file_info->file_size;

        server_msg.file_size = file_size;
        server_msg.label_index = file_info->label_index;
        server_msg.cache_file_idx = file_info->cache_file_idx;
        server_msg.msg_type = MSG_SERVER_TYPE;

        cs_port->SendToClient(server_msg, file_info->file_content);
        delete file_info;
        file_info = nullptr;

        if (request_idx == END_FLAG)
        {
            std::cout << "cs_port " << worker_id << " recieve END_FLAG\n";
            exit(0);
            break;
        }
    }
}