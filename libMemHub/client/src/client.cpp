#include "../include/client.h"

CClient::CClient(int worker_id, int train)
{
    this->m_worker_id = worker_id;
    this->m_train = train;

    this->InitMsgQueue();
    this->InitMsgShm();
}

void CClient::InitMsgQueue()
{
    this->m_msg_queue = new CIPCQueue(this->m_worker_id, false, this->m_train);
}

void CClient::InitMsgShm()
{
    this->m_msg_shm = new CIPCShm(this->m_worker_id, false, this->m_train);
}

TrainDataInfo *CClient::get_train_data_info(int request_idx)
{

    ClientMsg client_msg;
    client_msg.request_idx = request_idx;
    client_msg.msg_type = MSG_CLIENT_TYPE;

    bool msg_send_result = this->m_msg_queue->MsgSend(&client_msg, CLIENT_MSG_SIZE, IPC_WAIT);

    if (msg_send_result == false)
    {
        std::cerr << "client " + std::to_string(this->m_worker_id) + " send request msg to server failed, request_idx:" + std::to_string(request_idx) << std::endl;
        exit(0);
    }

    ServerMsg server_msg;
    bool msg_recv_result = this->m_msg_queue->MsgRecv(&server_msg, Server_MSG_SIZE, MSG_SERVER_TYPE, IPC_WAIT);
    if (msg_recv_result == false)
    {
        std::cerr << "client " + std::to_string(this->m_worker_id) + " recieve request result from server failed, request_idx:" + std::to_string(request_idx) << std::endl;
        exit(0);
    }

    TrainDataInfo *data_info = new TrainDataInfo;
    data_info->file_content = new char[server_msg.file_size];
    bool shm_send_result = this->m_msg_shm->ShmRead(data_info->file_content, server_msg.file_size);
    data_info->file_size = server_msg.file_size;
    data_info->label_index = server_msg.label_index;
    return data_info;
}