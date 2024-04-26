#include "../include/ipc_queue.h"

/**
 * Constructor for CIPCQueue.
 *
 * @param job_id The ID of the job associated with the queue.
 * @param server Boolean flag indicating whether the queue object is constructed for the server (true) or the client (false).
 * @param train Flag indicating whether the queue is for training (1) or validating (0).
 */
CIPCQueue::CIPCQueue(int job_id, bool server, int train)
{
    if (access(MSG_PATH, 0) == -1)
    {
        int flag = mkdir(MSG_PATH, S_IRWXU);
        if (flag != 0)
        {
            std::cout << "Fail to create directory." << std::endl;
            throw std::exception();
        }
    }

    int msg_job_id;
    if (train == 1)
    {
        msg_job_id = MSG_TRAIN_QUEUE_JOB_ID + job_id;
    }
    else
    {
        msg_job_id = MSG_VAL_QUEUE_JOB_ID + job_id;
    }
    // Note: According to ftok requirements, msg_job_id must be between 0 and 255.
    this->m_msg_key = ftok(MSG_PATH, msg_job_id);
    if (-1 == this->m_msg_key)
    {
        std::cerr << "m_msg_key create failed\n";
        exit(0);
    }
    else
    {
        bool result;
        if (server)
        {
            result = this->MsgCreate();
        }
        else
        {
            result = this->MsgGet();
        }
        if (result == false)
        {
            std::cerr << "m_msg_queue_id create failed\n";
            exit(0);
        }
    }
}

/**
 * Retrieves an existing IPC queue.
 *
 * @return True if the queue is successfully retrieved, false otherwise.
 */
bool CIPCQueue::MsgGet()
{
    this->m_msg_queue_id = msgget(this->m_msg_key, IPC_CREAT | 0666);
    if (-1 == this->m_msg_queue_id)
    {
        return false;
    }
    else
    {
        return true;
    }
}

/**
 * Attempts to create an IPC queue. If creation fails, it retrieves an existing queue and deletes the newly created one.
 *
 * @return True if the queue is successfully created or retrieved, false otherwise.
 */
bool CIPCQueue::MsgCreate()
{
    int nQueueID = msgget(this->m_msg_key, IPC_CREAT | IPC_EXCL | 0666);
    if (-1 == nQueueID)
    {
        this->MsgGet();
        msgctl(this->m_msg_queue_id, IPC_RMID, NULL);
        this->m_msg_queue_id = 0;
        return this->MsgGet();
    }
    this->m_msg_queue_id = nQueueID;
    return true;
}

/**
 * Sends a message to the IPC queue.
 *
 * @param msg Pointer to the message to be sent.
 * @param msg_size Size of the message to be sent.
 * @param msg_flg Message flags (IPC_NOWAIT, IPC_WAIT, etc.).
 * @return True if the message is successfully sent, false otherwise.
 */
bool CIPCQueue::MsgSend(const void *msg, int msg_size, int msg_flg)
{
    int result = msgsnd(this->m_msg_queue_id, msg, msg_size, msg_flg);

    if (-1 == result)
    {
        perror("msgsnd");
        std::cerr << "send msg  failed" << msg_size << "\n";
        return false;
    }
    return true;
}

/**
 * Receives a message from the IPC queue.
 *
 * @param msg Pointer to the received message.
 * @param msg_size Size of the received message.
 * @param msg_type Type of the message to receive (MSG_CLIENT_TYPE or MSG_SERVER_TYPE).
 *       Note: if only translating one type of message, set msg_type = 0;
 * @param msg_flg Message flags (IPC_NOWAIT, IPC_WAIT, etc.).
 * @return True if the message is successfully received, false otherwise.
 */
bool CIPCQueue::MsgRecv(void *msg, int msg_size, int msg_type, int msg_flg)
{
    int result = msgrcv(this->m_msg_queue_id, msg, msg_size, msg_type, msg_flg);
    if (-1 == result)
    {
        if (msg_flg != IPC_NOWAIT)
        {
            std::cerr << "recieve msg failed\n";
        }
        return false;
    }
    return true;
}

/**
 * Destroys the IPC queue.
 *
 * @return True if the queue is successfully destroyed, false otherwise.
 */
bool CIPCQueue::Destory()
{
    int result = msgctl(this->m_msg_queue_id, IPC_RMID, NULL);
    if (-1 == result)
    {
        std::cerr << "Destory message queue failed\n";
        return false;
    }
    std::cout << "ipc msg " << this->m_msg_queue_id << " has been destoryed\n"
              << std::endl;
    return true;
}
