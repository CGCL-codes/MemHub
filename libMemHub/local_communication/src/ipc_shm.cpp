#include "../include/ipc_shm.h"

/**
 * Similar functions refer to ipc_queue
 */
CIPCShm::CIPCShm(int job_id, bool server, int train)
{
    if (access(MSG_PATH, 0) == -1)
    {
        int flag = mkdir(MSG_PATH, S_IRWXU);
        if (flag == 0)
        {
        }
        else
        {
            std::cout << "Fail to create directory." << std::endl;
            throw std::exception();
        }
    }

    int msg_job_id;
    if (train == 1)
    {
        msg_job_id = MSG_TRAIN_SHM_JOB_ID + job_id;
    }
    else
    {
        msg_job_id = MSG_VAL_SHM_JOB_ID + job_id;
    }
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
            result = this->ShmCreate();
        }
        else
        {
            result = this->ShmGet();
        }

        if (result == false)
        {
            std::cerr << "m_msg_shm_id create failed\n";
            exit(0);
        }
    }
}

bool CIPCShm::ShmGet()
{
    this->m_msg_shm_id = shmget(this->m_msg_key, MAX_FILE_SIZE, IPC_CREAT | 0666);
    if (-1 == this->m_msg_shm_id)
    {
        return false;
    }
    else
    {
        // Attach shared memory identified by m_msg_shm_id to m_shm_buffer.
        this->m_shm_buffer = (char *)shmat(this->m_msg_shm_id, NULL, 0);
        return true;
    }
}

bool CIPCShm::ShmCreate()
{
    int nQueueID = shmget(this->m_msg_key, MAX_FILE_SIZE, IPC_CREAT | IPC_EXCL | 0666);
    if (-1 == nQueueID)
    {
        this->ShmGet();
        shmctl(this->m_msg_shm_id, IPC_RMID, NULL);
        this->m_msg_shm_id = 0;
        return this->ShmGet();
    }
    this->m_msg_shm_id = nQueueID;
    // Attach shared memory identified by m_msg_shm_id to m_shm_buffer.
    this->m_shm_buffer = (char *)shmat(this->m_msg_shm_id, NULL, 0);
    return true;
}

bool CIPCShm::ShmWrite(char *msg, int msg_size)
{
    memcpy(this->m_shm_buffer, msg, msg_size);
    try
    {
        delete[] msg;
        msg = nullptr;
    }
    catch (...)
    {
        std::cerr << "write shm failed\n";
        exit(0);
    }
    return true;
}

bool CIPCShm::ShmRead(char *msg, int msg_size)
{
    memcpy(msg, this->m_shm_buffer, msg_size);
    return true;
}

bool CIPCShm::Destory()
{
    int result = shmctl(this->m_msg_shm_id, IPC_RMID, NULL);
    if (-1 == result)
    {
        std::cerr << "Destory message shm failed\n";
        return false;
    }
    return true;
}
