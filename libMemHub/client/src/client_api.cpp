
#include "../include/client_api.h"

void Setup(int job_id, int train)
{
    m_job_id = job_id;
    client = new CClient(job_id, train);
}

TrainDataInfo *get_sample(int request_idx)
{
    TrainDataInfo *data_info = client->get_train_data_info(request_idx);
    return data_info;
}

void delete_char_pointer(char *p)
{
    delete[] p;
    p = nullptr;
}

void delete_train_data_info_pointer(TrainDataInfo *p)
{
    delete p;
    p = nullptr;
}