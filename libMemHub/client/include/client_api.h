
// This is the interface file for the Client of MemHub, which serves as the interface between the Python and C++ backend.

#ifndef CLIENT_API_H
#define CLIENT_API_H

#include "client.h"
#include "util.h"

extern "C"
{
    int m_job_id;
    int m_debug_log;
    CClient *client;
    void Setup(int job_id, int train);
    TrainDataInfo *get_sample(int request_idx);

    void delete_char_pointer(char *p);
    void delete_train_data_info_pointer(TrainDataInfo *p);
}

#endif