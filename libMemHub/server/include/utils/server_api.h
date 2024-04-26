
// This is the interface file for the Server of MemHub, which serves as the interface between the Python and C++ backend.

#ifndef SERVER_API_H
#define SERVER_API_H

#include "../../include/utils/cs_port.h"
#include "../../include/manager/data_manager.h"
#include "../../include/random_sampler/sampler.h"
#include "../../include/remote_communicator/communicator_backend.h"
#include "util.h"

extern "C"
{
    CDataManager *data_manager;
    CSampler *sampler;
    CCommunicatorBackend *communicator_backend;
    std::vector<CClientServerPort *> v_cs_port;

    void Setup(wchar_t *dataset_path, int chunk_size, int batch_size, float cache_ratio,
               int nodes_num, int worker_num, int gpu_num, int rank,
               int seed, int train);
    void InitCSPort(int worker_num, int train);
    void SetEpoch(int epoch);
};

#endif