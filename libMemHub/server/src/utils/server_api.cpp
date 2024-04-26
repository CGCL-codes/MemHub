#include "../../include/utils/server_api.h"

/**
 * Sets up the Server of MemHub.
 *
 * @param dataset_path The path to the origin dataset to be used for training.
 * @param chunk_size The size of data chunks to be processed (files/chunk).
 * @param batch_size The number of samples in each training batch.
 * @param cache_ratio The ratio of data to be cached in memory.
 * @param nodes_num The number of training nodes.
 * @param worker_num The number of workers per training node.
 * @param gpu_num The number of GPU devices per training node.
 * @param rank The rank of the current node in the distributed setup.
 * @param seed The seed for random sampling.
 * @param train Flag indicating whether this server is set up for the training phase (1) or validation (0).
 */

void Setup(wchar_t *dataset_path, int chunk_size, int batch_size, float cache_ratio,
           int nodes_num, int worker_num, int gpu_num, int rank, int seed, int train)
{
    std::cout << "***Server of MemHub Setup begin" << std::endl;
    using type = std::codecvt_utf8<wchar_t>;
    std::wstring_convert<type, wchar_t> converter;
    std::string str_dataset_path = converter.to_bytes(dataset_path);
    data_manager = new CDataManager(str_dataset_path, chunk_size, batch_size, cache_ratio, nodes_num, rank, worker_num);
    sampler = new CSampler(data_manager, nodes_num, rank, worker_num, gpu_num, batch_size, seed);
    data_manager->set_sampler(sampler);
    communicator_backend = new CCommunicatorBackend(data_manager, sampler, rank, nodes_num, worker_num);
    data_manager->set_communicator_backend(communicator_backend);
    InitCSPort(worker_num, train);
    std::cout << "***Server of MemHub Setup success" << std::endl;
}

/**
 * Initializes the interface for communication with various Clients on this node.
 */

void InitCSPort(int worker_num, int train)
{
    for (int worker_id = 0; worker_id < worker_num; ++worker_id)
    {
        CClientServerPort *cs_port = new CClientServerPort(worker_id, data_manager, train);
        v_cs_port.push_back(cs_port);
    }
}

/**
 * Set up a new epoch.
 *
 * Two synchronizations are required upon setting up a new epoch.
 * The first synchronization ensures that all nodes are ready to enter the new epoch.
 * The second synchronization ensures that all nodes have completed setting up the new epoch in the Server of MemHub.
 */
void SetEpoch(int epoch)
{
    communicator_backend->nodes_synch(0);
    data_manager->new_epoch();
    sampler->set_epoch(epoch);
    communicator_backend->nodes_synch(1);
}