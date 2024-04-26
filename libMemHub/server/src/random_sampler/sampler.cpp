#include "../../include/random_sampler/sampler.h"

CSampler::CSampler(CDataManager *data_manager, int nodes_num, int rank, int worker_num, int gpu_num, int batch_size, int seed)
{
    this->m_data_manager = data_manager;
    this->m_nodes_num = nodes_num;
    this->m_rank = rank;
    this->m_worker_num = worker_num;
    this->m_gpu_num = gpu_num;
    this->m_batch_size = batch_size;
    this->m_seed = seed;

    this->m_mem_loc_num = this->m_data_manager->get_mem_loc_num();
    this->m_file_num = this->m_data_manager->get_file_num();
    this->m_chunk_group_num = this->m_data_manager->get_chunk_group_num();

    this->m_chunk_group_node_map = std::vector<int>(this->m_chunk_group_num);
    this->m_file_idxs = std::vector<int>(this->m_file_num);
    this->m_node_file_idxs_map = std::vector<std::vector<int>>(this->m_nodes_num);
    this->m_node_file_fetched_map = std::vector<std::vector<bool>>(this->m_nodes_num);
    this->m_node_chunk_group_map = std::vector<std::vector<int>>(this->m_nodes_num);
    this->m_node_mem_loc_last_request_idx_map = std::vector<std::vector<WorkerRequestIdx>>(this->m_nodes_num, std::vector<WorkerRequestIdx>(this->m_mem_loc_num));
    this->m_remote_requests_to_local_map = std::vector<std::vector<std::vector<int>>>(this->m_nodes_num, std::vector<std::vector<int>>(this->m_worker_num));
    this->m_remote_requests_to_local_idx_map = std::vector<std::vector<std::unordered_map<int, int>>>(this->m_nodes_num, std::vector<std::unordered_map<int, int>>(this->m_worker_num));
    this->m_node_worker_fetch_queue = std::vector<std::vector<std::priority_queue<int, std::vector<int>, std::greater<int>>>>(this->m_nodes_num,
                                                                                                                              std::vector<std::priority_queue<int, std::vector<int>, std::greater<int>>>(this->m_worker_num));
    this->m_epoch = -1;
    this->set_epoch(0);
    this->InitMtxs();
}

/**
 * The mutex for memory location here is distinct from the mutex in the memory backend.
 * It is used to prevent the inconsistency of data caused by multiple workers from the same node
 * simultaneously issuing requests to the same target node, thus preventing the same memory location
 * from being prefetched simultaneously.
 */

void CSampler::InitMtxs()
{
    this->m_mem_loc_mtx_map = std::vector<std::vector<std::mutex *>>(this->m_nodes_num);
    for (int node_id = 0; node_id < this->m_nodes_num; ++node_id)
    {
        for (int mem_loc = 0; mem_loc < this->m_mem_loc_num; ++mem_loc)
        {
            std::mutex *mtx = new std::mutex;
            this->m_mem_loc_mtx_map[node_id].push_back(mtx);
        }
    }
}

void CSampler::mem_loc_mtx_lock(int node_id, int request_idx)
{
    int mem_loc = this->get_mem_loc_by_request_idx(node_id, request_idx);
    this->m_mem_loc_mtx_map[node_id][mem_loc]->lock();
}

void CSampler::mem_loc_mtx_unlock(int node_id, int request_idx)
{
    int mem_loc = this->get_mem_loc_by_request_idx(node_id, request_idx);
    this->m_mem_loc_mtx_map[node_id][mem_loc]->unlock();
}

void CSampler::set_epoch(int epoch)
{
    if (epoch != this->m_epoch)
    {
        this->m_epoch = epoch;
        this->NewEpoch();
    }
}

void CSampler::NewEpoch()
{
    this->InitIndices();
    this->ShuffleIndices();
    this->AllocateNodeIndices();
    this->AllocateNodeChunkGroups();
}

void CSampler::InitIndices()
{
    for (int idx = 0; idx < this->m_file_num; ++idx)
    {
        this->m_file_idxs[idx] = idx;
    }
}

/**
 * Generate a random access sequence for each epoch based on a random seed.
 * */
void CSampler::ShuffleIndices()
{
    std::mt19937 rng(this->m_epoch + this->m_seed);
    std::shuffle(this->m_file_idxs.begin(), this->m_file_idxs.end(), rng);
}

/**
 * Allocate the generated random access sequences to nodes.
 */
void CSampler::AllocateNodeIndices()
{
    for (int node_id = 0; node_id < this->m_nodes_num; ++node_id)
    {
        for (int mem_loc = 0; mem_loc < static_cast<int>(this->m_node_mem_loc_last_request_idx_map[node_id].size()); ++mem_loc)
        {
            this->m_node_mem_loc_last_request_idx_map[node_id][mem_loc].worker_id = -1;
            this->m_node_mem_loc_last_request_idx_map[node_id][mem_loc].request_idx = -1;
        }
    }

    if (this->m_epoch == 0)
    {
        int node_id;
        for (int idx = 0; idx < this->m_file_num; ++idx)
        {
            node_id = idx % this->m_nodes_num;
            int file_idx = this->m_file_idxs[idx];
            this->m_node_file_idxs_map[node_id].push_back(file_idx);
            this->m_node_file_fetched_map[node_id].push_back(false);
        }
    }
    else
    {
        int node_id;
        int map_idx = 0;
        for (int idx = 0; idx < this->m_file_num; ++idx)
        {
            node_id = idx % this->m_nodes_num;
            int file_idx = this->m_file_idxs[idx];
            if (idx != 0 && node_id == 0)
            {
                ++map_idx;
            }
            this->m_node_file_idxs_map[node_id][map_idx] = file_idx;
            this->m_node_file_fetched_map[node_id][map_idx] = false;
        }
    }
}

/**
 *  Allocate nodes for chunk group read permissions
 */
void CSampler::AllocateNodeChunkGroups()
{
    // Reset the relevant objects.
    for (int node_id = 0; node_id < this->m_nodes_num; ++node_id)
    {
        this->m_node_chunk_group_map[node_id].clear();
        for (int worker_id = 0; worker_id < this->m_worker_num; ++worker_id)
        {
            this->m_remote_requests_to_local_map[node_id][worker_id].clear();
            this->m_remote_requests_to_local_idx_map[node_id][worker_id].clear();
            while (!this->m_node_worker_fetch_queue[node_id][worker_id].empty())
            {
                this->m_node_worker_fetch_queue[node_id][worker_id].pop();
            }
        }
    }

    // Algorithm 1: Random Allocation Algorithm
    // std::vector<int> chunk_groups(this->m_chunk_group_num);
    // for(int chunk_group_id=0;chunk_group_id<this->m_chunk_group_num;++chunk_group_id){
    //     chunk_groups[chunk_group_id]=chunk_group_id;
    // }
    // std::mt19937 rng((this->m_epoch+this->m_seed)*2);
    // std::shuffle(chunk_groups.begin(),chunk_groups.end(),rng);

    // int node_id=0;
    // for(int idx=0;idx<this->m_chunk_group_num;++idx){
    //     this->m_chunk_group_node_map[chunk_groups[idx]]=node_id;
    //     this->m_node_chunk_group_map[node_id].push_back(chunk_groups[idx]);
    //     node_id=(node_id+1)%this->m_nodes_num;
    // }

    // Algorithm 2: Highest Frequency Allocation Algorithm
    // Calculate the frequency of occurrence of each chunk group on each node for this epoch.
    std::vector<std::unordered_map<int, int>> chunk_group_node_frequence_map(m_chunk_group_num);
    for (int node_id = 0; node_id < this->m_nodes_num; ++node_id)
    {
        for (auto file_idx : this->m_node_file_idxs_map[node_id])
        {
            int chunk_group_id = this->m_data_manager->get_file_chunk_group_id(file_idx);

            if (chunk_group_node_frequence_map[chunk_group_id].find(node_id) == chunk_group_node_frequence_map[chunk_group_id].end())
            {
                chunk_group_node_frequence_map[chunk_group_id][node_id] = 1;
            }
            else
            {
                ++chunk_group_node_frequence_map[chunk_group_id][node_id];
            }
        }
    }
    // Allocate chunk groups to the highest frequency nodes.
    for (int chunk_group_id = 0; chunk_group_id < this->m_chunk_group_num; ++chunk_group_id)
    {
        int max_node_id = -1;
        int max_frequence = -1;
        for (auto iter = chunk_group_node_frequence_map[chunk_group_id].begin(); iter != chunk_group_node_frequence_map[chunk_group_id].end(); ++iter)
        {
            int node_id = (*iter).first;
            int frequence = (*iter).second;
            if (frequence > max_frequence)
            {
                max_node_id = node_id;
                max_frequence = frequence;
            }
        }
        this->m_chunk_group_node_map[chunk_group_id] = max_node_id;
        this->m_node_chunk_group_map[max_node_id].push_back(chunk_group_id);
    }

    // Calculate which chunks need to be fetched from the current node in the access sequence
    // of workers on other nodes, based on the allocation of chunk groups to nodes, facilitating
    // direct determination during prefetching.
    int gpu_worker_num = this->m_worker_num / this->m_gpu_num;
    for (int node_id = 0; node_id < this->m_nodes_num; ++node_id)
    {
        int worker_id;
        std::vector<std::vector<int>> check_node_worker_idx = std::vector<std::vector<int>>(this->m_worker_num);
        int gpu_file_idxs_num = (static_cast<int>(this->m_node_file_idxs_map[node_id].size()) + this->m_gpu_num - 1) / this->m_gpu_num;
        for (int request_idx = 0; request_idx < static_cast<int>(this->m_node_file_idxs_map[node_id].size()); ++request_idx)
        {
            int file_idx = this->m_node_file_idxs_map[node_id][request_idx];
            int chunk_group_id = this->m_data_manager->get_file_chunk_group_id(file_idx);

            int gpu_id = request_idx / gpu_file_idxs_num;
            int base_request_idx = gpu_file_idxs_num * gpu_id;
            int base_worker_id = gpu_worker_num * gpu_id;

            worker_id = ((request_idx - base_request_idx) / this->m_batch_size) % gpu_worker_num + base_worker_id;

            check_node_worker_idx[worker_id].push_back(request_idx);
            if (this->m_chunk_group_node_map[chunk_group_id] == this->m_rank)
            {
                this->m_remote_requests_to_local_map[node_id][worker_id].push_back(request_idx);
                this->m_remote_requests_to_local_idx_map[node_id][worker_id][request_idx] =
                    static_cast<int>(this->m_remote_requests_to_local_map[node_id][worker_id].size()) - 1;
            }
        }
    }
}

int CSampler::get_mem_loc_by_request_idx(int node_id, int request_idx)
{
    int file_idx = this->get_file_idx(node_id, request_idx);
    return this->m_data_manager->get_file_mem_loc(file_idx);
}

int CSampler::get_chunk_group_node_id(int chunk_group_id)
{
    return this->m_chunk_group_node_map[chunk_group_id];
}

int CSampler::get_file_idx(int node_id, int request_idx)
{
    if (request_idx == END_FLAG)
    {
        return END_FLAG;
    }
    return this->m_node_file_idxs_map[node_id][request_idx];
}

bool CSampler::CheckCurNode(int chunk_group_id)
{
    if (this->m_chunk_group_node_map[chunk_group_id] == this->m_rank)
    {
        return true;
    }
    else
    {
        return false;
    }
}

/**
 * Update relevant information after the file is read:
 * 1. Update node_file_fetched_map: Mark that the data corresponding to this request from this node has been retrieved.
 * 2. Update node_worker_fetch_queue: Mark all requests from this worker on this node to fetch data from the current node.
 */

void CSampler::UpdateAfterFetch(int node_id, int worker_id, int request_idx, int base_request_idx)
{
    this->m_node_file_fetched_map[node_id][request_idx] = true;
    if (request_idx != base_request_idx)
    {
        this->m_node_worker_fetch_queue[node_id][worker_id].push(request_idx);
    }
}

bool CSampler::is_fetched(int node_id, int request_idx)
{
    return this->m_node_file_fetched_map[node_id][request_idx];
}

bool CSampler::is_correct_request_idx(int node_id, int request_idx)
{
    if (request_idx < static_cast<int>(this->m_node_file_idxs_map[node_id].size()))
    {
        return true;
    }
    else
    {
        return false;
    }
}

/**
 * Query the index of this request from this worker on this node in m_remote_requests_to_local_map.
 */

int CSampler::get_remote_requests_to_local_map_idx(int node_id, int worker_id, int request_idx)
{
    return this->m_remote_requests_to_local_idx_map[node_id][worker_id][request_idx];
}

/**
 * Query the request_idx of this worker on this node at map_idx in m_remote_requests_to_local_map.
 */
int CSampler::get_remote_request_idx_to_local(int node_id, int worker_id, int map_idx)
{
    if (map_idx < static_cast<int>(this->m_remote_requests_to_local_map[node_id][worker_id].size()))
    {
        return this->m_remote_requests_to_local_map[node_id][worker_id][map_idx];
    }
    else
    {
        return -1;
    }
}

/**
 * Query the next request that needs to fetch data from the current node in the access sequence of
 * requests for this worker on this node, starting from request_idx.
 */
int CSampler::get_worker_next_request_idx(int node_id, int worker_id, int request_idx)
{
    int map_idx = this->get_remote_requests_to_local_map_idx(node_id, worker_id, request_idx);
    if (map_idx < static_cast<int>(this->m_remote_requests_to_local_map[node_id][worker_id].size()) - 1)
    {
        int next_request_idx = get_remote_request_idx_to_local(node_id, worker_id, map_idx + 1);
        return next_request_idx;
    }
    else
    {
        return -1;
    }
}

/**
 * Pop all requests in node_worker_fetch_queue for this node and this worker that are smaller than request_idx.
 * Upon receiving a data request from this worker on this node, it indicates that the previously
 * prefetched data for this request has been used. All memory locations corresponding to this node
 * are now empty, and prefetching can continue.
 *
 */
void CSampler::DealFetchQueue(int node_id, int worker_id, int request_idx)
{

    int prefetched_request_idx;
    if (static_cast<int>(this->m_node_worker_fetch_queue[node_id][worker_id].size()))
    {
        prefetched_request_idx = this->m_node_worker_fetch_queue[node_id][worker_id].top();
    }

    while (static_cast<int>(this->m_node_worker_fetch_queue[node_id][worker_id].size()) && prefetched_request_idx < request_idx)
    {
        int mem_loc = this->get_mem_loc_by_request_idx(node_id, prefetched_request_idx);
        this->m_mem_loc_mtx_map[node_id][mem_loc]->lock();
        this->m_node_mem_loc_last_request_idx_map[node_id][mem_loc].worker_id = -1;
        this->m_node_mem_loc_last_request_idx_map[node_id][mem_loc].request_idx = -1;
        this->m_node_worker_fetch_queue[node_id][worker_id].pop();
        this->m_mem_loc_mtx_map[node_id][mem_loc]->unlock();

        if (static_cast<int>(this->m_node_worker_fetch_queue[node_id][worker_id].size()))
        {
            prefetched_request_idx = this->m_node_worker_fetch_queue[node_id][worker_id].top();
        }
    }
}

/**
 * Mark the information of the last fetch operation from this memory location on this node.
 */
void CSampler::set_node_mem_loc_last_request_data_info(int node_id, int worker_id, int request_idx)
{
    int mem_loc = this->get_mem_loc_by_request_idx(node_id, request_idx);
    this->m_node_mem_loc_last_request_idx_map[node_id][mem_loc].worker_id = worker_id;
    this->m_node_mem_loc_last_request_idx_map[node_id][mem_loc].request_idx = request_idx;
}

/**
 * Check if the data fetched from this memory location on this node during the last fetch operation
 * has been used by this node.
 */
bool CSampler::is_last_reply_used(int node_id, int worker_id, int request_idx, int base_request_idx)
{
    int mem_loc = this->get_mem_loc_by_request_idx(node_id, request_idx);

    WorkerRequestIdx last_worker_request_idx = this->m_node_mem_loc_last_request_idx_map[node_id][mem_loc];
    if (last_worker_request_idx.worker_id == -1 && last_worker_request_idx.request_idx == -1)
    {
        return true;
    }
    return false;
}
