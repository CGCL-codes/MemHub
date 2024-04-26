/**
 * Random Sampler in MemHub.
 * It primarily accomplishes the following tasks:
 *      1. Generate a random access sequence for each epoch based on a random seed, and allocate the generated random access sequences to nodes.
 *      2. Allocate nodes for chunk group read permissions, ensuring that within an epoch, each data can only be read from the file system into memory by one node, ensuring data consistency in distributed training.
 *      3. Based on the random access sequence and the node allocation result for data read permissions, assist the fast prefetching algorithm in completing prefetching tasks.
 */

#ifndef SAMPLER_H
#define SAMPLER_H

#include "../../include/manager/data_manager.h"
#include "i_sampler.h"
#include "../utils/util.h"

typedef struct WorkerRequestIdx
{
    int worker_id;
    int request_idx;
    WorkerRequestIdx() : worker_id(-1), request_idx(-1) {}
    WorkerRequestIdx(int w_id, int a_idx) : worker_id(w_id), request_idx(a_idx) {}
    bool operator==(const WorkerRequestIdx &p) const
    {
        return worker_id == p.worker_id && request_idx == p.request_idx;
    }
    const WorkerRequestIdx operator=(const WorkerRequestIdx &p)
    {
        worker_id = p.worker_id;
        request_idx = p.request_idx;
        return *this;
    }
} WorkerRequestIdx;

class CSampler : public CISampler
{
public:
    CSampler(CDataManager *data_manager, int nodes_num, int rank, int worker_num, int gpu_num, int batch_size, int seed = 0);

    void set_epoch(int epoch);
    void set_node_mem_loc_last_request_data_info(int node_id, int worker_id, int request_idx);
    void UpdateAfterFetch(int node_id, int worker_id, int request_idx, int base_request_idx);
    int get_chunk_group_node_id(int chunk_group_id);
    int get_file_idx(int node_id, int request_idx);
    int get_mem_loc_by_request_idx(int node_id, int request_idx);
    int get_remote_request_idx_to_local(int node_id, int worker_id, int map_idx);
    int get_remote_requests_to_local_map_idx(int node_id, int worker_id, int request_idx);
    int get_worker_next_request_idx(int node_id, int worker_id, int request_idx);

    bool is_fetched(int node_id, int request_idx);
    bool is_correct_request_idx(int node_id, int request_idx);
    bool is_last_reply_used(int node_id, int worker_id, int request_idx, int base_request_idx);
    void mem_loc_mtx_lock(int node_id, int request_idx);
    void mem_loc_mtx_unlock(int node_id, int request_idx);

    bool CheckCurNode(int chunk_group_id);
    void RedoLastReplyUsed(int node_id, int worker_id, int request_idx);
    void DealFetchQueue(int node_id, int worker_id, int request_idx);

private:
    CDataManager *m_data_manager;
    int m_nodes_num;
    int m_worker_num;
    int m_gpu_num;
    int m_batch_size;
    int m_rank;
    int m_seed;
    int m_epoch;
    int m_file_num;
    int m_mem_loc_num;
    int m_chunk_group_num;
    std::vector<int> m_file_idxs;
    std::vector<std::vector<std::mutex *>> m_mem_loc_mtx_map;                                                          // <memory_loc, mutex>
    std::vector<std::vector<int>> m_node_file_idxs_map;                                                                // <node_idx, <files>>
    std::vector<int> m_chunk_group_node_map;                                                                           // <chunk_group_id, node_id>
    std::vector<std::vector<int>> m_node_chunk_group_map;                                                              // <node_id, <chunk groups>>
    std::vector<std::vector<std::priority_queue<int, std::vector<int>, std::greater<int>>>> m_node_worker_fetch_queue; // Fetch queues for each worker on each node.

    /**
     * Mark whether the data assigned to each node has been fetched.
     */
    std::vector<std::vector<bool>> m_node_file_fetched_map;

    /**
     * Mark the information of the last request from this node to retrieve data from the memory location,
     *      including worker_id and request_id.
     * For each memory location: <-1, -1> indicates that the data fetched during the last fetch operation has been used.

     */
    std::vector<std::vector<WorkerRequestIdx>> m_node_mem_loc_last_request_idx_map;

    /**
     * Store the information of the requests that each worker on remote nodes is going to read data from this node,
     * including the request_id.
     */
    std::vector<std::vector<std::vector<int>>> m_remote_requests_to_local_map;

    /**
     * Store the reverse indices of each request in m_remote_requests_to_local_map,
     *      where each request is associated with its index in m_remote_requests_to_local_map.
     * The storage format is <request_idx, idx>.
     */
    std::vector<std::vector<std::unordered_map<int, int>>> m_remote_requests_to_local_idx_map;

    void InitMtxs();
    void NewEpoch();
    void InitIndices();
    void ShuffleIndices();
    void AllocateNodeIndices();
    void AllocateNodeChunkGroups();
};

#endif