
// Interface of sampler.

#ifndef ISAMPLER_H
#define ISAMPLER_H

class CISampler
{
public:
    virtual void set_epoch(int epoch) = 0;
    virtual void set_node_mem_loc_last_request_data_info(int node_id, int worker_id, int request_idx) = 0;
    virtual int get_chunk_group_node_id(int chunk_group_id) = 0;
    virtual int get_file_idx(int node_id, int request_idx) = 0;
    virtual int get_mem_loc_by_request_idx(int node_id, int request_idx) = 0;
    virtual int get_remote_request_idx_to_local(int node_id, int worker_id, int map_idx) = 0;
    virtual int get_remote_requests_to_local_map_idx(int node_id, int worker_id, int request_idx) = 0;
    virtual int get_worker_next_request_idx(int node_id, int worker_id, int request_idx) = 0;

    virtual bool is_correct_request_idx(int node_id, int request_idx) = 0;
    virtual bool is_fetched(int node_id, int request_idx) = 0;
    virtual bool is_last_reply_used(int node_id, int worker_id, int request_idx, int base_request_idx) = 0;
    virtual void mem_loc_mtx_lock(int node_id, int request_idx) = 0;
    virtual void mem_loc_mtx_unlock(int node_id, int request_idx) = 0;

    virtual bool CheckCurNode(int chunk_group_id) = 0;
    virtual void DealFetchQueue(int node_id, int worker_id, int request_idx) = 0;
    virtual void UpdateAfterFetch(int node_id, int worker_id, int request_idx, int base_as_id) = 0;
};

#endif