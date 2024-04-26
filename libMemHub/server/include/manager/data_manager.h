
/**
 * Central hub for data management in MemHub.
 *
 * Handles interactions with cs_port above, memory, and file system below,
 * and communicates horizontally with other nodes via remote_communicator.
 */

#ifndef DATA_MANAGER_H
#define DATA_MANAGER_H

#include "memory_backend.h"
#include "filesystem_backend.h"
#include "i_data_manager.h"
#include "../utils/util.h"

#include "../../../local_communication/include/msg_info.h"
#include "../../../local_communication/include/semaphore.h"

#include "../random_sampler/i_sampler.h"
#include "../remote_communicator/communicator_backend.h"

class CDataManager : public CIDataManager
{
public:
    CDataManager(const std::string &dataset_path, int chunk_size, int batch_size, float cache_ratio,
                 int nodes_num, int rank, int worker_num, std::string chunk_root = " ");
    ~CDataManager();

    int get_file_num();
    int get_mem_loc_num();
    int get_chunk_group_num();
    int get_file_idx(int request_idx);
    int get_file_mem_loc(int file_idx);
    int get_file_chunk_group_id(int file_idx);
    void get_file_from_mem(int mem_loc, FileInfo *file_info);
    std::vector<Semaphore *> get_request_server_semaphores();
    std::vector<int> get_chunk_group_unfilled_mem_locs(int chunk_group_id);

    void set_communicator_backend(CCommunicatorBackend *communicator_backend);
    void set_sampler(CISampler *sampler);

    void remote_request_file(int file_idx, FileInfo *file_info);
    bool FillMemory(int file_idx);
    void request_from_remote(RequestDataInfo request_data_info, FileInfo *file_info);

    bool CheckMemoryOn(int file_idx, int request_idx = -1);
    bool CheckMemoryOff(int file_idx);
    bool CheckAndGet(int file_idx, FileInfo *file_info);
    bool CheckCurNode(int file_idx);
    bool CacheFile(FileInfo *file_info);
    void new_epoch();

    void request_file(RequestDataInfo &request_data_info, FileInfo *file_info);

private:
    int m_batch_size;
    float m_cache_ratio;
    int m_chunk_num;
    int m_chunk_size;
    int m_chunk_map_num;
    int m_chunk_group_num; // Number of chunks groups.
    float m_chunk_map_ratio;
    std::string m_chunk_root;
    std::string m_dataset_path;
    int m_debug_log;
    int m_file_num;
    int m_mem_loc_mapped_data_num; // Number of data mapped to one memory location.
    int m_mem_loc_num;
    int m_nodes_num;
    int m_rank;
    int m_worker_num;

    std::vector<int> m_chunk_mem_loc_num_map;                               // Number of memory locations each data chunk is mapped to.
    std::vector<std::mutex *> m_chunk_group_mtx_map;                        // Mutex corresponding to each chunk group.
    std::vector<std::vector<int>> m_chunk_group_chunks_map;                 // Data chunks mapped to the same chunk group.
    std::vector<std::unordered_set<int>> m_chunk_group_mem_locs_map;        // Memory locations each chunk group is mapped to.
    std::vector<int> m_mem_loc_chunk_group_map;                             // Chunk group mapped to each memory location.
    std::vector<std::unordered_set<int>> m_mem_loc_chunks_map;              // Data chuns mapped to each memory location.
    std::vector<int> m_file_mem_loc_map;                                    // Memory location each training data is mapped to.
    std::vector<int> m_file_chunk_group_map;                                // Chunk group each training data is mapped to.
    std::vector<Semaphore *> m_request_server_semaphores;                   // Semaphore used by each worker when requesting data from other nodes.
    std::vector<int> m_run_chunk_unfilled_mem_loc_num_map;                  // Number of unfilled memory locations for each data chunk during each epoch of the training process.
    std::vector<std::unordered_set<int>> m_run_mem_loc_unfilled_chunks_map; // Data chunks unfilled to each memory location during each epoch of the training process.

    CMemoryBackend *m_mem_backend;
    CFilesystemBackend *m_fs_backend;
    CCommunicatorBackend *m_communicator_backend;
    CISampler *m_sampler;
    void Init();
    void InitMaps();
    void InitMtxs();
    void InitCache();
    void InitSemaphores();
    void InitFilsystemBackend();
};

#endif