#include "../../include/manager/data_manager.h"

/**
 * Constructor for CDataManager class.
 *
 * @param dataset_path The path to the origin dataset to be used for training.
 * @param chunk_size The size of data chunks to be processed (files/chunk).
 * @param batch_size The number of samples in each training batch.
 * @param cache_ratio The ratio of data to be cached in memory.
 * @param nodes_num The number of training nodes.
 * @param worker_num The number of workers per training node.
 * @param rank The rank of the current node in the distributed setup.
 * @param chunk_root The root directory for storing data chunks.
 */
CDataManager::CDataManager(const std::string &dataset_path, int chunk_size, int batch_size, float cache_ratio, int nodes_num, int rank, int worker_num, std::string chunk_root)
{
    this->m_dataset_path = dataset_path;
    this->m_chunk_size = chunk_size;
    this->m_batch_size = batch_size;
    this->m_cache_ratio = cache_ratio;
    this->m_nodes_num = nodes_num;
    this->m_worker_num = worker_num;
    this->m_rank = rank;
    this->m_chunk_root = chunk_root;
    this->m_mem_loc_mapped_data_num = ceil(1 / this->m_cache_ratio);
    this->Init();
}

CDataManager::~CDataManager()
{
}

void CDataManager::Init()
{
    this->InitFilsystemBackend();
    this->InitMaps();
    this->InitCache();
    this->InitMtxs();
    this->InitSemaphores();
}

void CDataManager::InitFilsystemBackend()
{
    this->m_fs_backend = new CFilesystemBackend(this->m_dataset_path, this->m_chunk_size, this->m_chunk_root);
    this->m_file_num = this->m_fs_backend->get_file_num();
    this->m_chunk_num = this->m_fs_backend->get_chunk_num();
}

void CDataManager::InitMaps()
{
    // Amount of memory locations.
    this->m_mem_loc_num = 0;
    // A chunk group contains several data chunks.
    int chunk_group_id = 0;
    std::vector<int> chunk_group;
    // Amount of memory locations mapped to each chunk group.
    std::vector<int> chunk_group_mem_loc_num;
    // Distributes each data chunk to chunk groups based on m_mem_loc_mapped_data_num.
    for (int chunk_id = 0; chunk_id < this->m_chunk_num; ++chunk_id)
    {
        if (static_cast<int>(chunk_group.size()) == this->m_mem_loc_mapped_data_num)
        {
            this->m_chunk_group_chunks_map.push_back(chunk_group);
            chunk_group.clear();
            chunk_group_id += 1;
        }
        chunk_group.push_back(chunk_id);

        int chunk_size = this->m_fs_backend->get_chunk_size(chunk_id);

        if (static_cast<int>(chunk_group_mem_loc_num.size()) == chunk_group_id)
        {
            // When processing the first data chunk in the chunk group.
            chunk_group_mem_loc_num.push_back(chunk_size);
            this->m_mem_loc_num += chunk_size;
        }
        else if (chunk_group_mem_loc_num[chunk_group_id] < chunk_size)
        {
            //  When processing a larger data chunk.
            this->m_mem_loc_num += (chunk_size - chunk_group_mem_loc_num[chunk_group_id]);
            chunk_group_mem_loc_num[chunk_group_id] = chunk_size;
        }
        this->m_chunk_mem_loc_num_map.push_back(chunk_size);
    }
    if (static_cast<int>(chunk_group.size()) != 0)
    {
        this->m_chunk_group_chunks_map.push_back(chunk_group);
        chunk_group.clear();
    }
    this->m_chunk_group_num = static_cast<int>(this->m_chunk_group_chunks_map.size());

    this->m_file_mem_loc_map = std::vector<int>(this->m_file_num);
    this->m_file_chunk_group_map = std::vector<int>(this->m_file_num);
    this->m_mem_loc_chunk_group_map = std::vector<int>(this->m_mem_loc_num);
    this->m_mem_loc_chunks_map = std::vector<std::unordered_set<int>>(this->m_mem_loc_num);
    this->m_chunk_group_mem_locs_map = std::vector<std::unordered_set<int>>(this->m_chunk_group_num);
    this->m_run_mem_loc_unfilled_chunks_map = std::vector<std::unordered_set<int>>(this->m_mem_loc_num);
    this->m_run_chunk_unfilled_mem_loc_num_map = std::vector<int>(this->m_chunk_num);
    // Establishes the mapping between training data, data chunks, chunk groups, and memory locations.
    // The first memory location mapped to by the chunk group.
    int chunk_group_base_mem_loc = 0;
    for (int chunk_group_id = 0; chunk_group_id < this->m_chunk_group_num; ++chunk_group_id)
    {
        if (chunk_group_id > 0)
        {
            chunk_group_base_mem_loc += chunk_group_mem_loc_num[chunk_group_id - 1];
        }
        for (auto chunk_id : this->m_chunk_group_chunks_map[chunk_group_id])
        {
            int chunk_size = this->m_fs_backend->get_chunk_size(chunk_id);
            for (int file_pos = 0; file_pos < chunk_size; ++file_pos)
            {
                int mem_loc = chunk_group_base_mem_loc + file_pos;
                int file_idx = this->m_fs_backend->get_file_idx_by_chunk(chunk_id, file_pos);
                this->m_file_mem_loc_map[file_idx] = mem_loc;
                this->m_file_chunk_group_map[file_idx] = chunk_group_id;
                this->m_mem_loc_chunk_group_map[mem_loc] = chunk_group_id;
                this->m_mem_loc_chunks_map[mem_loc].insert(chunk_id);
                this->m_chunk_group_mem_locs_map[chunk_group_id].insert(mem_loc);
            }
        }
    }
}

void CDataManager::InitCache()
{
    this->m_mem_backend = new CMemoryBackend(this->m_mem_loc_num);
}

void CDataManager::InitMtxs()
{
    for (int chunk_group_id = 0; chunk_group_id < this->m_chunk_group_num; ++chunk_group_id)
    {
        std::mutex *mtx = new std::mutex;
        this->m_chunk_group_mtx_map.push_back(mtx);
    }
}

void CDataManager::InitSemaphores()
{
    for (int i = 0; i < this->m_worker_num; ++i)
    {
        this->m_request_server_semaphores.push_back(new Semaphore());
    }
}

void CDataManager::set_sampler(CISampler *sampler)
{
    this->m_sampler = sampler;
}

void CDataManager::set_communicator_backend(CCommunicatorBackend *communicator_backend)
{
    this->m_communicator_backend = communicator_backend;
}

int CDataManager::get_file_idx(int request_idx)
{
    return this->m_sampler->get_file_idx(this->m_rank, request_idx);
}

int CDataManager::get_file_chunk_group_id(int file_idx)
{
    return this->m_file_chunk_group_map[file_idx];
}

int CDataManager::get_mem_loc_num()
{
    return this->m_mem_loc_num;
}

int CDataManager::get_file_num()
{
    return this->m_file_num;
}

std::vector<Semaphore *> CDataManager::get_request_server_semaphores()
{
    return this->m_request_server_semaphores;
}

int CDataManager::get_chunk_group_num()
{
    return this->m_chunk_group_num;
}

int CDataManager::get_file_mem_loc(int file_idx)
{
    return this->m_file_mem_loc_map[file_idx];
}

void CDataManager::get_file_from_mem(int file_idx, FileInfo *file_info)
{
    int mem_loc = this->get_file_mem_loc(file_idx);
    this->m_mem_backend->lock_mem_loc(mem_loc);

    int cached_file_idx = this->m_mem_backend->get_mem_loc_file_idx(mem_loc);
    file_info->file_size = this->m_fs_backend->get_file_size(cached_file_idx);
    file_info->label_index = this->m_fs_backend->get_file_label_index(cached_file_idx);
    file_info->file_content = new char[file_info->file_size];
    file_info->cache_file_idx = cached_file_idx;
    this->m_mem_backend->get(mem_loc, file_info->file_content);

    this->m_mem_backend->unlock_mem_loc(mem_loc);
}

/**
 *  Query for the currently unfilled memory locations mapped to by the specified chunk group.
 */
std::vector<int> CDataManager::get_chunk_group_unfilled_mem_locs(int chunk_group_id)
{
    std::vector<int> unfilled_mem_locs;
    for (auto mem_loc : this->m_chunk_group_mem_locs_map[chunk_group_id])
    {
        bool is_cache_off = this->m_mem_backend->CheckMemOff(mem_loc);
        if (is_cache_off)
        {
            unfilled_mem_locs.push_back(mem_loc);
        }
    }
    return unfilled_mem_locs;
}

/**
 * Check if the memory location corresponding to file_idx is currently cached with data.
 */
bool CDataManager::CheckMemoryOn(int file_idx, int request_idx)
{
    int mem_loc = this->get_file_mem_loc(file_idx);
    this->m_mem_backend->lock_mem_loc(mem_loc);
    bool result = this->m_mem_backend->CheckMemOn(mem_loc, request_idx);
    this->m_mem_backend->unlock_mem_loc(mem_loc);
    return result;
}

/**
 * Check if the memory location corresponding to file_idx is unfilled with data.
 */
bool CDataManager::CheckMemoryOff(int file_idx)
{

    int mem_loc = this->get_file_mem_loc(file_idx);
    return this->m_mem_backend->CheckMemOff(mem_loc);
}

/**
 * Check if the read permission for the training data corresponding to file_idx
 * has been assigned to the current node.
 */
bool CDataManager::CheckCurNode(int file_idx)
{
    int mem_loc = this->get_file_mem_loc(file_idx);
    int chunk_group_id = this->m_mem_loc_chunk_group_map[mem_loc];
    return this->m_sampler->CheckCurNode(chunk_group_id);
}

bool CDataManager::CheckAndGet(int file_idx, FileInfo *file_info)
{
    bool memory_on = this->CheckMemoryOn(file_idx);
    if (memory_on == false)
        return false;
    this->get_file_from_mem(file_idx, file_info);
    return true;
}

/**
 * Cache data in memory.
 * */
bool CDataManager::CacheFile(FileInfo *file_info)
{
    int mem_loc = this->m_file_mem_loc_map[file_info->cache_file_idx];
    return this->m_mem_backend->CacheFile(mem_loc, file_info);
}

/**
 * Enter a new epoch and reset the dynamically changing data information during training.
 */
void CDataManager::new_epoch()
{
    for (int mem_loc = 0; mem_loc < this->m_mem_loc_num; ++mem_loc)
    {
        this->m_run_mem_loc_unfilled_chunks_map[mem_loc] = this->m_mem_loc_chunks_map[mem_loc];
    }
    for (int chunk_id = 0; chunk_id < this->m_chunk_num; ++chunk_id)
    {
        this->m_run_chunk_unfilled_mem_loc_num_map[chunk_id] = this->m_chunk_mem_loc_num_map[chunk_id];
    }
}

/**
 * Fills memory with data.
 *
 * @param file_idx Index of the data triggering the memory miss.
 * @return True if the memory is successfully filled; otherwise, false.
 */

bool CDataManager::FillMemory(int file_idx)
{
    if (this->CheckMemoryOff(file_idx) == true)
    {
        int mem_loc = this->get_file_mem_loc(file_idx);
        int chunk_group_id = this->m_mem_loc_chunk_group_map[mem_loc];
        this->m_chunk_group_mtx_map[chunk_group_id]->lock();
        if (this->m_mem_backend->get_mem_loc_status(mem_loc) == BUFFER_ON)
        {
            this->m_chunk_group_mtx_map[chunk_group_id]->unlock();
            return false;
        }
        else
        {
            // Optimal Chunk Selecting Algorithm.
            int optimal_chunk_id = -1;
            // The amount of data from the optimal data chunk that can be filled into memory this time.
            int optimal_fillable_number = -1;
            // The fill rate of the optimal data chunk this time. (chunk_fill_rate = (chunk_fillable_number * 1.0) / chunk_unfilled_mem_loc_num)
            float optimal_fill_rate = -1.0;

            std::vector<int> unfilled_mem_locs = this->get_chunk_group_unfilled_mem_locs(chunk_group_id);
            std::vector<int> valid_chunks(this->m_run_mem_loc_unfilled_chunks_map[mem_loc].begin(), this->m_run_mem_loc_unfilled_chunks_map[mem_loc].end());
            std::mt19937 rng(std::chrono::system_clock::now().time_since_epoch().count());
            std::shuffle(valid_chunks.begin(), valid_chunks.end(), rng);
            for (auto chunk_id : valid_chunks)
            {
                int chunk_fillable_number = 0;
                for (auto mem_loc : unfilled_mem_locs)
                {
                    if (this->m_run_mem_loc_unfilled_chunks_map[mem_loc].find(chunk_id) != this->m_run_mem_loc_unfilled_chunks_map[mem_loc].end())
                    {
                        chunk_fillable_number += 1;
                    }
                }

                float chunk_fill_rate = 0.0;
                int chunk_unfilled_mem_loc_num = this->m_run_chunk_unfilled_mem_loc_num_map[chunk_id];
                if (chunk_unfilled_mem_loc_num != 0)
                {
                    chunk_fill_rate = (chunk_fillable_number * 1.0) / chunk_unfilled_mem_loc_num;
                }
                if (
                    (optimal_fill_rate < chunk_fill_rate) ||
                    (optimal_fill_rate == chunk_fill_rate && optimal_fillable_number < chunk_fillable_number))
                {
                    optimal_chunk_id = chunk_id;
                    optimal_fillable_number = chunk_fillable_number;
                    optimal_fill_rate = chunk_fill_rate;
                }
            }
            if (optimal_chunk_id != -1)
            {
                // Load the optimal data chunk from file system into memory.
                std::vector<int> filled_mem_locs = this->m_fs_backend->FillMemory(optimal_chunk_id, this->m_mem_backend, chunk_group_id, this->m_run_mem_loc_unfilled_chunks_map);
                // Updates the dynamically changing data information during training.
                for (auto mem_loc : filled_mem_locs)
                {
                    auto optimal_chunk_iter = this->m_run_mem_loc_unfilled_chunks_map[mem_loc].find(optimal_chunk_id);
                    this->m_run_mem_loc_unfilled_chunks_map[mem_loc].erase(optimal_chunk_iter);
                    --this->m_run_chunk_unfilled_mem_loc_num_map[optimal_chunk_id];
                }
            }
        }
        this->m_chunk_group_mtx_map[chunk_group_id]->unlock();
        return true;
    }
    else
    {
        return false;
    }
}

/**
 * Requests data from a remote node.
 */
void CDataManager::remote_request_file(int file_idx, FileInfo *file_info)
{
    bool memory_on = this->CheckMemoryOn(file_idx);
    if (memory_on != true)
    {
        while (memory_on != true)
        {
            this->FillMemory(file_idx);
            memory_on = this->CheckMemoryOn(file_idx);
        }
    }
    this->get_file_from_mem(file_idx, file_info);
}

/**
 * Entry point for receiving data requests.
 * Receives the index of the requested data from the model side: request_idx.
 * Note: request_idx refers to the index of the data requested by this node,
 * which corresponds to the position in the random access sequence, not the actual file_idx of training data.
 * The actual random access sequence is generated by the distributed_sampler in the random_sampler,
 * which is also responsible for mapping request_idx to the corresponding file_idx.
 */
void CDataManager::request_file(RequestDataInfo &request_data_info, FileInfo *file_info)
{
    int request_idx = request_data_info.request_idx;
    int file_idx = this->get_file_idx(request_idx);
    request_data_info.file_idx = file_idx;

    bool memory_hit = true;
    while (true)
    {
        bool memory_on = this->CheckMemoryOn(file_idx, request_idx);
        if (memory_on == false)
        {
            if (memory_hit == true)
            {
                memory_hit = false;
            }
            bool is_cur_node = this->CheckCurNode(file_idx);
            if (is_cur_node == false)
            {
                this->request_from_remote(request_data_info, file_info);

                if (file_info->cache_file_idx == -1)
                {
                    continue;
                }
                else
                {
                    return;
                }
            }
            else
            {
                while (memory_on != true)
                {
                    this->FillMemory(file_idx);
                    memory_on = this->CheckMemoryOn(file_idx);
                }
            }
        }
        this->get_file_from_mem(file_idx, file_info);
        break;
    }
    return;
}

/**
 * Request data from remote node.
 */
void CDataManager::request_from_remote(RequestDataInfo request_data_info, FileInfo *file_info)
{
    int mem_loc = this->get_file_mem_loc(request_data_info.file_idx);
    int chunk_group_id = this->m_mem_loc_chunk_group_map[mem_loc];
    int node_id = this->m_sampler->get_chunk_group_node_id(chunk_group_id);
    this->m_communicator_backend->request_from_remote(node_id, request_data_info, file_info);
    this->m_request_server_semaphores[request_data_info.worker_id]->Wait();
}