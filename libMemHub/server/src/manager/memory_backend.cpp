#include "../../include/manager/memory_backend.h"

CMemoryBackend::CMemoryBackend(int mem_loc_num)
{
    this->m_mem_loc_num = mem_loc_num;
    this->Init();
}

CMemoryBackend::~CMemoryBackend()
{
}
void CMemoryBackend::Init()
{

    this->m_memory = std::vector<char *>(this->m_mem_loc_num);
    for (int mem_loc = 0; mem_loc < this->m_mem_loc_num; ++mem_loc)
    {
        this->m_mem_loc_file_idx_map.push_back(-1);
        this->m_mem_loc_file_size_map.push_back(0);
        this->m_mem_loc_status_map.push_back(BUFFER_OFF);
        std::mutex *mtx = new std::mutex;
        this->m_mem_loc_mtx_map.push_back(mtx);
        this->m_mem_loc_request_idx_map.push_back(-1);
    }
}

/**
 * Checks if the specified memory location is cached with data.
 * If mem_loc_request_idx is set for this memory location, it also verifies if request_idx equals mem_loc_request_idx.
 * If the result is true, sets the status of this memory location to BUFFER_USING to prevent other workers from reading the data at this memory location.
 */

bool CMemoryBackend::CheckMemOn(int mem_loc, int request_idx)
{

    if (this->m_mem_loc_request_idx_map[mem_loc] != -1)
    {
        if (request_idx != this->m_mem_loc_request_idx_map[mem_loc])
        {
            return false;
        }
    }

    if (this->m_mem_loc_status_map[mem_loc] == BUFFER_ON)
    {
        this->m_mem_loc_status_map[mem_loc] = BUFFER_USING;
        return true;
    }
    else
    {
        return false;
    }
}

/**
 * Checks if the specified memory location does not cache any data.
 */
bool CMemoryBackend::CheckMemOff(int mem_loc)
{
    this->m_mem_loc_mtx_map[mem_loc]->lock();
    int status = this->m_mem_loc_status_map[mem_loc];
    bool result = false;
    if (status == BUFFER_OFF)
    {
        result = true;
    }
    this->m_mem_loc_mtx_map[mem_loc]->unlock();
    return result;
}

/**
 * Retrieves the file_idx of the data cached in this memory location.
 */

int CMemoryBackend::get_mem_loc_file_idx(int mem_loc)
{
    return this->m_mem_loc_file_idx_map[mem_loc];
}

/**
 * Reads the data cached in this memory location and releases the memory space.
 */

void CMemoryBackend::get(int mem_loc, char *dst)
{
    unsigned long file_size = this->m_mem_loc_file_size_map[mem_loc];
    memcpy(dst, this->m_memory[mem_loc], file_size);

    delete[] this->m_memory[mem_loc];
    this->m_memory[mem_loc] = nullptr;
    this->m_mem_loc_status_map[mem_loc] = BUFFER_OFF;
    this->m_mem_loc_request_idx_map[mem_loc] = -1;
}

// Cache file into the target memory location.
bool CMemoryBackend::CacheFile(int mem_loc, FileInfo *file_info)
{
    this->m_mem_loc_mtx_map[mem_loc]->lock();
    this->m_memory[mem_loc] = new char[file_info->file_size];
    memcpy(this->m_memory[mem_loc], file_info->file_content, file_info->file_size);
    this->m_mem_loc_file_idx_map[mem_loc] = file_info->cache_file_idx;
    this->m_mem_loc_file_size_map[mem_loc] = file_info->file_size;
    this->m_mem_loc_status_map[mem_loc] = BUFFER_ON;
    this->m_mem_loc_request_idx_map[mem_loc] = file_info->request_idx;
    this->m_mem_loc_mtx_map[mem_loc]->unlock();
    return true;
}

void CMemoryBackend::lock_mem_loc(int mem_loc)
{
    this->m_mem_loc_mtx_map[mem_loc]->lock();
}

void CMemoryBackend::unlock_mem_loc(int mem_loc)
{
    this->m_mem_loc_mtx_map[mem_loc]->unlock();
}

void CMemoryBackend::set_mem_loc_info(int mem_loc, int file_idx, int file_size, int buffer_status)
{
    this->m_mem_loc_file_idx_map[mem_loc] = file_idx;
    this->m_mem_loc_file_size_map[mem_loc] = file_size;
    this->m_mem_loc_status_map[mem_loc] = buffer_status;
}

int CMemoryBackend::get_mem_loc_status(int mem_loc)
{
    return this->m_mem_loc_status_map[mem_loc];
}

/**
 * Dynamically allocates memory space of size file_size for the specified memory location.
 */
char *CMemoryBackend::AllocateMemorySpace(int mem_loc, unsigned long file_size)
{
    this->m_memory[mem_loc] = new char[file_size];
    return this->m_memory[mem_loc];
}
