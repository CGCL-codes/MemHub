
// Memory management backend in MemHub.

#ifndef MEMORY_BACKEND_H
#define MEMORY_BACKEND_H
#include "../../../local_communication/include/msg_info.h"
#include "../utils/util.h"

class CMemoryBackend
{
public:
    CMemoryBackend(int mem_loc_num);
    ~CMemoryBackend();

    bool CheckMemOn(int mem_loc, int request_idx = -1);
    bool CheckMemOff(int mem_loc);
    bool CacheFile(int mem_loc, FileInfo *file_info);
    char *AllocateMemorySpace(int mem_loc, unsigned long file_size);

    void get(int mem_loc, char *dst);
    int get_mem_loc_status(int mem_loc);
    int get_mem_loc_file_idx(int mem_loc);
    void set_mem_loc_info(int mem_loc, int file_idx, int file_size, int buffer_status);

    void lock_mem_loc(int mem_loc);
    void unlock_mem_loc(int mem_loc);

private:
    int m_mem_loc_num;                           // Number of memmory locations;
    std::vector<char *> m_memory;                // <memory location, cached data>
    std::vector<int> m_mem_loc_status_map;       // Status of each memory location: BUFFER_ON(25), BUFFER_OFF(26), BUFFER_USING(27)
    std::vector<int> m_mem_loc_file_idx_map;     // File index of the data cached in each memory location.
    std::vector<int> m_mem_loc_file_size_map;    // File size of the data cached in each memory location.
    std::vector<int> m_mem_loc_request_idx_map;  // The request_idx triggering a memory miss for each memory location. Ensure that when a memory miss occurs at this location, the data at this location can only be read by the request triggering the memory miss.
    std::vector<std::mutex *> m_mem_loc_mtx_map; // Mutex of each memory location.

    void Init();
};

#endif