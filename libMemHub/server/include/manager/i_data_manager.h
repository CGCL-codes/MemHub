
// Interface of data_manager.

#ifndef ICACHE_BACKEND_H
#define ICACHE_BACKEND_H

#include "../../../local_communication/include/msg_info.h"
#include "../utils/util.h"
#include "../../../local_communication/include/semaphore.h"

class CIDataManager
{
public:
    virtual void remote_request_file(int request_file_idx, FileInfo *file_info) = 0;
    virtual int get_file_chunk_group_id(int file_idx) = 0;
    virtual std::vector<Semaphore *> get_request_server_semaphores() = 0;
    virtual bool CheckAndGet(int request_file_idx, FileInfo *file_info) = 0;
    virtual bool CacheFile(FileInfo *file_info) = 0;
};

#endif