#ifndef UTIL_H
#define UTIL_H
#include <iostream>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <cmath>
#include <thread>
#include <string.h>
#include <unordered_map>
#include <random>
#include <algorithm>
#include <semaphore.h>
#include <mutex>
#include <iomanip>
#include <unistd.h>
#include <codecvt>
#include <locale>
#include <vector>
#include <time.h>
#include <sstream>
#include <libgen.h>
#include <dirent.h>
#include <unordered_set>
#include <iterator>
#include <fcntl.h>
#include <chrono>
#include <stdexcept>
#include <queue>
#include <memory>
#include <sys/time.h>
#include <chrono>

/**
 * Status of training node.
 */
// Indicates that the node is not ready for synchronization.
#define NODEUNREADY 1135
// Indicates that the node is ready for synchronization.
#define NODEREADY 1136
// Indicates that the node has completed synchronization.
#define NODERUN 1137

/**
 * Status of memory location.
 */
// Indicates that data is cached in this memory location.
#define BUFFER_ON 25
// Indicates that this memory location does not cache any data.
#define BUFFER_OFF 26
// Indicates that the data cached in this memory location is currently being used(read).
#define BUFFER_USING 27

// The maximum data size for GRPC cross-node data sharing.
#define MAX_MESSAGE_LENGTH 128 * 1024 * 1024 // 128MB

typedef struct FileInfo
{
    int cache_file_idx;
    char *file_content;
    int label_index;
    unsigned long file_size;
    int request_idx;
    bool operator==(const FileInfo p)
    {
        return (this->cache_file_idx == p.cache_file_idx) && (this->file_content == p.file_content) && (this->label_index == p.label_index) && (this->file_size == p.file_size);
    }
} FileInfo;

typedef struct RequestDataInfo
{
    int worker_id;
    int request_idx;
    int file_idx;
    int chunk_group_id;
    bool operator==(const RequestDataInfo p)
    {
        return (this->worker_id == p.worker_id) && (this->request_idx == p.request_idx) && (this->file_idx == p.file_idx) && (this->chunk_group_id == p.chunk_group_id);
    }
} RequestDataInfo;

typedef struct RemoteRequestInfo
{
    int worker_id;
    int request_idx;
    FileInfo *file_info;

    RemoteRequestInfo(int w, int a, FileInfo *f) : worker_id(w), request_idx(a), file_info(f) {}
} RemoteRequestInfo;

struct hash_pair
{
    template <class T1, class T2>
    size_t operator()(const std::pair<T1, T2> &p) const
    {
        auto hash1 = std::hash<T1>{}(p.first);
        auto hash2 = std::hash<T2>{}(p.second);
        return hash1 ^ hash2;
    }
};
using namespace std;
#endif