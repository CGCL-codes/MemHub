
// File system management backend in MemHub.

#ifndef FILESYSTEM_BACKEND_H
#define FILESYSTEM_BACKEND_H

#include "memory_backend.h"
#include "../../../local_communication/include/msg_info.h"
#include "../utils/util.h"

typedef struct
{
    std::vector<int> v_file_idx;
    std::vector<unsigned long> v_file_size;
    std::vector<char *> v_file_content;
} ChunkInfo;

class CFilesystemBackend
{
public:
    CFilesystemBackend(const std::string &dataset_path, int chunk_size = 256, std::string chunk_root = " ");

    void get(int file_idx, char *dst);
    std::string get_chunk_filepath(int chunk_id) const;
    int get_chunk_num() const;
    int get_chunk_size(int chunk_id) const;
    int get_file_chunk_id(int file_idx) const;
    int get_file_idx_by_chunk(int chunk_id, int file_pos) const;
    int get_file_label_index(int file_idx) const;
    int get_file_num() const;
    std::string get_file_path(int file_idx);
    unsigned long get_file_size(int file_idx) const;
    std::string get_label_name(int label_index) const;

    std::vector<int> FillMemory(int chunk_id, CMemoryBackend *cache, int chunk_group_id, std::vector<std::unordered_set<int>> &mem_loc_unfilled_chunks_map);

private:
    std::string m_path;
    int m_chunk_size;
    std::string m_chunk_root;
    int m_file_num;
    int m_chunk_num;
    std::string m_file_info_map_path;
    std::string m_label_info_path;
    std::vector<std::vector<int>> m_chunk_files_map; // <chunk_id, <files>>
    std::vector<int> m_file_chunk_map;               // <file_idx, chunk_id>
    std::vector<std::string> m_file_path_map;        //<file_idx, file_path>
    std::vector<int> m_file_label_map;               //<file_idx, label_idx>
    std::vector<std::string> m_label_idx_name_map;   //<label_idx, label_name>
    std::vector<int> m_file_size_map;                //<file_idx, file_size>

    void LoadMetaData();
    void InitMetaData();
    void InitChunkFiles();
    void ReadMetaData();
    void WriteMetaData();
};

bool is_regular_file(const char *path);
void shuffle_vector(std::vector<int> &vc);

#endif