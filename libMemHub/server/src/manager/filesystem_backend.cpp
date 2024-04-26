
#include "../../include/manager/filesystem_backend.h"

CFilesystemBackend::CFilesystemBackend(const std::string &dataset_path, int chunk_size, std::string chunk_root)
{
    this->m_path = dataset_path;
    this->m_chunk_size = chunk_size;
    this->m_chunk_root = chunk_root;
    this->LoadMetaData();
    this->InitChunkFiles();
}

/**
 * Load meta data information of the dataset.
 * If the meta data files exist,
 * read them directly. Otherwise, init the meta data files and write them
 * to the file system.
 * Meta data files containing information about the dataset:
 *      1. label_info_map.txt:
 *              The first line of this file is: <file_num, chunk_num>.
 *              After that is the mapping of label indices to label names.
 *                Format: <label_idx, label_name>
 *
 *      2. file_info_chunk< chunk_size >map.txt: Mapping of file indices to file paths, label indices, file sizes, and data chunk indices.
 *                   Format: <file_idx, file_path, label_idx, file_size, chunk_id>
 *
 * For an original dataset stored at "/dataset/imagenet/train/class1/xyz.jpeg",
 * the meta data files are stored at:
 *   - "/dataset/imagenet/MemHub_MetaData/label_info_map.txt"
 *   - "/dataset/imagenet/MemHub_MetaData/file_info_chunk<chunk_size>_map.txt"
 *
 * where <chunk_size> is the size of the data chunk.

 * Based on the meta data information of dataset, MemHub will generate 5  mappings:
 *
 *      (1)file_path_map-> file_idx:file_path
 *      (2)file_label_map-> file_idx: label_idx
 *      (3)label_idx_name_map-> label_idx: label_name
 *      (4)file_chunk_map-> file_idx:chunk_id
 *      (5)chunk_files_map-> chunk_id: file_idxs
*/

void CFilesystemBackend::LoadMetaData()
{
    std::string dataset_path = this->m_path;
    std::string map_root = dirname((char *)dataset_path.c_str());
    dataset_path = this->m_path;

    std::string map_subroot = basename((char *)dataset_path.c_str());
    map_root += "/MemHub_MetaData/" + map_subroot;

    if (access(map_root.c_str(), 0) == -1)
    {
        mkdir(map_root.c_str(), S_IRWXU);
    }
    this->m_label_info_path = map_root + "/label_info_map.txt";
    this->m_file_info_map_path = map_root + "/file_info_chunk" + std::to_string(this->m_chunk_size) + "map.txt";

    if (access(this->m_file_info_map_path.c_str(), 0) == -1 || access(this->m_label_info_path.c_str(), 0) == -1)
    {
        this->InitMetaData();
        this->WriteMetaData();
    }
    else
    {
        this->ReadMetaData();
    }
}

/**
 * Init 5 meta data mappings of dataset.
 */
void CFilesystemBackend::InitMetaData()
{
    int label_idx = 0;
    struct dirent **namelist;
    int n = scandir(this->m_path.c_str(), &namelist, 0, alphasort);

    if (n < 0)
    {
        std::cerr << "memery error " << std::endl;
        exit(0);
    }
    for (int i = 0; i < n; ++i)
    {
        std::cout << "InitMetaData: init file info meta data, total data class:" << n << " now class id:" << i << std::endl;

        if (namelist[i]->d_name[0] != '.' && namelist[i]->d_type == DT_DIR)
        {
            int file_idx = 0;
            std::string label_name = namelist[i]->d_name;
            this->m_label_idx_name_map.push_back(label_name);
            std::string label_path = this->m_path + "/" + label_name;

            struct dirent **sub_namelist;
            int sub_n = scandir(label_path.c_str(), &sub_namelist, 0, alphasort);
            if (sub_n < 0)
            {
                std::cerr << "memery error " << std::endl;
                exit(0);
            }
            for (int j = 0; j < sub_n; ++j)
            {
                std::string file_name = sub_namelist[j]->d_name;
                std::string file_path = label_path + "/" + file_name;
                if (is_regular_file(file_path.c_str()))
                {
                    this->m_file_path_map.push_back(file_path);
                    this->m_file_label_map.push_back(label_idx);
                    int fd = open(file_path.c_str(), O_RDONLY);
                    struct stat stbuf;
                    fstat(fd, &stbuf);
                    close(fd);
                    if (!S_ISREG(stbuf.st_mode))
                    {
                        // Not a regular file
                        continue;
                    }
                    this->m_file_size_map.push_back(stbuf.st_size);
                    file_idx += 1;
                }
                else
                {
                    std::cout << "Not DT_REG: " << sub_namelist[j]->d_name << " type:" << sub_namelist[j]->d_type << std::endl;
                }
            }
            label_idx += 1;
        }
    }
    this->m_file_num = static_cast<int>(this->m_file_path_map.size());

    std::vector<int> v_file_idxs(this->m_file_num, 0);
    for (int idx = 0; idx < this->m_file_num; ++idx)
    {
        v_file_idxs[idx] = idx;
    }
    shuffle_vector(v_file_idxs);

    std::vector<int> v_chunk_files;
    this->m_file_chunk_map = std::vector<int>(this->m_file_num, 0);
    int chunk_id = 0;
    for (int idx = 0; idx < this->m_file_num; ++idx)
    {
        int file_idx = v_file_idxs[idx];
        if (static_cast<int>(v_chunk_files.size()) < this->m_chunk_size)
        {
            v_chunk_files.push_back(file_idx);
        }
        else
        {
            std::cout << file_idx << std::endl;
            std::cout << "InitMetaData:  init chunk info meta data, now chunk id: " << chunk_id << std::endl;
            this->m_chunk_files_map.push_back(v_chunk_files);
            chunk_id += 1;
            v_chunk_files.clear();
            v_chunk_files.push_back(file_idx);
        }
        this->m_file_chunk_map[file_idx] = chunk_id;
    }
    if (static_cast<int>(v_chunk_files.size()) != 0)
    {
        this->m_chunk_files_map.push_back(v_chunk_files);
    }
    this->m_chunk_num = static_cast<int>(this->m_chunk_files_map.size());
}

/**
 * Write 2 meta data files of dataset into filesystem.
 */
void CFilesystemBackend::WriteMetaData()
{
    std::ofstream map_stream;
    // Write label_info_map.txt
    if (static_cast<int>(this->m_label_idx_name_map.size()) > 0)
    {
        map_stream.open(this->m_label_info_path, std::ofstream::out | std::ofstream::trunc);
        for (int label_idx = 0; label_idx < static_cast<int>(this->m_label_idx_name_map.size()); ++label_idx)
        {
            map_stream << label_idx << "," << this->m_label_idx_name_map[label_idx] << std::endl;
        }
        map_stream.close();
    }

    // Wrtite file_info_chunk<chunk_size> map.txt
    if (static_cast<int>(this->m_file_path_map.size()) > 0 && static_cast<int>(this->m_file_label_map.size()) > 0 &&
        static_cast<int>(this->m_file_size_map.size()) > 0)
    {
        map_stream.open(this->m_file_info_map_path, std::ofstream::out | std::ofstream::trunc);
        map_stream << this->m_file_num << "," << this->m_chunk_num << std::endl;
        for (int chunk_id = 0; chunk_id < this->m_chunk_num; ++chunk_id)
        {
            for (auto file_idx : this->m_chunk_files_map[chunk_id])
            {
                map_stream << file_idx << "," << this->m_file_path_map[file_idx] << "," << this->m_file_label_map[file_idx] << "," << this->m_file_size_map[file_idx] << "," << chunk_id << std::endl;
            }
        }
        map_stream.close();
    }
}

// Read meta data files from filesystem and generate the 5 meta data mappings.
void CFilesystemBackend::ReadMetaData()
{
    std::string line;
    std::ifstream label_map_stream(this->m_label_info_path);
    while (std::getline(label_map_stream, line))
    {
        std::stringstream line_stream(line);
        std::string label_index;
        std::string label_name;
        std::getline(line_stream, label_index, ',');
        line_stream >> label_name;
        this->m_label_idx_name_map.push_back(label_name);
    }
    label_map_stream.close();

    std::ifstream file_map_stream(this->m_file_info_map_path);
    std::getline(file_map_stream, line);
    std::stringstream line_stream(line);
    std::string file_num;
    std::getline(line_stream, file_num, ',');
    line_stream >> this->m_chunk_num;
    this->m_file_num = atoi(file_num.c_str());

    this->m_file_path_map = std::vector<std::string>(this->m_file_num);
    this->m_file_label_map = std::vector<int>(this->m_file_num);
    this->m_file_size_map = std::vector<int>(this->m_file_num);
    this->m_file_chunk_map = std::vector<int>(this->m_file_num);
    this->m_chunk_files_map = std::vector<std::vector<int>>(this->m_chunk_num);

    while (std::getline(file_map_stream, line))
    {
        std::stringstream line_stream(line);
        std::string s_file_idx;
        std::string file_path;
        std::string label_index;
        std::string file_size;
        int chunk_id;
        std::getline(line_stream, s_file_idx, ',');
        std::getline(line_stream, file_path, ',');
        std::getline(line_stream, label_index, ',');
        std::getline(line_stream, file_size, ',');
        line_stream >> chunk_id;

        int file_idx = atoi(s_file_idx.c_str());
        this->m_file_path_map[file_idx] = file_path;
        this->m_file_label_map[file_idx] = atoi(label_index.c_str());
        this->m_file_size_map[file_idx] = atoi(file_size.c_str());
        this->m_file_chunk_map[file_idx] = chunk_id;
        this->m_chunk_files_map[chunk_id].push_back(file_idx);
    }
    file_map_stream.close();
}

void CFilesystemBackend::InitChunkFiles()
{

    if (m_chunk_size > 1)
    {
        if (this->m_chunk_root == " ")
        {
            std::string dataset_path = this->m_path;
            this->m_chunk_root = dirname((char *)dataset_path.c_str());
            dataset_path = this->m_path;
            std::string map_subroot = basename((char *)dataset_path.c_str());

            this->m_chunk_root += "/MemHub_Chunks/" + map_subroot + "/" + std::to_string(this->m_chunk_size);
        }
        if (access(this->m_chunk_root.c_str(), 0) == -1)
        {
            mkdir(this->m_chunk_root.c_str(), S_IRWXU);
        }
        struct dirent **namelist;
        int n = scandir(this->m_chunk_root.c_str(), &namelist, 0, alphasort);

        if (n < 0)
        {
            std::cerr << "memery error " << std::endl;
            exit(0);
        }
        else if (n - 2 != static_cast<int>(this->m_chunk_files_map.size()))
        {
            std::cerr << "CFilesystemBackend::InitChunkFiles begin write chunks" << std::endl;

            for (int chunk_id = 0; chunk_id < static_cast<int>(this->m_chunk_files_map.size()); ++chunk_id)
            {
                std::cerr << "CFilesystemBackend::InitChunkFiles  write chunk " + std::to_string(chunk_id) << " total:" << this->m_chunk_files_map.size() << std::endl;
                FILE *filep = fopen(this->get_chunk_filepath(chunk_id).c_str(), "wb");
                for (int idx = 0; idx < static_cast<int>(this->m_chunk_files_map[chunk_id].size()); idx++)
                {
                    int file_idx = m_chunk_files_map[chunk_id][idx];
                    unsigned long file_size = this->get_file_size(file_idx);
                    char *content = new char[file_size];
                    this->get(file_idx, content);
                    fwrite(content, file_size, 1, filep);
                    delete[] content;
                }
                fclose(filep);
            }
        }
    }
    // Once the data chunks are generated, the file_path_map will no longer be needed.
    std::vector<std::string>().swap(this->m_file_path_map);
}

std::string CFilesystemBackend::get_file_path(int file_idx)
{
    return this->m_file_path_map[file_idx];
}

int CFilesystemBackend::get_file_label_index(int file_idx) const
{
    return this->m_file_label_map[file_idx];
}

std::string CFilesystemBackend::get_label_name(int label_index) const
{
    return this->m_label_idx_name_map[label_index];
}

int CFilesystemBackend::get_file_num() const
{
    return this->m_file_num;
}

int CFilesystemBackend::get_chunk_num() const
{
    return this->m_chunk_num;
}

unsigned long CFilesystemBackend::get_file_size(int file_idx) const
{
    return this->m_file_size_map[file_idx];
}

void CFilesystemBackend::get(int file_idx, char *dst)
{
    std::string file_path = this->get_file_path(file_idx);
    unsigned long file_size = this->get_file_size(file_idx);
    FILE *f = fopen(file_path.c_str(), "rb");
    fread(dst, file_size, 1, f);
    fclose(f);
}

/**
 * Attempt to load the specified data chunk from the file system and fill memory as much as possible.
 * @param chunk_id The id of the specified data chunk to be load from the file system.
 * @param memory Pointer to the memory backend of MemHub.
 * @param chunk_group_id The id of the chunk group to which the specified data chunk belongs.
 * @param mem_loc_unfilled_chunks_map Unfilled data chunks for all memory locations up to now.
 *
 * @return All the memory locations which are filled with data at this time.
 */
std::vector<int> CFilesystemBackend::FillMemory(int chunk_id, CMemoryBackend *memory, int chunk_group_id, std::vector<std::unordered_set<int>> &mem_loc_unfilled_chunks_map)
{
    FILE *filep = fopen(this->get_chunk_filepath(chunk_id).c_str(), "rb");
    fseek(filep, 0, SEEK_END);
    long chunk_file_size = ftell(filep);
    rewind(filep);

    int idx = 0;
    long file_pos = 0;
    int cache_num = 0;
    int file_idx;
    unsigned long file_size;
    std::vector<int> filled_mem_locs;
    while (file_pos < chunk_file_size)
    {
        file_idx = this->m_chunk_files_map[chunk_id][idx];
        file_size = this->get_file_size(file_idx);
        int mem_loc = chunk_group_id * this->m_chunk_size + idx;

        auto chunk_iter = mem_loc_unfilled_chunks_map[mem_loc].find(chunk_id);
        int status = memory->get_mem_loc_status(mem_loc);
        if (chunk_iter != mem_loc_unfilled_chunks_map[mem_loc].end() && status == BUFFER_OFF)
        {
            fseek(filep, file_pos, SEEK_SET);
            cache_num += 1;
            filled_mem_locs.push_back(mem_loc);
            memory->lock_mem_loc(mem_loc);
            char *cache_pointer = memory->AllocateMemorySpace(mem_loc, file_size);
            fread(cache_pointer, file_size, 1, filep);
            memory->set_mem_loc_info(mem_loc, file_idx, file_size, BUFFER_ON);
            memory->unlock_mem_loc(mem_loc);
        }
        file_pos += file_size;
        idx += 1;
    }
    fclose(filep);
    return filled_mem_locs;
}

std::string CFilesystemBackend::get_chunk_filepath(int chunk_id) const
{
    return this->m_chunk_root + "/" + std::to_string(chunk_id) + ".bin";
}

int CFilesystemBackend::get_file_chunk_id(int file_idx) const
{
    return this->m_file_chunk_map[file_idx];
}

/**
 * Retrieve the number of data contained in the data chunk.
 */
int CFilesystemBackend::get_chunk_size(int chunk_id) const
{
    return static_cast<int>(this->m_chunk_files_map[chunk_id].size());
}

/**
 * Query the file index based on the relative position of the file in the data chunk.
 */
int CFilesystemBackend::get_file_idx_by_chunk(int chunk_id, int file_pos) const
{
    return this->m_chunk_files_map[chunk_id][file_pos];
}

bool is_regular_file(const char *path)
{
    struct stat path_stat;
    if (stat(path, &path_stat) != 0)
    {
        // 处理错误
        perror("stat");
        return false;
    }
    return S_ISREG(path_stat.st_mode);
}

void shuffle_vector(std::vector<int> &vc)
{
    std::random_device rd;
    std::mt19937 rng(rd());
    std::shuffle(vc.begin(), vc.end(), rng);
}
