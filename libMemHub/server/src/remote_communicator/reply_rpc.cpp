#include "../../include/remote_communicator/reply_rpc.h"
#include <unistd.h>

CReplyService::CReplyService(CIDataManager *data_manager, CISampler *sampler, int node_id, int nodes_num, CConfiguration *configuration, CICommunicatorBackend *communicator_backend)
{
    this->m_data_manager = data_manager;
    this->m_sampler = sampler;
    this->m_rank = node_id;
    this->m_nodes_num = nodes_num;
    this->m_configuration = configuration;
    this->m_communicator_backend = communicator_backend;

    this->m_max_check_num = this->m_configuration->get_max_check_num();
    this->m_max_reply_num = this->m_configuration->get_max_reply_num();
}

Status CReplyService::RequestMultiFiles(ServerContext *context, const MultiFilesRequest *multi_files_request, ServerWriter<MutiFilesReply> *writer)
{
    int reply_num = 0;
    int request_node_id = multi_files_request->node_id();
    // Keep track of all worker_ids and their corresponding request_idxs in this request
    std::vector<std::pair<int, int>> worker_base_request_idx_infos;

    for (const FileRequest &request : multi_files_request->file_request())
    {

        MutiFilesReply multi_files_reply;
        int base_request_idx = request.request_idx();
        int worker_id = request.worker_id();

        worker_base_request_idx_infos.push_back(std::pair<int, int>(worker_id, base_request_idx));

        // Remove the data returned by requests prior to base_request_idx from the FetchQueue, as they have been used by the requesting node.
        this->m_sampler->DealFetchQueue(request_node_id, worker_id, base_request_idx);

        this->m_sampler->mem_loc_mtx_lock(request_node_id, base_request_idx);

        // Check if the data for base_request_idx has been fetched yet.
        bool is_fetched = this->m_sampler->is_fetched(request_node_id, base_request_idx);

        if (is_fetched)
        {
            this->m_sampler->mem_loc_mtx_unlock(request_node_id, base_request_idx);

            // If the data for base_request_idx has already been fetched.
            FileReply *reply = multi_files_reply.add_file_reply();
            reply->set_worker_id(worker_id);
            reply->set_content(" ");
            reply->set_reply_file_id(-1);
            reply->set_file_size(-1);
            reply->set_label_idx(-1);
            reply->set_request_idx(base_request_idx);
            multi_files_reply.set_reply_file_num(0);
        }
        else
        {
            // Fetch data.
            FileInfo *file_info = new FileInfo();
            int request_file_idx = this->m_sampler->get_file_idx(request_node_id, base_request_idx);
            this->m_data_manager->remote_request_file(request_file_idx, file_info);

            // Notify the sampler that the data has been fetched.
            this->m_sampler->UpdateAfterFetch(request_node_id, worker_id, base_request_idx, base_request_idx);
            this->m_sampler->mem_loc_mtx_unlock(request_node_id, base_request_idx);

            FileReply *reply = multi_files_reply.add_file_reply();
            reply->set_worker_id(worker_id);
            reply->set_content(file_info->file_content, file_info->file_size);
            reply->set_reply_file_id(file_info->cache_file_idx);
            reply->set_file_size(file_info->file_size);
            reply->set_label_idx(file_info->label_index);
            reply->set_request_idx(base_request_idx);

            multi_files_reply.set_reply_file_num(1);
            reply_num++;

            delete[] file_info->file_content;
            file_info->file_content = nullptr;
            delete file_info;
            file_info = nullptr;
        }

        writer->Write(multi_files_reply);
    }

    // Fast Prefetch Algorithm
    MutiFilesReply multi_files_reply;
    for (auto info : worker_base_request_idx_infos)
    {
        int worker_id = info.first;
        int base_request_idx = info.second;
        int request_idx = base_request_idx;

        for (int prefetch_check_idx = 0; prefetch_check_idx < this->m_max_check_num; ++prefetch_check_idx)
        {
            if (reply_num == this->m_max_reply_num)
            {
                break;
            }
            request_idx = this->m_sampler->get_worker_next_request_idx(request_node_id, worker_id, request_idx);
            // Break if the last element of the random access sequence has been reached during traversal.
            if (request_idx == -1)
            {
                break;
            }

            // // 正确性检查
            // bool is_correct_request_idx = this->m_sampler->is_correct_request_idx(request_node_id, request_idx);
            // if (is_correct_request_idx == false)
            // {
            //     break;
            // }

            int request_file_idx = this->m_sampler->get_file_idx(request_node_id, request_idx);
            this->m_sampler->mem_loc_mtx_lock(request_node_id, request_idx);
            // Check if the data corresponding to this request_id has already been fetched
            bool is_fetched = this->m_sampler->is_fetched(request_node_id, request_idx);
            if (is_fetched)
            {
                this->m_sampler->mem_loc_mtx_unlock(request_node_id, request_idx);
                continue;
            }

            // Check if the data fetched from the target memory location during the last fetch has been used by the requesting node.
            // In other words, check if the target memory location is empty.
            bool is_last_reply_used = this->m_sampler->is_last_reply_used(request_node_id, worker_id, request_idx, base_request_idx);
            if (is_last_reply_used == false)
            {
                this->m_sampler->mem_loc_mtx_unlock(request_node_id, request_idx);
                continue;
            }

            // Check if the target memory location has cached data
            FileInfo *file_info = new FileInfo();
            bool success = this->m_data_manager->CheckAndGet(request_file_idx, file_info);
            if (success == true)
            {
                // Notify the sampler that the data has been fetched.
                // TODO Merge this function into UpdateAfterFetch.
                this->m_sampler->set_node_mem_loc_last_request_data_info(request_node_id, worker_id, request_idx);
                this->m_sampler->UpdateAfterFetch(request_node_id, worker_id, request_idx, base_request_idx);
                this->m_sampler->mem_loc_mtx_unlock(request_node_id, request_idx);

                FileReply *reply = multi_files_reply.add_file_reply();
                reply->set_worker_id(worker_id);
                reply->set_content(file_info->file_content, file_info->file_size);
                reply->set_reply_file_id(file_info->cache_file_idx);
                reply->set_file_size(file_info->file_size);
                reply->set_label_idx(file_info->label_index);
                reply->set_request_idx(request_idx);

                delete[] file_info->file_content;
                file_info->file_content = nullptr;
                delete file_info;
                file_info = nullptr;

                ++reply_num;
            }
            else
            {
                this->m_sampler->mem_loc_mtx_unlock(request_node_id, request_idx);
                delete file_info;
            }
        }
    }
    multi_files_reply.set_reply_file_num(reply_num);

    writer->Write(multi_files_reply);

    return Status::OK;
}

Status CReplyService::SayNewEpoch(ServerContext *context, const NewEpochRequest *request, NewEpochReply *reply)
{
    int request_node_id = request->node_id();
    int synch_id = request->synch_id();
    this->m_communicator_backend->set_node_status(request_node_id, NODEREADY, synch_id);
    int node_status = this->m_communicator_backend->get_node_status(request_node_id, synch_id);
    reply->set_node_status(node_status);
    std::cout << "@@@CRpcServer: SayNewEpoch node" << this->m_rank << " reveive ready signal from node" << request_node_id << std::endl;
    return Status::OK;
}

Status CReplyService::SynchNewEpoch(ServerContext *context, const NewEpochRequest *request, NewEpochReply *reply)
{
    int request_node_id = request->node_id();
    int synch_id = request->synch_id();

    int node_status = this->m_communicator_backend->get_node_status(request_node_id, synch_id);
    reply->set_node_status(node_status);
    return Status::OK;
}
