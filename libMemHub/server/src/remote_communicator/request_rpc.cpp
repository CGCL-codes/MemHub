#include "../../include/remote_communicator/request_rpc.h"

CRquestRpc::CRquestRpc(std::shared_ptr<Channel> channel, int node_id, int target_node_id, int worker_num)
{
  this->m_node_id = node_id;
  this->m_target_node_id = target_node_id;
  this->m_worker_num = worker_num;
  for (int i = 0; i < 1; ++i)
  {
    this->m_v_stubs.push_back(DBMFRPC::NewStub(channel));
  }
}

void CRquestRpc::RequestMultiFiles(std::vector<RemoteRequestInfo> &remote_request_infos, std::vector<Semaphore *> &semaphores, CIDataManager *data_manager)
{
  // Record each request information to help determine whether the returned data is requested or prefetched.
  std::unordered_map<std::pair<int, int>, FileInfo *, hash_pair> worker_file_info_map;

  // Send request.
  MultiFilesRequest requests;
  requests.set_node_id(this->m_node_id);
  for (auto request_info : remote_request_infos)
  {
    FileRequest *file_request = requests.add_file_request();
    file_request->set_worker_id(request_info.worker_id);
    file_request->set_request_idx(request_info.request_idx);
    std::pair<int, int> worker_request_idx_info(request_info.worker_id, request_info.request_idx);
    worker_file_info_map[worker_request_idx_info] = request_info.file_info;
  }

  ClientContext context;
  std::unique_ptr<ClientReader<MutiFilesReply>> reader(this->m_v_stubs[0]->RequestMultiFiles(&context, requests));

  // Receive replied data
  MutiFilesReply multi_files_reply;
  while (reader->Read(&multi_files_reply))
  {
    for (const FileReply &reply : multi_files_reply.file_reply())
    {
      int request_idx = reply.request_idx();
      int worker_id = reply.worker_id();
      int file_idx = reply.reply_file_id();
      std::pair<int, int> worker_request_idx_info(worker_id, request_idx);

      if (worker_file_info_map.find(worker_request_idx_info) != worker_file_info_map.end())
      {
        // If it is a requested data
        FileInfo *request_file_info = worker_file_info_map[worker_request_idx_info];
        request_file_info->cache_file_idx = file_idx;
        if (request_file_info->cache_file_idx == -1)
        {
          // If the returned cache_file_idx is -1.
          // It indicates that the data corresponding to the request_id has been fetched by the target node.
          // Immediately notify the worker.
          semaphores[worker_id]->Signal();
          continue;
        }
        else
        {
          request_file_info->file_size = reply.file_size();
          request_file_info->label_index = reply.label_idx();
          request_file_info->file_content = new char[request_file_info->file_size];
          std::string content_str = reply.content();
          memcpy(request_file_info->file_content, content_str.c_str(), request_file_info->file_size);
          // Immediately notify the worker to fetch data.
          semaphores[worker_id]->Signal();
        }
      }
      else
      {
        // If it is a prefetched data
        FileInfo *file_info = new FileInfo();
        file_info->cache_file_idx = file_idx;
        file_info->request_idx = request_idx;
        if (file_info->cache_file_idx == -1)
        {
          continue;
        }
        file_info->file_size = reply.file_size();
        file_info->label_index = reply.label_idx();
        file_info->file_content = new char[file_info->file_size];
        std::string content_str = reply.content();

        // Cache the prefteched data into memory.
        memcpy(file_info->file_content, content_str.c_str(), file_info->file_size);
        data_manager->CacheFile(file_info);

        delete[] file_info->file_content;
        file_info->file_content = nullptr;
        delete file_info;
        file_info = nullptr;
      }
    }
  }
}

int CRquestRpc::SayNewEpoch(int synch_id)
{
  NewEpochRequest request;
  NewEpochReply reply;
  ClientContext context;
  request.set_node_id(this->m_node_id);
  request.set_synch_id(synch_id);

  Status status = this->m_v_stubs[0]->SayNewEpoch(&context, request, &reply);
  while (!status.ok())
  {
    std::cout << "~~~CRquestRpc:SayNewEpoch rpc failed." << std::endl;

    std::cout << "~~~CRquestRpc:SayNewEpoch,error" << status.error_code() << ": " << status.error_message()
              << " send request again!" << std::endl;
    status = this->m_v_stubs[0]->SayNewEpoch(&context, request, &reply);
  }
  return reply.node_status();
}

int CRquestRpc::SynchNewEpoch(int synch_id)
{
  NewEpochRequest request;
  NewEpochReply reply;
  ClientContext context;

  request.set_node_id(this->m_node_id);
  request.set_synch_id(synch_id);

  Status status = this->m_v_stubs[0]->SynchNewEpoch(&context, request, &reply);
  while (!status.ok())
  {
    std::cout << "~~~CRquestRpc:SynchNewEpoch rpc failed." << std::endl;

    std::cout << "~~~CRquestRpc:SynchNewEpoch,error" << status.error_code() << ": " << status.error_message()
              << " send request again!" << std::endl;
    status = this->m_v_stubs[0]->SynchNewEpoch(&context, request, &reply);
  }
  return reply.node_status();
}