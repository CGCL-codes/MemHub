#include "../../include/remote_communicator/communicator_backend.h"

CCommunicatorBackend::CCommunicatorBackend(CIDataManager *data_manager, CISampler *sampler, int node_id, int nodes_num, int worker_num)
{
    this->m_data_manager = data_manager;
    this->m_sampler = sampler;
    this->m_rank = node_id;
    this->m_nodes_num = nodes_num;
    this->m_worker_num = worker_num;
    this->Init();
}

CCommunicatorBackend::~CCommunicatorBackend()
{
}

void CCommunicatorBackend::Init()
{
    this->InitConfigration();
    this->InitRun();
    this->InitNodeStatusTool();
}

void CCommunicatorBackend::InitConfigration()
{
    // 获取程序运行的路径
    char buff[1000];
    getcwd(buff, 1000);
    std::string currentDirectory = buff;
    std::string configure_path = currentDirectory + "/libMemHub/configure/grpc_configure.cfg";
    this->m_configuration = new CConfiguration(configure_path);
    this->m_configuration->get_node_ip_port(&this->m_node_ip_port_info);
}

void CCommunicatorBackend::InitRun()
{
    this->m_thread_num = (this->m_nodes_num - 1) * this->m_worker_num;
    this->InitReplyServerRun();
    this->InitRequestServerRun();
}

/*
 * For each worker on other nodes, the current node will respectively start a thread
 *    to respond to the cross-node data requests from the corresponding worker.
 */
void CCommunicatorBackend::InitReplyServerRun()
{
    std::thread reply_server_run_thread_v[this->m_thread_num];
    this->m_reply_server_run_thread_p = reply_server_run_thread_v;
    int thread_id = 0;
    for (auto iter = this->m_node_ip_port_info.begin(); iter != this->m_node_ip_port_info.end(); ++iter)
    {
        if (iter->first != this->m_rank)
        {
            int node_port = atoi(iter->second["port"].c_str());
            for (int worker_id = 0; worker_id < this->m_worker_num; worker_id++)
            {
                int port_id = node_port + worker_id;
                this->m_reply_server_run_thread_p[thread_id] = std::thread(ReplyServerWorker, port_id, this->m_data_manager, this->m_sampler, this->m_rank, this->m_nodes_num, this->m_configuration, this);
                this->m_reply_server_run_thread_p[thread_id].detach();
                ++thread_id;
            }
        }
    }
}

/*
 * For each worker on the current node, n threads are started respectively
 *      to complete the task of requesting data from other nodes,
 *      where n = the total number of training nodes - 1, representing the total number of other nodes.
 */
void CCommunicatorBackend::InitRequestServerRun()
{
    this->InitRequestChannels();
    this->InitRequestQueues();

    std::thread request_server_run_thread_v[this->m_thread_num];
    this->m_request_server_run_thread_p = request_server_run_thread_v;
    for (int thread_id = 0; thread_id < this->m_thread_num; ++thread_id)
    {
        this->m_request_server_run_thread_p[thread_id] =
            std::thread(RequestServerWorker, thread_id, this->m_request_channels[thread_id], this->m_request_queues[thread_id], this->m_data_manager);
        this->m_request_server_run_thread_p[thread_id].detach();
    }
}

/**
 * GRPC channels for the request_server.

*/
void CCommunicatorBackend::InitRequestChannels()
{
    this->m_request_channels = std::vector<CRquestRpc *>((this->m_nodes_num - 1) * this->m_worker_num);
    int node_port = atoi(this->m_node_ip_port_info[this->m_rank]["port"].c_str());
    for (auto iter = this->m_node_ip_port_info.begin(); iter != this->m_node_ip_port_info.end(); ++iter)
    {
        int node_id = iter->first;
        if (node_id != this->m_rank)
        {
            for (int port_id = 0; port_id < this->m_worker_num; port_id++)
            {
                std::string target_addr = iter->second["ip"] + ":" + std::to_string(node_port + port_id);
                grpc::ChannelArguments channel_args;
                channel_args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, MAX_MESSAGE_LENGTH);
                CRquestRpc *request_channel = new CRquestRpc(
                    grpc::CreateCustomChannel(target_addr, grpc::InsecureChannelCredentials(), channel_args), this->m_rank, node_id, this->m_worker_num);
                int converted_node_id = this->convert_node_id(node_id);
                this->m_request_channels[converted_node_id * this->m_worker_num + port_id] = request_channel;
            }
        }
    }
}

/**
 * Each worker will use its corresponding request_queue to send cross-node data requests to the request_server.
 */
void CCommunicatorBackend::InitRequestQueues()
{
    for (int node_id = 0; node_id < this->m_nodes_num - 1; ++node_id)
    {
        for (int worker_id = 0; worker_id < this->m_worker_num; worker_id++)
        {
            CThreadQueue<RemoteRequestInfo, int> *request_queue = new CThreadQueue<RemoteRequestInfo, int>(node_id * this->m_worker_num + worker_id);
            m_request_queues.push_back(request_queue);
        }
    }
}

/**
 * For each new epoch, two node synchronizations will be executed.
 * Each synchronization ensures consistent status among all nodes.
 */
void CCommunicatorBackend::InitNodeStatusTool()
{
    if (this->m_rank == 0)
    {
        this->m_nodes_ready_num = std::vector<int>(2, 0);
        // this->m_synch_status_mtxs = std::vector<std::mutex *>(2);
        for (int i = 0; i < 2; ++i)
        {
            std::mutex *mtx = new std::mutex;
            this->m_synch_status_mtxs.push_back(mtx);
        }
        this->m_node_status_map = std::vector<std::vector<int>>(2, std::vector<int>(this->m_nodes_num, NODEUNREADY));
    }
}

/*
 * Convert node_id for convenient use as array indices for communication
 * If not converted, assuming the current rank=0, requests to other nodes would be 1 and 2, not starting from 0
 * After conversion, they become 0 and 1, facilitating array access
 */
int CCommunicatorBackend::convert_node_id(int node_id)
{
    return node_id < this->m_rank ? node_id : node_id - 1;
}

/* During node synchronization, the node with rank=0 is responsible for collecting synchronization information from all nodes.
 * When synchronization information is received from all nodes, it indicates that all nodes are in a consistent state at this moment.
 * The node with rank=0 then notifies all nodes that synchronization is complete.
 */

void CCommunicatorBackend::nodes_synch(int synch_id)
{
    int node_status;
    if (this->m_rank == 0)
    {
        this->set_node_status(this->m_rank, NODEREADY, synch_id);
        while (this->m_nodes_ready_num[synch_id] != this->m_nodes_num)
        {
            sleep(1);
        }
        this->m_nodes_ready_num[synch_id] = 0;
        for (int node_id = 0; node_id < this->m_nodes_num; ++node_id)
        {
            this->set_node_status(node_id, NODERUN, synch_id);
        }
        node_status = this->get_node_status(this->m_rank, synch_id);
    }
    else
    {
        node_status = this->SayNewEpoch(synch_id);
        while (node_status != NODERUN)
        {
            sleep(1);
            node_status = this->SynchNewEpoch(synch_id);
        }
    }
}

void CCommunicatorBackend::set_node_status(int node_id, int status, int synch_id)
{
    this->m_synch_status_mtxs[synch_id]->lock();
    if (status == NODEREADY && this->m_node_status_map[synch_id][node_id] == NODEUNREADY)
    {
        ++this->m_nodes_ready_num[synch_id];
    }
    this->m_node_status_map[synch_id][node_id] = status;
    this->m_synch_status_mtxs[synch_id]->unlock();
}

int CCommunicatorBackend::get_node_status(int node_id, int synch_id)
{
    this->m_synch_status_mtxs[synch_id]->lock();
    int status = this->m_node_status_map[synch_id][node_id];
    this->m_synch_status_mtxs[synch_id]->unlock();
    if (status == NODERUN)
    {
        // When status == NODERUN, it indicates that this synchronization is complete.
        // The status is then initialized to NODEUNREADY for the next synchronization.
        this->set_node_status(node_id, NODEUNREADY, synch_id);
    }
    return status;
}

int CCommunicatorBackend::SayNewEpoch(int synch_id)
{
    return this->m_request_channels[0]->SayNewEpoch(synch_id);
}

int CCommunicatorBackend::SynchNewEpoch(int synch_id)
{
    return this->m_request_channels[0]->SynchNewEpoch(synch_id);
}

/**
 * Send cross-node data request to request_server for cross-node data sharing.
 */
void CCommunicatorBackend::request_from_remote(int node_id, RequestDataInfo request_data_info, FileInfo *file_info)
{
    int converted_node_id = this->convert_node_id(node_id);
    RemoteRequestInfo remote_request_info(request_data_info.worker_id, request_data_info.request_idx, file_info);
    this->m_request_queues[converted_node_id * this->m_worker_num + request_data_info.worker_id]->SendRequest(remote_request_info);
}
/**
 * Thread worker of reply_server.
 */
void ReplyServerWorker(uint16_t port, CIDataManager *data_manager, CISampler *sampler, int node_id, int nodes_num, CConfiguration *configuration, CICommunicatorBackend *communicator_backend)
{
    std::string server_address = absl::StrFormat("0.0.0.0:%d", port);
    CReplyService service(data_manager, sampler, node_id, nodes_num, configuration, communicator_backend);
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    builder.SetMaxSendMessageSize(MAX_MESSAGE_LENGTH);
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    server->Wait();
}

/**
 * Thread worker of request_server.
 */
void RequestServerWorker(int thread_id, CRquestRpc *channel, CThreadQueue<RemoteRequestInfo, int> *request_queue, CIDataManager *data_manager)
{
    std::vector<Semaphore *> semaphores = data_manager->get_request_server_semaphores();
    std::vector<RemoteRequestInfo> remote_request_infos;
    while (true)
    {
        remote_request_infos.push_back(request_queue->RecvRequest());
        while (!request_queue->EmptyRequest())
        {
            remote_request_infos.push_back(request_queue->RecvRequest());
        }
        if (static_cast<int>(remote_request_infos.size()) > 0)
        {
            channel->RequestMultiFiles(remote_request_infos, semaphores, data_manager);
            remote_request_infos.clear();
        }
    }
}