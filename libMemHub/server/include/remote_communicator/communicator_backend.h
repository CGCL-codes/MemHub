
// Remote communication backend of MemHub

#ifndef COMMUNICATOR_BACKEND_H
#define COMMUNICATOR_BACKEND_H

#include "reply_rpc.h"
#include "request_rpc.h"
#include "i_communicator_backend.h"

#include "../utils/util.h"
#include "../../configure/include/configuration.h"
#include "../../../local_communication/src/thread_queue.cpp"
#include "../../../local_communication/include/msg_info.h"
class CCommunicatorBackend : public CICommunicatorBackend
{
public:
    CCommunicatorBackend(CIDataManager *data_manager, CISampler *sampler, int node_id, int nodes_num, int worker_num);
    ~CCommunicatorBackend();

    void request_from_remote(int node_id, RequestDataInfo request_data_info, FileInfo *file_info);
    void nodes_synch(int synch_id);
    void set_node_status(int node_id, int status, int synch_id);
    int get_node_status(int node_id, int synch_id);

private:
    CIDataManager *m_data_manager;
    CISampler *m_sampler;
    CConfiguration *m_configuration;
    int m_rank;
    int m_nodes_num;
    int m_worker_num;
    int m_thread_num;

    std::thread *m_reply_server_run_thread_p;
    std::thread *m_request_server_run_thread_p;

    std::vector<CRquestRpc *> m_request_channels;                                              // GRPC channels for the request_server.
    std::vector<CThreadQueue<RemoteRequestInfo, int> *> m_request_queues;                      // Queues fo each worker to send cross-node data requests to the request_server.
    std::unordered_map<int, std::unordered_map<std::string, std::string>> m_node_ip_port_info; // <node_id, <ip,port>>

    // used when node synch;
    std::vector<int> m_nodes_ready_num;              // <synch_id, number of ready nodes>
    std::vector<std::vector<int>> m_node_status_map; // <node_id, <node_status>>, node_status: {NODEUNREADY, NODEREADY, NODERUN}
    std::vector<std::mutex *> m_synch_status_mtxs;   // <synch_id, mutex>

    void Init();
    void InitConfigration();

    void InitRun();
    void InitReplyServerRun();
    void InitRequestServerRun();
    void InitRequestChannels();
    void InitRequestQueues();

    void InitNodeStatusTool();

    int convert_node_id(int node_id);

    int SayNewEpoch(int synch_id);
    int SynchNewEpoch(int synch_id);
};

void ReplyServerWorker(uint16_t port, CIDataManager *data_manager, CISampler *sampler, int node_id, int nodes_num, CConfiguration *configuration, CICommunicatorBackend *communicator_backend);
void RequestServerWorker(int thread_id, CRquestRpc *channel, CThreadQueue<RemoteRequestInfo, int> *request_queue, CIDataManager *data_manager);
#endif