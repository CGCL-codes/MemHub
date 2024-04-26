#ifndef REPLY_RPC_H
#define REPLY_RPC_H

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include "DBMF.grpc.pb.h"

#include "i_communicator_backend.h"
#include "../utils/util.h"
#include "../manager/i_data_manager.h"
#include "../random_sampler/i_sampler.h"
#include "../../configure/include/configuration.h"

using DBMF::DBMFRPC;
using DBMF::FileReply;
using DBMF::FileRequest;
using DBMF::MultiFilesRequest;
using DBMF::MutiFilesReply;
using DBMF::NewEpochReply;
using DBMF::NewEpochRequest;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerWriter;
using grpc::Status;

// ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");

class CReplyService final : public DBMFRPC::Service
{
public:
    CReplyService(CIDataManager *data_manager, CISampler *sampler, int node_id, int nodes_num, CConfiguration *configuration, CICommunicatorBackend *communicator_backend);

    Status RequestMultiFiles(ServerContext *context, const MultiFilesRequest *multi_files_request,
                             ServerWriter<MutiFilesReply> *writer) override;

    Status SayNewEpoch(ServerContext *context, const NewEpochRequest *request,
                       NewEpochReply *reply) override;
    Status SynchNewEpoch(ServerContext *context, const NewEpochRequest *request,
                         NewEpochReply *reply) override;

private:
    CIDataManager *m_data_manager;
    CISampler *m_sampler;
    CConfiguration *m_configuration;
    CICommunicatorBackend *m_communicator_backend;

    int m_rank;
    int m_nodes_num;
    int m_max_check_num;
    int m_max_reply_num;
    std::unordered_map<int, std::unordered_map<std::string, int>> m_node_ip_port_info;
};

#endif
