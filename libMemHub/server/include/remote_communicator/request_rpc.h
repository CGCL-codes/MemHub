
#ifndef REQUEST_RPC_H
#define REQUEST_RPC_H

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include <grpcpp/grpcpp.h>
#include "DBMF.grpc.pb.h"

#include "../utils/util.h"
#include "../manager/i_data_manager.h"
#include "../../../local_communication/include/msg_info.h"
#include "../../../local_communication/include/semaphore.h"

// ABSL_FLAG(std::string, target, "11.11.11.15:50051", "Server address");

using DBMF::DBMFRPC;
using DBMF::FileReply;
using DBMF::FileRequest;
using DBMF::MultiFilesRequest;
using DBMF::MutiFilesReply;
using DBMF::NewEpochReply;
using DBMF::NewEpochRequest;
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::Status;

class CRquestRpc
{
public:
  CRquestRpc(std::shared_ptr<Channel> channel, int node_id, int target_node_id, int worker_num);

  void RequestMultiFiles(std::vector<RemoteRequestInfo> &remote_request_infos, std::vector<Semaphore *> &semaphores, CIDataManager *data_manager);
  int SayNewEpoch(int synch_id);
  int SynchNewEpoch(int synch_id);

private:
  std::vector<std::unique_ptr<DBMFRPC::Stub>> m_v_stubs;
  int m_node_id;
  int m_target_node_id;
  int m_worker_num;
};

#endif
