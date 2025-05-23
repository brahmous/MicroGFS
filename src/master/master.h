#ifndef MASTER_H
#define MASTER_H

#include "../headers/main.h"
#include "generated/GFSMasterService.pb.h"
#include <future>
#include <ostream>
#include <tuple>
#include <vector>

namespace GFSNameSpace {
enum class GFSFileType { NORMAL = 1, ATOMIC_APPEND = 2 };

struct chunk_server_handle_pair {
  uint64_t handle;
  tcp_rpc_server_descriptor_t server_info;
};

class write_coordinate {
public:
  void
  setClient(const GFSMaster::TcpRpcServerInfo *request_client_server_info) {
    client_server_info.ip = request_client_server_info->ip();
    client_server_info.rpc_port = request_client_server_info->rpc_port();
    client_server_info.tcp_port = request_client_server_info->tcp_port();
  };

  tcp_rpc_server_descriptor_t client_server_info;
  std::size_t write_offset;
  int write_id;
	int write_size;
  std::vector<chunk_server_handle_pair> chunk_server_handle_list;
};

struct chunk_replica_descriptor {
  uint64_t handle;
  int host_id;
};

struct lease_descriptor {
  int host_id = -1;
  int write_id = -1;
  double issued_at = 0.0;
};

struct chunk_descriptor {
  std::size_t size;
  std::vector<chunk_replica_descriptor> replicas;
  lease_descriptor lease;
};

struct GFSFile {
  GFSFileType mode;
  std::size_t size;
  std::size_t chunk_size;
  std::vector<chunk_descriptor> chunks;
};
std::ostream &operator<<(std::ostream &out, const GFSFile &file);
} // namespace GFSNameSpace
#endif
