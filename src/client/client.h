#ifndef GFS_CLIENT_H
#define GFS_CLIENT_H

#include "../headers/main.h"
#include "utils.h"
#include <condition_variable>
#include <thread>

#include "../master/generated/GFSMasterService.grpc.pb.h"
#include "../master/generated/GFSMasterService.pb.h"

#include <algorithm>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

struct counter_cv_pair {
  int counter;
  std::condition_variable cv;
	counter_cv_pair (int counter_): counter {counter_}{};
};

class MicroGFS {
public:
  MicroGFS(client_master_connection_descriptor_t connection_options)
      : connection_options_{connection_options},
        master_stub_{GFSMaster::ChunkServerService::NewStub(
            grpc::CreateChannel(grpc_connection_string<rpc_server_descriptor_t>(
                                    connection_options_.master),
                                grpc::InsecureChannelCredentials()))} {
    MESSAGE("[LOG]: connection options initialized.");
    MESSAGE("[LOG]: master stub created.");
    grpc_server_worker =
        std::thread(client_acknowledgement_grpc_server_worker, this);
  }

  void write(std::string file_path, unsigned int offset,
             unsigned int buffer_size, const char *buffer);
  void read(std::string file_name, unsigned int offset,
            unsigned int number_of_bytes, char *buffer);

	~MicroGFS(){
		grpc_server_worker.join();
	}

  static void client_acknowledgement_grpc_server_worker(MicroGFS *);

  client_master_connection_descriptor_t connection_options_;
  std::unique_ptr<GFSMaster::ChunkServerService::Stub> master_stub_;
  std::thread grpc_server_worker;
  std::thread tcp_client_worker;
  std::unordered_map<int /*write id*/, counter_cv_pair>
      write_acknowlegement_map_;
};

#endif
