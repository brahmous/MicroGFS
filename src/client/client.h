#ifndef GFS_CLIENT_H
#define GFS_CLIENT_H

#include "../headers/main.h"
#include "utils.h"

#include "../master/generated/GFSMasterService.pb.h"
#include "../master/generated/GFSMasterService.grpc.pb.h"

#include <algorithm>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>


class MicroGFS {
public:
	MicroGFS (client_master_connection_descriptor_t connection_options)
	: connection_options_{connection_options},
	master_stub_{GFSMaster::ChunkServerService::NewStub(grpc::CreateChannel(grpc_connection_string<rpc_server_descriptor_t>(connection_options_.master), grpc::InsecureChannelCredentials()))}
	{
		MESSAGE("[LOG]: connection options initialized.");
		MESSAGE("[LOG]: master stub created.");
	}

	void write(std::string file_path, unsigned int offset, unsigned int buffer_size, const char * buffer);
	void read(std::string file_name, unsigned int offset, unsigned int number_of_bytes, char * buffer);

private:
	client_master_connection_descriptor_t connection_options_;
	std::unique_ptr<GFSMaster::ChunkServerService::Stub> master_stub_;
};

#endif
