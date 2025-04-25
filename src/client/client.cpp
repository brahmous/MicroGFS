#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <iostream>
#include "../headers/main.h"
#include "../lib/tcp/tcp_client.h"
#include "utils.h"

#include "client.h"
#include "../server/generated/GFSChunkServer.grpc.pb.h"
#include "../server/generated/GFSChunkServer.pb.h"
#include "generated/GFSClientService.grpc.pb.h"
#include "generated/GFSClientService.pb.h"

#include "../lib/logger/logger.h"

class GFSClientServiceImplementation final : public GFSClient::GFSClientService::Service {

public:
	explicit GFSClientServiceImplementation () {};

	grpc::Status AcknowledgeDataReceipt(
		grpc::ServerContext* context, 
		const GFSClient::AcknowledgeDataReceiptRequest* request,
		GFSClient::AcknowledgeDataReceiptResponse* response)
	override 
	{

		if (acknowledgement_count > 0){
			acknowledgement_count--;
		}

		if (acknowledgement_count == 0) {


			tcp_rpc_server_descriptor_t primary_server_info;
			primary_server_info.ip = "0.0.0.0";
			primary_server_info.tcp_port = 6000;
			primary_server_info.rpc_port = 6500;

			std::unique_ptr<GFSChunkServer::ChunkServerService::Stub> stub_
				= GFSChunkServer::ChunkServerService::NewStub(
						grpc::CreateChannel(grpc_connection_string(primary_server_info),
						grpc::InsecureChannelCredentials()
					)
				);

			grpc::ClientContext context;
			GFSChunkServer::FlushRequest request;
			GFSChunkServer::FlushResponse response; 

			request.set_write_id(20);

			stub_->Flush(&context, request, &response);

			if (response.body().acknowledgement()) {
				MESSAGE("DATA WRITTEEEEEEEEEEEENNNNNNNNNNN WOOOOHOOOOOOOOOOOOOOOOOOOOOO!!!!!!!!!!!!!");
			}

		} 

		response->set_acknowledgement_accepted(true);
		return grpc::Status::OK;
	}

private:
	int acknowledgement_count = 4;
};

void MicroGFS::write(std::string file_path, unsigned int offset, unsigned int size, const char* buffer) {
	grpc::ClientContext context;
	GFSMaster::WriteRequest request;
	GFSMaster::WriteResponse response;

	MAINLOG_INFO("Write command issued!");

	request.set_file_path(file_path);
	request.set_data_size(offset);
	request.set_offset(size);

	master_stub_->Write(&context,request, &response);

	tcp_rpc_server_descriptor_t chunk_server;
	chunk_server.ip = response.response_body().primary_server().ip();
	chunk_server.tcp_port = response.response_body().primary_server().tcp_port();
	chunk_server.rpc_port =  response.response_body().primary_server().rpc_port();

	MAINLOG_INFO("primary credentials:\n IP: {}, TCP PORT: {}, RPC PORT: {}\n", chunk_server.ip, chunk_server.tcp_port, chunk_server.rpc_port);

	TCPClient tcp_client (chunk_server);
	tcp_client.connectToServer();

	grpc::ServerBuilder builder;
	builder.AddListeningPort(grpc_connection_string(connection_options_.client), grpc::InsecureServerCredentials());
	GFSClientServiceImplementation service;
	builder.RegisterService(&service);
	std::unique_ptr<grpc::Server> server (builder.BuildAndStart());

	server->Wait();

	/*Initiate tcp connection and data transfer*/
	/*need to get acknowledgement*/
	/*start rpc server*/

	/*teadown server*/
	/*
	 *
	 * The behavior i want is execute the script and teardown don't continue to block.
	 *
	 * */
}

int main(int argc, char *argv[])
{
	
	GFSLogger::Logger main_logger;
	main_logger.init();

	client_master_connection_descriptor_t connection_options;
	connection_options.master.ip = "0.0.0.0";
	connection_options.master.rpc_port = 5000;
	connection_options.client.ip = "0.0.0.0";
	connection_options.client.rpc_port = 5100;
	connection_options.client.tcp_port = 3100;
	MicroGFS client (connection_options);

	client.write("testfile", 20, 200, nullptr);
}
