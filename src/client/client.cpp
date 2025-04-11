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

class GFSClientServiceImplementation final : public GFSClient::GFSClientService::Service {

public:
	explicit GFSClientServiceImplementation () {};

	grpc::Status AcknowledgeDataReceipt(
		grpc::ServerContext* context, 
		const GFSClient::AcknowledgeDataReceiptRequest* request,
		GFSClient::AcknowledgeDataReceiptResponse* response)
	override 
	{
		MESSAGE("[LOG]: Acknowledgment received.");

		if (acknowledgement_count > 0){
			acknowledgement_count--;
			MESSAGE("[LOG]: Acknowledgement count: " << acknowledgement_count << ".");
		}

		if (acknowledgement_count == 0) {

			MESSAGE("[LOG]: Acknowledgement count is " << acknowledgement_count << ".");

			tcp_rpc_server_descriptor_t primary_server_info;
			primary_server_info.ip = "0.0.0.0";
			primary_server_info.tcp_port = 6000;
			primary_server_info.rpc_port = 6500;

			MESSAGE("[LOG]: PRIMARY SERVER INFO: ");
			MESSAGE("[LOG]: Primary server ip: " << primary_server_info.ip);
			MESSAGE("[LOG]: Primary server tcp port: " << primary_server_info.tcp_port);
			MESSAGE("[LOG]: Primary rpc port: " << primary_server_info.rpc_port);
			MESSAGE("[LOG]: PRIMARY SERVER INFO END");

			std::unique_ptr<GFSChunkServer::ChunkServerService::Stub> stub_
				= GFSChunkServer::ChunkServerService::NewStub(
						grpc::CreateChannel(grpc_connection_string(primary_server_info),
						grpc::InsecureChannelCredentials()
					)
				);

			MESSAGE("[LOG]: PRIMARY SERVER STUB CONSTRUCTED");
			grpc::ClientContext context;
			GFSChunkServer::FlushRequest request;
			GFSChunkServer::FlushResponse response; 

			request.set_write_id("hello");

			MESSAGE("[LOG]: Flush call initiated.");
			stub_->Flush(&context, request, &response);
			MESSAGE("[LOG]: Flush call response." << response.flushed());

			if (response.flushed()) {
				MESSAGE("DATA WRITTEEEEEEEEEEEENNNNNNNNNNN WOOOOHOOOOOOOOOOOOOOOOOOOOOO!!!!!!!!!!!!!");
			}

		} 

		response->set_acknowledgement_accepted(true);
		return grpc::Status::OK;
	}

private:
	int acknowledgement_count = 4;
};

void MicroGFS::write(std::string file_path, unsigned int offset, unsigned int size) {
	MESSAGE("[LOG]: file_path: " << file_path);
	MESSAGE("[LOG]: offset: " << offset);
	MESSAGE("[LOG]: data size: " << size);

	grpc::ClientContext context;
	GFSMaster::WriteRequest request;
	GFSMaster::WriteResponse response;

	request.set_file_path(file_path);
	request.set_data_size(offset);
	request.set_offset(size);


	MESSAGE("[LOG]: Write called on the stub.");
	master_stub_->Write(&context,request, &response);
	MESSAGE("[LOG]: Write results:");
	MESSAGE("[LOG] PRIMARY INFO: ");
	MESSAGE("[->]ip: " << response.primary_server().ip());
	MESSAGE("[->]port: " << response.primary_server().rpc_port());

	for (int i=0; i<response.secondary_servers_size(); ++i) {
		MESSAGE("[LOG]: SECONDARY " << i << " INFO");
		MESSAGE("[->]ip: " << response.secondary_servers(i).ip());
		MESSAGE("[->]port: " << response.secondary_servers(i).rpc_port());
	}

	tcp_rpc_server_descriptor_t chunk_server;
	chunk_server.ip = "0.0.0.0";
	chunk_server.tcp_port =  6000;
	chunk_server.rpc_port =  6500;

	MESSAGE("[LOG]: CHUNK SERVER TCP CHAIN START INFO START");
	MESSAGE("[LOG]: Chunk server ip: " << chunk_server.ip);
	MESSAGE("[LOG]: Chunk server tcp port: " << chunk_server.tcp_port);
	MESSAGE("[LOG]: Chunk server rpc port: " << chunk_server.rpc_port);
	MESSAGE("[LOG]: CHUNK SERVER TCP CHAIN START INFO END");

	MESSAGE("[LOG]: TCP connection started.");
	TCPClient tcp_client (chunk_server);
	tcp_client.connectToServer();
	MESSAGE("[LOG]: RPC endpoint to wait for acknowledgemnt started.");
	grpc::ServerBuilder builder;
	builder.AddListeningPort(grpc_connection_string(connection_options_.client), grpc::InsecureServerCredentials());
	GFSClientServiceImplementation service;
	builder.RegisterService(&service);
	std::unique_ptr<grpc::Server> server (builder.BuildAndStart());

	MESSAGE("[LOG]: Server is waiting for transmission over tcp to cascade and acknowledgements to be received.");
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

	MESSAGE("[LOG]: Client started.");
	client_master_connection_descriptor_t connection_options;
	MESSAGE("[LOG BEGIN]: Client-Master connection info.");
	connection_options.master.ip = "0.0.0.0";
	connection_options.master.rpc_port = 5000;
	connection_options.client.ip = "0.0.0.0";
	connection_options.client.rpc_port = 4500;
	connection_options.client.tcp_port = 4000;
	MESSAGE("[->]: master ip: " << connection_options.master.ip);
	MESSAGE("[->]: master rpc_port: " << connection_options.master.rpc_port);
	MESSAGE("[->]: client ip: " << connection_options.client.ip);
	MESSAGE("[->]: client rpc_port: " << connection_options.client.rpc_port);
	MESSAGE("[->]: client tcp_port: " << connection_options.client.tcp_port);
	MESSAGE("[LOG END]: Client-Master connection info.");

	MESSAGE("[LOG BEGIN]: Client Construction start.");
	MicroGFS client (connection_options);
	MESSAGE("[LOG END]: Client Construction end.");

	MESSAGE("[LOG BEGIN]: write operation start.");
	client.write("/hello", 20, 200);
	MESSAGE("[LOG END]: Write operation end.");
}
