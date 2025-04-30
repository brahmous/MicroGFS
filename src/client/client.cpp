#include "../headers/main.h"
#include "../lib/tcp/tcp_client.h"
#include "utils.h"
#include <algorithm>
#include <cstring>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <iostream>

#include "../server/generated/GFSChunkServer.grpc.pb.h"
#include "../server/generated/GFSChunkServer.pb.h"
#include "client.h"
#include "generated/GFSClientService.grpc.pb.h"
#include "generated/GFSClientService.pb.h"

#include "../lib/logger/logger.h"

class GFSClientServiceImplementation final
    : public GFSClient::GFSClientService::Service {

public:
  explicit GFSClientServiceImplementation() {};

  grpc::Status AcknowledgeDataReceipt(
      grpc::ServerContext *context,
      const GFSClient::AcknowledgeDataReceiptRequest *request,
      GFSClient::AcknowledgeDataReceiptResponse *response) override {

    if (acknowledgement_count > 0) {
      acknowledgement_count--;
    }

    if (acknowledgement_count == 0) {

      tcp_rpc_server_descriptor_t primary_server_info;
      primary_server_info.ip = "0.0.0.0";
      primary_server_info.tcp_port = 6000;
      primary_server_info.rpc_port = 6500;

      std::unique_ptr<GFSChunkServer::ChunkServerService::Stub> stub_ =
          GFSChunkServer::ChunkServerService::NewStub(
              grpc::CreateChannel(grpc_connection_string(primary_server_info),
                                  grpc::InsecureChannelCredentials()));

      grpc::ClientContext context;
      GFSChunkServer::FlushRequest request;
      GFSChunkServer::FlushResponse response;

      request.set_write_id(20);

      stub_->Flush(&context, request, &response);

      if (response.body().acknowledgement()) {
        MESSAGE("DATA WRITTEEEEEEEEEEEENNNNNNNNNNN "
                "WOOOOHOOOOOOOOOOOOOOOOOOOOOO!!!!!!!!!!!!!");
      }
    }

    response->set_acknowledgement_accepted(true);
    return grpc::Status::OK;
  }

private:
  int acknowledgement_count = 4;
};
void MicroGFS::write(std::string file_path, unsigned int offset,
                     unsigned int size, const char *buffer) {

  grpc::ClientContext context;
  GFSMaster::WriteRequest request;
  GFSMaster::WriteResponse response;

  request.set_file_path(file_path);
  request.set_data_size(offset);
  request.set_offset(size);

  request.mutable_client_server_info()->set_ip(connection_options_.client.ip);
  request.mutable_client_server_info()->set_rpc_port(
      connection_options_.client.rpc_port);
  request.mutable_client_server_info()->set_tcp_port(
      connection_options_.client.tcp_port);

  MAINLOG_INFO("(1) Write rpc function invoked!, file name: {}, offset: {}, "
               "write size {}",
               file_path, offset, size);

  master_stub_->Write(&context, request, &response);

  tcp_rpc_server_descriptor_t transmission_chain_head_server;
  transmission_chain_head_server.ip =
      response.response_body().primary_server().ip();
  transmission_chain_head_server.tcp_port =
      response.response_body().primary_server().tcp_port();
  transmission_chain_head_server.rpc_port =
      response.response_body().primary_server().rpc_port();

  MAINLOG_INFO("(28) Transmission chain head server:\n IP: {}, TCP PORT: {}, "
               "RPC PORT: {}\n",
               transmission_chain_head_server.ip,
               transmission_chain_head_server.tcp_port,
               transmission_chain_head_server.rpc_port);

  TCPClient tcp_client(transmission_chain_head_server);
  tcp_client.connectToServer();

	int write_id = response.response_body().write_id();

	char * write_id_buffer = new char[sizeof(int)];
	memcpy(write_id_buffer, &write_id, sizeof(int));

	MAINLOG_INFO("WRITE ID: {}", write_id);

	sleep(2);
	tcp_client.write(write_id_buffer, sizeof(int));

  grpc::ServerBuilder builder;
  builder.AddListeningPort(grpc_connection_string(connection_options_.client),
                           grpc::InsecureServerCredentials());

  GFSClientServiceImplementation service;
  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

  server->Wait();

  /*Initiate tcp connection and data transfer*/
  /*need to get acknowledgement*/
  /*start rpc server*/

  /*teadown server*/
  /*
   *
   * The behavior i want is execute the script and teardown don't continue to
   * block.
   *
   * */
}

int main(int argc, char *argv[]) {

  GFSLogger::Logger main_logger;
  main_logger.init();

  client_master_connection_descriptor_t connection_options;
  connection_options.master.ip = "0.0.0.0";
  connection_options.master.rpc_port = 5000;
  connection_options.client.ip = "0.0.0.0";
  connection_options.client.rpc_port = 5100;
  connection_options.client.tcp_port = 3100;
  MicroGFS client(connection_options);

  client.write("testfile", 20, 200, nullptr);
}
