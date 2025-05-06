#include "../headers/main.h"
#include "../lib/tcp/tcp_client.h"
#include "utils.h"
#include <algorithm>
#include <condition_variable>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <iostream>
#include <mutex>
#include <string>
#include <utility>

#include "../server/generated/GFSChunkServer.grpc.pb.h"
#include "../server/generated/GFSChunkServer.pb.h"
#include "client.h"
#include "generated/GFSClientService.grpc.pb.h"
#include "generated/GFSClientService.pb.h"
#include <tuple>

#include "../lib/logger/logger.h"

class GFSClientServiceImplementation final
    : public GFSClient::GFSClientService::Service {

public:
  explicit GFSClientServiceImplementation(MicroGFS *client)
      : client_{client} {};
  grpc::Status AcknowledgeDataReceipt(
      grpc::ServerContext *context,
      const GFSClient::AcknowledgeDataReceiptRequest *request,
      GFSClient::AcknowledgeDataReceiptResponse *response) override {

    int write_id = request->write_id();
    if (auto find_result = client_->write_acknowlegement_map_.find(write_id);
        find_result == client_->write_acknowlegement_map_.end()) {
      MAINLOG_ERROR("Write id not found in the map!\n\t\twrite_id: {}",
                    write_id);
      response->set_acknowledgement_accepted(false);
    } else {
      MAINLOG_ERROR("Write id found in the map!\n\t\twrite_id: {}", write_id);
      MAINLOG_INFO("ACK COUNTER: {}", find_result->second.counter);
      --find_result->second.counter;
      MAINLOG_INFO("ACK COUNTER: {}", find_result->second.counter);
      find_result->second.cv.notify_one();
      response->set_acknowledgement_accepted(true);
    }
    return grpc::Status::OK;
  }

private:
  MicroGFS *client_;
};

void MicroGFS::client_acknowledgement_grpc_server_worker(MicroGFS *client) {
  grpc::ServerBuilder builder;
  builder.AddListeningPort(
      grpc_connection_string(client->connection_options_.client),
      grpc::InsecureServerCredentials());

  GFSClientServiceImplementation service{client};

  builder.RegisterService(&service);
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());

  MAINLOG_WARN("RPC SERVER WAINTING FOR ACK");
  server->Wait();
};

void client_data_transmission_worker(int socket, int write_id,
                                     size_t write_size, const char *buffer) {

  MAINLOG_INFO("write size argument {}", write_size);
  constexpr size_t intrem_buffer_size = 8192;
  std::array<char, intrem_buffer_size> intrem_buffer;

  size_t intrem_buffer_data_size = std::min(intrem_buffer_size, write_size);

  MAINLOG_WARN("intrem_buffer_data_size: {}", intrem_buffer_data_size);

  memcpy(intrem_buffer.data(), &write_id, sizeof write_id);

  MAINLOG_WARN("intrem buffer: {}",
               *reinterpret_cast<int *>(intrem_buffer.data()));

  memcpy(intrem_buffer.data() + (sizeof write_id), buffer,
         intrem_buffer_data_size);

  // MAINLOG_WARN("string: {}", intrem_buffer.data() + (sizeof write_id));

  size_t copy_position = 0;
  size_t total_bytes_sent = 0;
  long int bytes_sent = 0;

  while (bytes_sent < intrem_buffer_data_size) {
    MAINLOG_ERROR("Intered the loop");
    int send_return =
        send(socket, &intrem_buffer + bytes_sent, intrem_buffer_data_size, 0);
    MAINLOG_ERROR("send return: {}", send_return);
    if (send_return < 0) {
      MAINLOG_ERROR("First send return bellow 0");
      return;
    }
    bytes_sent += send_return;
    MAINLOG_ERROR("bytes sent: {}", bytes_sent);
  }

  total_bytes_sent += bytes_sent;
  MAINLOG_INFO("TOTAL BYTES SENT SO FAR : {}, WRITE SIZE : {}",
               total_bytes_sent, write_size);
  bytes_sent = 0;
  while (total_bytes_sent < write_size) {
    MAINLOG_ERROR("SHOULD NOT EXECUTE");
    size_t remaining_bytes = write_size - total_bytes_sent;
    MAINLOG_INFO("RAMAINING BYTES: {}", remaining_bytes);
    intrem_buffer_data_size = std::min(remaining_bytes, intrem_buffer_size);
    memcpy(&intrem_buffer, buffer + total_bytes_sent, intrem_buffer_size);

    while (bytes_sent < intrem_buffer_data_size) {
      int send_return =
          send(socket, &intrem_buffer + bytes_sent, intrem_buffer_data_size, 0);
      if (send_return < 0) {
        MAINLOG_ERROR("Seoncd send return below 0");
        return;
      }
      bytes_sent += send_return;
    }

    total_bytes_sent += bytes_sent;
    bytes_sent = 0;
  }
  MAINLOG_INFO("Write operation SUCCESS✔  ");
}

void MicroGFS::write(std::string file_path, unsigned int offset,
                     unsigned int size, const char *buffer) {
  size += 4;
  MAINLOG_INFO("sizeof(int): {}", sizeof(int));
  std::mutex mu;
  grpc::ClientContext write_context;
  GFSMaster::WriteRequest write_request;
  GFSMaster::WriteResponse write_response;

  /*Adding write offset, size and file path to the request*/
  write_request.set_file_path(file_path);
  write_request.set_offset(offset);
  write_request.set_data_size(size);
  MAINLOG_INFO("write request data size: {}", write_request.data_size());

  /*Adding client RPC credentials to the request*/
  write_request.mutable_client_server_info()->set_ip(
      connection_options_.client.ip);
  write_request.mutable_client_server_info()->set_rpc_port(
      connection_options_.client.rpc_port);
  write_request.mutable_client_server_info()->set_tcp_port(
      connection_options_.client.tcp_port);

  MAINLOG_INFO("(1) Write rpc function invoked!, file name: {}, offset: {}, "
               "write size {}",
               file_path, offset, size);

  /*BLOCKING: Invoking RPC Call*/
  master_stub_->Write(&write_context, write_request, &write_response);

  /*Setting up TCP transmssion channles*/
  tcp_rpc_server_descriptor_t transmission_chain_head_server;
  transmission_chain_head_server.ip =
      write_response.response_body().primary_server().ip();
  transmission_chain_head_server.tcp_port =
      write_response.response_body().primary_server().tcp_port();
  transmission_chain_head_server.rpc_port =
      write_response.response_body().primary_server().rpc_port();

  MAINLOG_INFO("(28) Transmission chain head server:\n IP: {}, TCP PORT: {}, "
               "RPC PORT: {}\n",
               transmission_chain_head_server.ip,
               transmission_chain_head_server.tcp_port,
               transmission_chain_head_server.rpc_port);

  TCPClient tcp_client(transmission_chain_head_server);
  tcp_client.connectToServer();

  int write_id = write_response.response_body().write_id();
  int ack_count = write_response.response_body().secondary_servers().size() + 1;
  MAINLOG_INFO("################ =========> WRITE ID: {}", write_id);

  /*Spawing thread to handle transmission so we can start listening for
   * acknowledgement as soon as possible*/
  tcp_client_worker = std::thread(client_data_transmission_worker,
                                  tcp_client.socket_, write_id, size, buffer);

  /*TODO: Block this with conditional variable (not all of it)*/

  auto [it, inserted] = write_acknowlegement_map_.emplace(
      std::piecewise_construct, std::forward_as_tuple(write_id),
      std::forward_as_tuple(ack_count));

  counter_cv_pair &ack_count_cv_pair = it->second;

  std::unique_lock<std::mutex> lock(mu);

  MAINLOG_INFO("BEFORE CV WAIT");

  ack_count_cv_pair.cv.wait(
      lock, [&ack_count_cv_pair] { return ack_count_cv_pair.counter == 0; });

  MAINLOG_WARN("sleeping for 2 seconds ....");
  sleep(2);
  MAINLOG_INFO("PASSED CV WAIT");

  tcp_rpc_server_descriptor_t primary_server_info;
  primary_server_info.ip = write_response.response_body().primary_server().ip();
  primary_server_info.tcp_port =
      write_response.response_body().primary_server().tcp_port();
  primary_server_info.rpc_port =
      write_response.response_body().primary_server().rpc_port();

  std::unique_ptr<GFSChunkServer::ChunkServerService::Stub> stub_ =
      GFSChunkServer::ChunkServerService::NewStub(
          grpc::CreateChannel(grpc_connection_string(primary_server_info),
                              grpc::InsecureChannelCredentials()));

  grpc::ClientContext flush_context;
  GFSChunkServer::FlushRequest flush_request;
  GFSChunkServer::FlushResponse flush_response;

  MAINLOG_INFO("WRITE ID ACKNOWLEDGED TO THE CLIENT: {}", write_id);

  flush_request.set_write_id(write_id);

  grpc::Status status =
      stub_->Flush(&flush_context, flush_request, &flush_response);

  MAINLOG_INFO("After flush rpc call!");
  if (status.ok()) {
    if (flush_response.body().acknowledgement()) {
      MAINLOG_WARN("DATA WRITTEEEEEEEEEEEENNNNNNNNNNN "
                   "WOOOOHOOOOOOOOOOOOOOOOOOOOOO!!!!!!!!!!!!!😍😍😍😍😍");
    }
  } else {
    MAINLOG_ERROR("DATA NOT WRITTEN 😢😢😢😢😢");
  }
  tcp_client_worker.join();
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

  constexpr int data_size = 4096;

  char *data_buffer = new char[data_size];

  std::filesystem::path hamlet_txt_path("hamlet.txt");
  std::ifstream hamlet_input_file;
  hamlet_input_file.open(hamlet_txt_path, std::ios::in);

  if (hamlet_input_file.is_open()) {
    hamlet_input_file.read(data_buffer, data_size);
    client.write("hamlet.txt", 0, data_size, data_buffer);
    hamlet_input_file.read(data_buffer, data_size);
    client.write("hamlet.txt", 0, data_size, data_buffer);
    std::string str;
    std::cin >> str;
  } else {
    MAINLOG_ERROR("Couldn't open the file: {}", hamlet_txt_path.string());
  }
  /*
  std::string str;
  str.resize(4096);
  memcpy(str.data(), data_buffer, 4096);
  MAINLOG_INFO("The string read is: {} ", str);
  */
  // hamlet_input_file.close();
}
