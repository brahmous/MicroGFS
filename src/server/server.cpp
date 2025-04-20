#include <condition_variable>
#include <filesystem>
#include <google/protobuf/json/json.h>
#include <google/protobuf/message.h>
#include <grpcpp/channel.h>
#include <grpcpp/server_context.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/support/status.h>
#include <memory>
#include <mutex>
#include <netdb.h>
#include <queue>
#include <vector>

#include "../headers/main.h"
#include "./utils.h"

#include "../master/generated/GFSMasterService.grpc.pb.h"

#include "generated/GFSChunkServer.grpc.pb.h"
#include "generated/GFSChunkServer.pb.h"

#include "../client/generated/GFSClientService.grpc.pb.h"
#include "../client/generated/GFSClientService.pb.h"

#include "../lib/tcp/tcp_client.h"
#include "../lib/tcp/tcp_server.h"

class ChunkServer;
void tcp_server_worker(ChunkServer *chunk_server);

struct ChunkDescriptor {
  std::string name;
  std::size_t size;
  std::queue<std::string> write_order_queue;
};

struct assign_secondary_request_args {
  grpc::ClientContext context;
  GFSChunkServer::AssignSecondaryRequest request;
  GFSChunkServer::AssignSecondaryResponse response;
};

class SecondaryChunkServerController {
public:
  SecondaryChunkServerController(const tcp_rpc_server_descriptor_t &server_info)
      : server_info_{server_info},
        secondary_server_stub_{GFSChunkServer::ChunkServerService::NewStub(
            grpc::CreateChannel(grpc_connection_string(server_info),
                                grpc::InsecureChannelCredentials()))} {}

  void assignSecondary(std::atomic<int> &counter, std::condition_variable &cv,
                       std::mutex &mu, bool &acknowledged,
                       assign_secondary_request_args &args) {

    // grpc::ClientContext context;
    // GFSChunkServer::AssignSecondaryRequest req;
    // GFSChunkServer::AssignSecondaryResponse res;
    secondary_server_stub_->async()->AssignSecondary(
        &args.context, &args.request, &args.response,
        [&acknowledged, &mu, &cv, &counter, &args](grpc::Status status) {
          MESSAGE("[LOG]: Callback Hit");
          // MESSAGE("COUNTER: " << counter);
          bool result;
          if (!status.ok()) {
            MESSAGE("NOT OK CALL TO SECONDARY");
            result = false;
          } else {
            result = args.response.acknowledgment();
          }
          std::lock_guard<std::mutex> lock(mu);
          counter -= 1;
          acknowledged = acknowledged && result;
          cv.notify_one();
        });
  }

private:
  tcp_rpc_server_descriptor_t server_info_;
  std::unique_ptr<GFSChunkServer::ChunkServerService::Stub>
      secondary_server_stub_;
};

class ChunkServer {
public:
  class ChunkServerServiceImplementation final
      : public GFSChunkServer::ChunkServerService::CallbackService {
  public:
    ChunkServerServiceImplementation(ChunkServer *_chunkserver)
        : chunkserver{_chunkserver} {}

    grpc::ServerWriteReactor<GFSChunkServer::ChunkMetadata> *
    UploadChunkMetadata(
        grpc::CallbackServerContext *context,
        const GFSChunkServer::UploadChunkMetadataRequest *request) override {
      class Writer
          : public grpc::ServerWriteReactor<GFSChunkServer::ChunkMetadata> {
      public:
        Writer(
            std::shared_ptr<std::vector<ChunkDescriptor>> chunk_descriptor_list)
            : chunk_descriptor_list_{chunk_descriptor_list},
              it_{chunk_descriptor_list_->begin()} {
          NextWrite();
        }

        void OnWriteDone(bool ok) override {
          if (!ok) {
            Finish(
                grpc::Status(grpc::StatusCode::UNKNOWN, "Unexpected Failure"));
          }
          NextWrite();
        };

        void OnDone() override {
          MESSAGE("Uploading chunk metadata completed.");
          delete this;
        };

        void OnCancel() override {
          MESSAGE("Uploading chunk metadata cancelled.");
        }

      private:
        void NextWrite() {
          if (it_ == chunk_descriptor_list_->end()) {
            Finish(grpc::Status::OK);
            return;
          }
          descriptor.set_handle(std::atoi(it_->name.c_str()));
          descriptor.set_chunk_size(it_->size);
          ++it_;
          StartWrite(&descriptor);
          // Finish(grpc::Status::OK);
          return;
        }

        GFSChunkServer::ChunkMetadata descriptor;
        std::shared_ptr<std::vector<ChunkDescriptor>> chunk_descriptor_list_;
        std::vector<ChunkDescriptor>::const_iterator it_;
      };

      return new Writer(chunkserver->chunk_descriptor_list_);
    }

    grpc::ServerUnaryReactor *
    HeartBeat(grpc::CallbackServerContext *context,
              const GFSChunkServer::HeartBeatRequest *request,
              GFSChunkServer::HeartBeatResponse *response) override {
      response->set_extend_lease(true);
      auto *reactor = context->DefaultReactor();
      reactor->Finish(grpc::Status::OK);
      return reactor;
    }

    grpc::ServerUnaryReactor *
    AssignPrimary(grpc::CallbackServerContext *context,
                  const GFSChunkServer::AssignPrimaryRequest *request,
                  GFSChunkServer::AssignPrimaryResponse *response) override {
      // std::atomic<int> counter = request->secondary_servers().size();
      std::atomic<int> counter = 3;
      bool acknowledged = false;
      std::mutex mu;
      std::condition_variable cv;

      // set lease in local in memory chunk structure
      // forward the lease to the secondaries
      // MESSAGE("client ip: " << request->client_server().ip());
      // MESSAGE("client port: " << request->client_server().rpc_port());

      tcp_rpc_server_descriptor_t server1_info;
      server1_info.ip = "0.0.0.0";
      server1_info.rpc_port = 6501;
      server1_info.tcp_port = 6001;

      tcp_rpc_server_descriptor_t server2_info;
      server2_info.ip = "0.0.0.0";
      server2_info.rpc_port = 6502;
      server2_info.tcp_port = 6002;

      tcp_rpc_server_descriptor_t server3_info;
      server3_info.ip = "0.0.0.0";
      server3_info.rpc_port = 6503;
      server3_info.tcp_port = 6003;

      // std::vector<SecondaryChunkServerController> controllers;

      std::vector<SecondaryChunkServerController> controllers;

      controllers.emplace_back(server1_info);
      controllers.emplace_back(server2_info);
      controllers.emplace_back(server3_info);

      std::vector<assign_secondary_request_args> args(3);
      /*
      for (int i = 0; i < request->secondary_servers().size(); ++i) {
              MESSAGE("[LOG]: secondary ip: " <<
      request->secondary_servers(i).ip()); server_info.ip =
      request->secondary_servers(i).ip(); MESSAGE("[LOG]: secondary port: " <<
      request->secondary_servers(i).rpc_port()); server_info.rpc_port =
      request->secondary_servers(i).rpc_port();
              controllers.emplace_back(server_info);
      }
      */

      int args_i = 0;
      for (SecondaryChunkServerController &controller : controllers) {
        controller.assignSecondary(counter, cv, mu, acknowledged, args[args_i]);
        ++args_i;
      }

      std::unique_lock<std::mutex> lock(mu);
      cv.wait(lock, [&counter] { return counter == 0; });
      // responde with acknowledegement
      response->set_acknowledgment(acknowledged);
      auto *reactor = context->DefaultReactor();
      reactor->Finish(grpc::Status::OK);
      return reactor;
    }

    grpc::ServerUnaryReactor *AssignSecondary(
        grpc::CallbackServerContext *context,
        const GFSChunkServer::AssignSecondaryRequest *request,
        GFSChunkServer::AssignSecondaryResponse *response) override {
      response->set_acknowledgment(true);
      auto *reactor = context->DefaultReactor();
      reactor->Finish(grpc::Status::OK);
      return reactor;
    }

    grpc::ServerUnaryReactor *
    Flush(grpc::CallbackServerContext *context,
          const GFSChunkServer::FlushRequest *request,
          GFSChunkServer::FlushResponse *response) override {
      /*Just to avoid errors for now*/
      if (true) {
        // if (server_info_.rpc_port == /*primary port*/ 6000) {

        std::mutex mu;

        struct secondary_flush_request_args_t {
          grpc::ClientContext context;
          GFSChunkServer::FlushRequest req;
          GFSChunkServer::FlushResponse res;
        };

        std::vector<secondary_flush_request_args_t>
            secondary_flush_request_args(3);

        std::vector<std::unique_ptr<GFSChunkServer::ChunkServerService::Stub>>
            stubs;
        std::atomic<int> counter = stubs.size();

        tcp_rpc_server_descriptor_t server1_info;
        server1_info.ip = "0.0.0.0";
        server1_info.rpc_port = 6501;
        server1_info.tcp_port = 6001;
        tcp_rpc_server_descriptor_t server2_info;
        server2_info.ip = "0.0.0.0";
        server2_info.rpc_port = 6502;
        server2_info.tcp_port = 6002;
        tcp_rpc_server_descriptor_t server3_info;
        server2_info.ip = "0.0.0.0";
        server2_info.rpc_port = 6503;
        server2_info.tcp_port = 6003;

        stubs.emplace_back(GFSChunkServer::ChunkServerService::NewStub(
            grpc::CreateChannel(grpc_connection_string(server1_info),
                                grpc::InsecureChannelCredentials())));
        stubs.emplace_back(GFSChunkServer::ChunkServerService::NewStub(
            grpc::CreateChannel(grpc_connection_string(server2_info),
                                grpc::InsecureChannelCredentials())));
        stubs.emplace_back(GFSChunkServer::ChunkServerService::NewStub(
            grpc::CreateChannel(grpc_connection_string(server3_info),
                                grpc::InsecureChannelCredentials())));

        std::condition_variable cv;
        bool done = false;

        int args_i = 0;
        for (std::unique_ptr<GFSChunkServer::ChunkServerService::Stub> &stub :
             stubs) {
          stub->async()->Flush(
              &secondary_flush_request_args[args_i].context,
              &secondary_flush_request_args[args_i].req,
              &secondary_flush_request_args[args_i].res,
              [&done, &cv, &mu, &counter](grpc::Status status) {
                bool ret = false;

                if (!status.ok()) {
                  MESSAGE("forwarding flush to secondaries failed");
                  ret = false;
                } else {
                  ret = true;
                }

                --counter;
                std::lock_guard<std::mutex> lock(mu);
                done = done && ret;
                cv.notify_one();
              });

          ++args_i;
        }

        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock, [&counter, &done] { return (counter == 0 and done); });
        response->set_flushed(true);
        auto *reactor = context->DefaultReactor();
        reactor->Finish(grpc::Status::OK);
        return reactor;
      } else {
        response->set_flushed(true);
        auto *reactor = context->DefaultReactor();
        reactor->Finish(grpc::Status::OK);
        return reactor;
      }
    }

  private:
    ChunkServer *chunkserver;
  };

  ChunkServer(const chunkserver_master_connection_descriptor_t &server_info)
      : chunk_descriptor_list_{std::make_shared<
            std::vector<ChunkDescriptor>>()},
        chunkserver_master_connection_info_{server_info},
        chunk_server_service_{this},
        master_stub_{GFSMaster::ChunkServerService::NewStub(
            grpc::CreateChannel(grpc_connection_string<rpc_server_descriptor_t>(
                                    chunkserver_master_connection_info_.master),
                                grpc::InsecureChannelCredentials()))} {
    load_chunks_from_filesystem();
    grpc::ServerBuilder builder;
    builder.AddListeningPort(
        grpc_connection_string(
            chunkserver_master_connection_info_.chunk_server),
        grpc::InsecureServerCredentials());
    builder.RegisterService(&chunk_server_service_);
    server_ = std::unique_ptr<grpc::Server>(builder.BuildAndStart());
  }

  void Announce() {
    grpc::ClientContext context;
    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    bool result;

    GFSMaster::RegisterChunkServerRequest request;
    request.mutable_server_info()->set_ip(
        chunkserver_master_connection_info_.chunk_server.ip);
    request.mutable_server_info()->set_rpc_port(
        chunkserver_master_connection_info_.chunk_server.rpc_port);

    GFSMaster::RegisterChunkServerResponse response;

    master_stub_->async()->RegisterChunkServer(
        &context, &request, &response,
        [&result, &mu, &cv, &done, &response](grpc::Status status) {
          bool ret;
          if (!status.ok()) {
            ret = false;
          } else if (response.acknowledged() == true) {
            ret = true;
          } else {
            ret = false;
          }
          std::lock_guard<std::mutex> lock(mu);
          result = ret;
          done = true;
          cv.notify_one();
        });
    // std::cout << "ACK2: " << response.acknowledged() << "\n";
    std::unique_lock<std::mutex> lock(mu);
    cv.wait(lock, [&done] { return done; });
  }

  void start() {
    tcp_worker_thread = std::thread(tcp_server_worker, this);
    // spawn tcp thread.
    server_->Wait();
  }

  friend void tcp_server_worker(ChunkServer *chunk_server);

private:
  void load_chunks_from_filesystem() {
    std::filesystem::path chunks_folder{"/home/nitro/.local/share/gfs/"};
    for (auto const &directory_entry :
         std::filesystem::directory_iterator(chunks_folder)) {
      std::string path = directory_entry.path();
      ChunkDescriptor desc{
          path.substr(path.rfind("/") + 1, path.size() - path.rfind("/")),
          directory_entry.file_size()};
      chunk_descriptor_list_->push_back(desc);
    }
    chunk_descriptor_list_->shrink_to_fit();
  }

  std::shared_ptr<std::vector<ChunkDescriptor>> chunk_descriptor_list_;
  /* ChunkServer-Master Connection Info */
  chunkserver_master_connection_descriptor_t
      chunkserver_master_connection_info_;
  /* Stub */
  std::unique_ptr<GFSMaster::ChunkServerService::Stub> master_stub_;
  /* Services */
  ChunkServerServiceImplementation chunk_server_service_;
  /* Server */
  std::unique_ptr<grpc::Server> server_;

  std::thread tcp_worker_thread;

  /*LRUCache...
   * map:- write id -> buffer
   * */

  /* std::shared_ptr<std::vector<ServerController>> */
  /*
   * primary queues writes when it gets a write request from the client it
   * checks the order the secondaries don't check the oerder What's the best
   * data structure */
};

void tcp_server_worker(ChunkServer *chunk_server) {
  TCPServer tcp_server(
      chunk_server->chunkserver_master_connection_info_.chunk_server);
  tcp_server.wait([chunk_server](int socket) {
    tcp_rpc_server_descriptor_t next_chunk_server =
        chunk_server->chunkserver_master_connection_info_.chunk_server;
    next_chunk_server.tcp_port++;

    if (next_chunk_server.tcp_port < 6004) {
      TCPClient client{next_chunk_server};
      client.connectToServer();
    }

    rpc_server_descriptor_t client_server_info{"0.0.0.0", 4500};
    std::unique_ptr<GFSClient::GFSClientService::Stub> client_stub =
        GFSClient::GFSClientService::NewStub(
            grpc::CreateChannel(grpc_connection_string(client_server_info),
                                grpc::InsecureChannelCredentials()));

    grpc::ClientContext context;
    GFSClient::AcknowledgeDataReceiptRequest request;
    GFSClient::AcknowledgeDataReceiptResponse response;

    client_stub->AcknowledgeDataReceipt(&context, request, &response);
  });
}

int main(int argc, char *argv[]) {
  if (argc < 11) {
    MESSAGE_END_EXIT(
        "USAGE: ./server --ip <ipv4-address> --rpc-port <port> --tcp-port "
        "<port> --master-ip <ipv4-address> --master-port <port> ");
  }
  chunkserver_master_connection_descriptor_t
      chunkserver_master_connection_info = parse_cli_args(argv + 1, argc - 1);
  // std::cout << chunkserver_master_connection_info << "\n";

  ChunkServer main_server(chunkserver_master_connection_info);
  /* Announce to the master */
  main_server.Announce();
  main_server.start();
}
