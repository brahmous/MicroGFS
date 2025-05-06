#include <algorithm>
#include <array>
#include <cassert>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/json/json.h>
#include <google/protobuf/message.h>
#include <grpcpp/channel.h>
#include <grpcpp/server_context.h>

#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

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
#include <sstream>
#include <string>
#include <sys/types.h>
#include <utility>
#include <vector>

#include "../headers/main.h"
#include "./utils.h"

#include "../master/generated/GFSMasterService.grpc.pb.h"

#include "generated/GFSChunkServer.grpc.pb.h"
#include "generated/GFSChunkServer.pb.h"

#include "../client/generated/GFSClientService.grpc.pb.h"
#include "../client/generated/GFSClientService.pb.h"

#include "../lib/logger/logger.h"
#include "../lib/lru_cache/lru_cache.h"
#include "../lib/tcp/tcp_client.h"
#include "../lib/tcp/tcp_server.h"
#include "./server.h"

class ChunkServer;
void tcp_server_worker(ChunkServer *chunk_server);

struct ChunkDescriptor {
  std::size_t size;
  std::queue<int> write_order_queue;
  bool leased;
};

class ChunkServerController;
class ClientController;

struct WriteContext {
  uint64_t handle;
  std::size_t offset;
  std::vector<ChunkServerController> flush_list;
  tcp_rpc_server_descriptor_t forward_to;
  std::shared_ptr<ClientController> client;
  int write_size;
};

struct assign_secondary_request_args {
  grpc::ClientContext context;
  GFSChunkServer::AssignSecondaryRequest request;
  GFSChunkServer::AssignSecondaryResponse response;
};

struct secondary_flush_request_args_t {
  grpc::ClientContext context;
  GFSChunkServer::FlushRequest req;
  GFSChunkServer::FlushResponse res;
};

class ClientController {
public:
  ClientController(const rpc_server_descriptor_t &server_info)
      : server_info_{server_info},
        stub_{GFSClient::GFSClientService::NewStub(
            grpc::CreateChannel(grpc_connection_string(server_info),
                                grpc::InsecureChannelCredentials()))} {}

  void AcknowledgeDataReceipt(int write_id) {

    grpc::ClientContext context;
    GFSClient::AcknowledgeDataReceiptRequest request;
    GFSClient::AcknowledgeDataReceiptResponse response;

    request.set_write_id(write_id);

    MAINLOG_INFO("set write id {}", write_id);

    grpc::Status status =
        stub_->AcknowledgeDataReceipt(&context, request, &response);

    if (status.ok()) {
      MAINLOG_INFO("ACKNOWLEDGED TO THE CLIENT");
    } else {
      MAINLOG_ERROR("ACKNOWLEDGEMENT TO THE CLIENT FAILED");
    }
  }

  std::unique_ptr<GFSClient::GFSClientService::Stub> stub_;
  rpc_server_descriptor_t server_info_;
};

class ChunkServerController {
public:
  ChunkServerController(const tcp_rpc_server_descriptor_t &server_info,
                        uint64_t handle)
      : server_info_{server_info}, handle_{handle},
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
          // MESSAGE("COUNTER: " << counter);
          bool result;
          if (!status.ok()) {
            MAINLOG_INFO("NOT OK CALL TO SECONDARY");
            result = false;
          } else {
            result = args.response.body().acknowledgment();
          }
          std::lock_guard<std::mutex> lock(mu);
          counter -= 1;
          acknowledged = acknowledged && result;
          cv.notify_one();
        });
  }

  void Flush(secondary_flush_request_args_t &args, std::atomic<int> &counter,
             std::mutex &mu, std::condition_variable &cv, bool &done) {
    secondary_server_stub_->async()->Flush(
        &(args.context), &(args.req), &(args).res,
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
  }

  tcp_rpc_server_descriptor_t server_info_;

private:
  std::unique_ptr<GFSChunkServer::ChunkServerService::Stub>
      secondary_server_stub_;
  uint64_t handle_;
};

class ChunkServer {
public:
  class ChunkServerServiceImplementation final
      : public GFSChunkServer::ChunkServerService::CallbackService {
  public:
    ChunkServerServiceImplementation(ChunkServer *chunk_server)
        : chunk_server_{chunk_server} {}

    grpc::ServerWriteReactor<GFSChunkServer::ChunkMetadata> *
    UploadChunkMetadata(
        grpc::CallbackServerContext *context,
        const GFSChunkServer::UploadChunkMetadataRequest *request) override {

      MAINLOG_INFO("Uploading chunks metadata.");

      class Writer
          : public grpc::ServerWriteReactor<GFSChunkServer::ChunkMetadata> {
      public:
        Writer(ChunkServer *chunk_server)
            : chunk_server_{chunk_server},
              it_{chunk_server->chunk_dictionary_.begin()} {
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
          if (it_ == chunk_server_->chunk_dictionary_.end()) {
            Finish(grpc::Status::OK);
            return;
          }
          descriptor_.set_handle(it_->first);
          descriptor_.set_chunk_size(it_->second.size);
          ++it_;
          StartWrite(&descriptor_);
          // Finish(grpc::Status::OK);
          return;
        }

        GFSChunkServer::ChunkMetadata descriptor_;
        ChunkServer *chunk_server_;
        std::map<uint64_t, ChunkDescriptor>::const_iterator it_;
      };

      return new Writer(chunk_server_);
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

      MAINLOG_WARN("(14) I'M The Primary");
      std::atomic<int> counter = request->secondary_servers().size();
      int c = counter;
      MAINLOG_INFO(
          "(15) Number of secondary server to assign as secondaries: {}", c);

      bool acknowledged = true;
      std::mutex mu;
      std::condition_variable cv;

      WriteContext write_context;
      write_context.handle = request->handle();
      write_context.write_size = request->write_size();

      rpc_server_descriptor_t client_server_info;
      client_server_info.ip = request->client_server().ip();
      client_server_info.rpc_port = request->client_server().rpc_port();
      write_context.client =
          std::make_shared<ClientController>(client_server_info);

      write_context.forward_to.ip = request->forward_to().ip();
      write_context.forward_to.tcp_port = request->forward_to().tcp_port();
      write_context.forward_to.rpc_port = request->forward_to().rpc_port();

      MAINLOG_INFO("(16) Primary write-context setup: handle {}, forwar to "
                   "server [ip: {}, tcp port: {}, rpc port: {}]",
                   write_context.handle, write_context.forward_to.ip,
                   write_context.forward_to.tcp_port,
                   write_context.forward_to.rpc_port);

      MAINLOG_INFO("(17) Primary write-context flush list setup.");

      for (int i = 0; i < request->secondary_servers().size(); ++i) {
        tcp_rpc_server_descriptor_t forward_server_info;
        forward_server_info.ip =
            request->secondary_servers().at(i).server_info().ip();
        forward_server_info.tcp_port =
            request->secondary_servers().at(i).server_info().tcp_port();
        forward_server_info.rpc_port =
            request->secondary_servers().at(i).server_info().rpc_port();

        MAINLOG_INFO(
            "(18) flush list server {}, [ip: {}, tcp port {}, rpc port {}]",
            i + 1, forward_server_info.ip, forward_server_info.tcp_port,
            forward_server_info.rpc_port);

        write_context.flush_list.emplace_back(
            ChunkServerController(forward_server_info, request->handle()));
      }

      std::vector<assign_secondary_request_args> args(
          request->secondary_servers().size());

      int args_i = 0;
      for (ChunkServerController &controller : write_context.flush_list) {
        args[args_i].request.set_write_size(request->write_size());
        args[args_i].request.set_write_id(request->write_id());
        args[args_i].request.set_handle(
            request->secondary_servers().at(args_i).handle());
        args[args_i].request.set_forward(
            request->secondary_servers().at(args_i).forward());
        args[args_i].request.mutable_client_server()->set_ip(
            request->client_server().ip());
        args[args_i].request.mutable_client_server()->set_rpc_port(
            request->client_server().rpc_port());
        if (args[args_i].request.forward() and
            args_i + 1 < write_context.flush_list.size()) {
          args[args_i].request.mutable_forward_server()->set_ip(
              request->secondary_servers().at(args_i + 1).forward_to().ip());
          args[args_i].request.mutable_forward_server()->set_tcp_port(
              request->secondary_servers()
                  .at(args_i + 1)
                  .forward_to()
                  .tcp_port());
          args[args_i].request.mutable_forward_server()->set_rpc_port(
              request->secondary_servers()
                  .at(args_i + 1)
                  .forward_to()
                  .rpc_port());
        }

        MAINLOG_WARN("Secondary server ip: {}", controller.server_info_.ip);
        MAINLOG_WARN("Secondary server tcp port: {}",
                     controller.server_info_.tcp_port);
        MAINLOG_WARN("Secondary server rpc port: {}",
                     controller.server_info_.rpc_port);

        controller.assignSecondary(counter, cv, mu, acknowledged, args[args_i]);
        ++args_i;
      }

      std::unique_lock<std::mutex> lock(mu);
      cv.wait(lock, [&counter] { return counter == 0; });
      // responde with acknowledegement
      /*NOTE: UNCOMMENT*/ // response->set_acknowledgment(acknowledged);

      if (acknowledged == true) {
        MAINLOG_INFO(
            "(25) Secondary server acknowledged being ready to handle write");

        chunk_server_->write_id_to_context_map_.insert(
            {request->write_id(), std::move(write_context)});

        if (auto find_result =
                chunk_server_->chunk_dictionary_.find(request->handle());
            find_result == chunk_server_->chunk_dictionary_.end()) {
          auto [it, inserted] = chunk_server_->chunk_dictionary_.insert(
              std::make_pair(request->handle(), ChunkDescriptor()));

          it->second.leased = true;
          it->second.write_order_queue.push(request->write_id());

        } else {
          find_result->second.leased = true;
          find_result->second.write_order_queue.push(request->write_id());
        }

        // chunk_server_->chunk_dictionary_[request->handle()].leased = true;
        // chunk_server_->chunk_dictionary_[request->handle()]
        //    .write_order_queue.push(request->write_id());
        //

        MAINLOG_WARN("write id: {}, write size: {}", request->write_id(),
                     request->write_size());
        chunk_server_->buffer_cache.insert(std::make_pair(
            request->write_id(), std::vector<char>(request->write_size())));
        response->set_status(GFSChunkServer::Status::SUCCESS);
        response->mutable_body()->set_acknowledgment(true);
      } else {
        MAINLOG_INFO("(25) Secondary server did not acknowledge being ready to "
                     "handle write");
        response->set_status(GFSChunkServer::Status::ERROR);
        response->mutable_error()->set_error_code(2030);
        response->mutable_error()->set_error_message(
            std::string("ERROR Assignging the secondary servers for write ")
                .append(std::to_string(request->write_id())));
      }
      auto *reactor = context->DefaultReactor();
      reactor->Finish(grpc::Status::OK);
      MAINLOG_INFO("(26) Responding to the primary with acknowledgment");
      return reactor;
    }

    grpc::ServerUnaryReactor *AssignSecondary(
        grpc::CallbackServerContext *context,
        const GFSChunkServer::AssignSecondaryRequest *request,
        GFSChunkServer::AssignSecondaryResponse *response) override {

      MAINLOG_INFO("(19) I'm The Secondary");

      uint64_t handle = request->handle();
      int write_id = request->write_id();

      if (auto chunk_dictionary_entry_fr =
              chunk_server_->chunk_dictionary_.find(handle);
          chunk_dictionary_entry_fr == chunk_server_->chunk_dictionary_.end()) {

        auto [it, inserted] = chunk_server_->chunk_dictionary_.insert(
            std::make_pair(handle, ChunkDescriptor()));
        it->second.leased = false;
        it->second.write_order_queue.push(write_id);
      } else {
        auto [handle_k, descriptor_v] = *chunk_dictionary_entry_fr;
        descriptor_v.leased = false;
        descriptor_v.write_order_queue.push(write_id);
      }

      response->mutable_body()->set_acknowledgment(true);
      MAINLOG_INFO("(20) Response body set to true");

      //*TODO!IMPORTANT: add offset to write context and maybe size*//
      WriteContext write_context;
      write_context.handle = request->handle();
      write_context.forward_to.ip = request->forward_server().ip();
      write_context.forward_to.tcp_port = request->forward_server().tcp_port();
      write_context.forward_to.rpc_port = request->forward_server().rpc_port();
      write_context.write_size = request->write_size();

      MAINLOG_INFO("(21) Write context setup! handle {}, forward to server "
                   "[ip: {}, tcp port: {}, rpc port {}]]",
                   write_context.handle, write_context.forward_to.ip,
                   write_context.forward_to.tcp_port,
                   write_context.forward_to.tcp_port);

      rpc_server_descriptor_t client_server_info;
      client_server_info.ip = request->client_server().ip();
      client_server_info.rpc_port = request->client_server().rpc_port();
      write_context.client =
          std::make_shared<ClientController>(client_server_info);

      MAINLOG_INFO("(22) Write context setup! client server "
                   "[ip: {}, rpc port: {}]]",
                   client_server_info.ip, client_server_info.rpc_port);

      chunk_server_->write_id_to_context_map_.insert(
          std::make_pair(request->write_id(), std::move(write_context)));

      MAINLOG_WARN("write size: {}", request->write_size());
      chunk_server_->buffer_cache.insert(std::make_pair(
          request->write_id(), std::vector<char>(request->write_size())));

      MAINLOG_INFO("(23) Allocated buffer on LRU cache with write id: {}",
                   request->write_id());

      response->set_status(GFSChunkServer::Status::SUCCESS);
      response->mutable_body()->set_acknowledgment(true);
      auto *reactor = context->DefaultReactor();
      reactor->Finish(grpc::Status::OK);
      MAINLOG_INFO("(24) Responding to primary with acknowledgment!");
      return reactor;
    }

    grpc::ServerUnaryReactor *
    Flush(grpc::CallbackServerContext *context,
          const GFSChunkServer::FlushRequest *request,
          GFSChunkServer::FlushResponse *response) override {
      MAINLOG_ERROR("FLUSH CALLED");

      MAINLOG_ERROR("################ FLUSH START ####################");

      MAINLOG_INFO("request write id: {}", request->write_id());

      int write_id = request->write_id();

      WriteContext &write_context =
          chunk_server_->write_id_to_context_map_.find(request->write_id())
              ->second;

      std::string str(chunk_server_->buffer_cache[request->write_id()].data());
      MAINLOG_INFO("DATA TO BE FLUSHED TO THE DISK: {}", str);

      MAINLOG_INFO("Write context:\n\thandle: {}\n\t "
                   "client:\n\t\tip: {}\n\t\trpc port: {}\n\t\twrite size: {}",
                   write_context.handle, write_context.client->server_info_.ip,
                   write_context.client->server_info_.rpc_port,
                   write_context.write_size);

      ChunkDescriptor &chunk_descriptor =
          chunk_server_->chunk_dictionary_.find(write_context.handle)->second;

      MAINLOG_INFO("Chunk Descriptor:\n\tleased: {}\n\tcurrent write: {}",
                   (chunk_descriptor.leased ? "TRUE" : "FALSE"),
                   chunk_descriptor.write_order_queue.front());

      if (chunk_descriptor.leased) {
        std::mutex mu;

        std::vector<secondary_flush_request_args_t>
            secondary_flush_request_args(write_context.flush_list.size());

        std::atomic<int> counter = write_context.flush_list.size();
        MAINLOG_INFO("Flush list size: {}", write_context.flush_list.size());

        std::condition_variable cv;
        bool done = true;

        int args_i = 0;

        for (ChunkServerController &chunk_server_controller :
             write_context.flush_list) {
          secondary_flush_request_args[args_i].req.set_write_id(
              request->write_id());
          chunk_server_controller.Flush(secondary_flush_request_args[args_i],
                                        counter, mu, cv, done);
          ++args_i;
        }

        std::unique_lock<std::mutex> lock(mu);
        cv.wait(lock, [&counter, &done] {
          MAINLOG_INFO("done: {}", (done ? "TRUE" : "FALSE"));
          return (counter == 0 and done);
        });
        std::string file_path = chunk_server_->storage_folder_path_;
        file_path.append(std::to_string(write_context.handle));
        if (auto buffer_fr = chunk_server_->buffer_cache.find(write_id);
            buffer_fr != chunk_server_->buffer_cache.end()) {
          char *buffer = buffer_fr->second.data();
          std::ofstream output_file;
          output_file.open(file_path, std::ios::out);
          output_file.write(buffer + 4, write_context.write_size - 4);
        } else {
          MAINLOG_ERROR("NO BUFFER");
        }
        MAINLOG_WARN("FILE PATH: {}", file_path);

        MAINLOG_INFO("Data Flushed (lease:✔ )");
        response->set_status(GFSChunkServer::Status::SUCCESS);
        response->mutable_body()->set_acknowledgement(true);
      } else {
        std::string file_path = chunk_server_->storage_folder_path_;
        file_path.append(std::to_string(write_context.handle));
        if (auto buffer_fr = chunk_server_->buffer_cache.find(write_id);
            buffer_fr != chunk_server_->buffer_cache.end()) {
          char *buffer = buffer_fr->second.data();
          std::ofstream output_file;
          output_file.open(file_path, std::ios::out);
          output_file.write(buffer + 4, write_context.write_size - 4);
        } else {
          MAINLOG_ERROR("NO BUFFER");
        }
        MAINLOG_WARN("FILE PATH: {}", file_path);
        MAINLOG_INFO("Data Flushed");
        response->set_status(GFSChunkServer::Status::SUCCESS);
        response->mutable_body()->set_acknowledgement(true);
      }
      auto *reactor = context->DefaultReactor();
      reactor->Finish(grpc::Status::OK);
      return reactor;
    }

  private:
    ChunkServer *chunk_server_;
  };

  ChunkServer(const chunk_server_config_t &config)
      : chunkserver_master_connection_info_{config
                                                .chunkserver_master_connection_info},
        storage_folder_path_{config.storage_file_path},
        chunk_server_service_{this}, /*buffer_cache{8, 4096},*/
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

    for (auto dictionary_entry : chunk_dictionary_) {
      MAINLOG_INFO("\nHandle: {}\n\tleased: {}\n\tsize: {}\n",
                   dictionary_entry.first,
                   (dictionary_entry.second.leased ? "TRUE" : "FALSE"),
                   dictionary_entry.second.size);
    }
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
    request.mutable_server_info()->set_tcp_port(
        chunkserver_master_connection_info_.chunk_server.tcp_port);
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
    std::filesystem::path chunks_folder{storage_folder_path_};
    for (auto const &directory_entry :
         std::filesystem::directory_iterator(chunks_folder)) {
      std::string path = directory_entry.path();

      uint64_t chunk_handle = std::atol(
          path.substr(path.rfind("/") + 1, path.size() - path.rfind("/"))
              .c_str());

      ChunkDescriptor chunk_descriptor;
      chunk_descriptor.leased = false;
      chunk_descriptor.size = directory_entry.file_size();

      chunk_dictionary_.insert(std::make_pair(chunk_handle, chunk_descriptor));
    }
  }

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

  std::map<uint64_t, ChunkDescriptor> chunk_dictionary_;
  std::unordered_map<int, WriteContext> write_id_to_context_map_;
  std::unordered_map<int, std::vector<char>> buffer_cache;
  std::string storage_folder_path_;

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

  /*TCP Data Transfer Lambda*/
  tcp_server.wait([chunk_server](int socket) {
    MAINLOG_WARN("#### HANDLING TCP TRANSMISSION ####");

    size_t total_bytes_received = 0;
    int write_id = 0;
    int recv_write_id_result = recv(socket, &write_id, sizeof write_id, 0);

    if (recv_write_id_result < 0) {
      MAINLOG_ERROR("Error: this is ");
      return;
    }

    total_bytes_received = recv_write_id_result;

    MAINLOG_ERROR("recv_write_id_result: {}", recv_write_id_result);
    MAINLOG_INFO("WRITE ID: {}", write_id);

    WriteContext &write_context =
        chunk_server->write_id_to_context_map_.find(write_id)->second;

    char *buffer = chunk_server->buffer_cache[write_id].data();
    // char *buffer = new char[4096];
    // memcpy(buffer, &write_id, sizeof write_id);
    memcpy(buffer, &write_id, sizeof(write_id));

    MAINLOG_WARN("write_id 2: {}", *reinterpret_cast<int *>(buffer));

    int buffer_size = write_context.write_size;

    if (write_context.forward_to.ip.size() != 0) {
      MAINLOG_INFO("I HAVE SERVER TO FORWARD TO: {}",
                   write_context.forward_to.ip.size());
      TCPClient forward_to_server_(write_context.forward_to);
      forward_to_server_.connectToServer();

      int bytes_sent = 0;

      while (total_bytes_received < buffer_size) {
        int recv_return = recv(socket, buffer + total_bytes_received,
                               buffer_size - total_bytes_received, 0);
        /*
MAINLOG_WARN("************ recv return: {}, string received: {}, total "
"string {}",
recv_return, buffer + total_bytes_received, buffer);
        */

        MAINLOG_WARN("************ recv return: {}", recv_return);

        if (recv_return < 0) {
          MAINLOG_ERROR("Error receiving bytes");
          break;
        }

        total_bytes_received += recv_return;

        /*TODO: Fix send positions is forward offset by 4 bytes*/
        while (bytes_sent < total_bytes_received) {
          int send_return =
              send(forward_to_server_.socket_, buffer + bytes_sent,
                   total_bytes_received - bytes_sent, 0);

          MAINLOG_WARN("send return: {}", send_return);
          if (send_return < 0) {
            MAINLOG_ERROR("Error sending bytes");
            break;
          }
          bytes_sent += send_return;
        }
        bytes_sent = 0;
      }
    } else {
      MAINLOG_INFO("I DON'T HAVE SERVER TO FORWARD TO: {}",
                   write_context.forward_to.ip.size());
      while (total_bytes_received < buffer_size) {
        int recv_return = recv(socket, buffer + total_bytes_received,
                               buffer_size - total_bytes_received, 0);
        if (recv_return < 0) {
          MAINLOG_ERROR("Error receiving bytes");
          break;
        }
        total_bytes_received += recv_return;
      }
    }

    // MAINLOG_WARN("ALL DATA HAVE BEEN WRITTEN: {}", buffer+4);
    std::shared_ptr<ClientController> client_controller = write_context.client;
    client_controller->AcknowledgeDataReceipt(write_id);
  });
}

int main(int argc, char *argv[]) {
  GFSLogger::Logger::init();
  if (argc < 12) {
    MESSAGE_END_EXIT(
        "USAGE: ./server --ip <ipv4-address> --rpc-port <port> --tcp-port "
        "<port> --master-ip <ipv4-address> --master-port <port> "
        "--storage-folder <storage folder path>");
  }
  // std::cout << chunkserver_master_connection_info << "\n";
  chunk_server_config_t config = parse_cli_args(argv + 1, argc - 1);
  ;
  ChunkServer main_server(config);
  /* Announce to the master */
  main_server.Announce();
  main_server.start();
}
