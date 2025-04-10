#include <algorithm>
#include <iostream>
#include <condition_variable>
#include <filesystem>
#include <google/protobuf/json/json.h>
#include <google/protobuf/message.h>
#include <grpcpp/channel.h>

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
#include <regex>
#include <type_traits>
#include <vector>

#include "../headers/main.h"
#include "./utils.h"

#include "../master/generated/GFSMasterService.grpc.pb.h"

#include "generated/GFSChunkServer.grpc.pb.h"
#include "generated/GFSChunkServer.pb.h"

#include "../lib/tcp/tcp_client.h"
#include "../lib/tcp/tcp_server.h"

class ChunkServer;
void tcp_server_worker(ChunkServer *chunk_server);

struct ChunkDescriptor {
  std::string name;
  std::size_t size;
  std::queue<std::string> write_order_queue;
};

class SecondaryChunkServerController {
public:
  SecondaryChunkServerController(const tcp_rpc_server_descriptor_t &server_info)
      : server_info_{server_info},
        secondary_server_stub_{GFSChunkServer::ChunkServerService::NewStub(
            grpc::CreateChannel(grpc_connection_string(server_info),
                                grpc::InsecureChannelCredentials()))} {}

  void assignSecondary(std::atomic<int> &counter, std::condition_variable &cv,
                       std::mutex &mu, bool &acknowledged) {
    grpc::ClientContext context;
    GFSChunkServer::AssignSecondaryRequest req;
    GFSChunkServer::AssignSecondaryResponse res;
    secondary_server_stub_->async()->AssignSecondary(
        &context, &req, &res,
        [&acknowledged, &mu, &cv, &counter, &res](grpc::Status status) {
          bool result;
          if (!status.ok()) {
            MESSAGE("NOT OK CALL TO SECONDARY");
            result = false;
          } else {
            result = res.acknowledgment();
          }
          --counter;
          std::lock_guard<std::mutex> lock(mu);
          acknowledged &= result;
          cv.notify_one();
        });
  }

private:
  tcp_rpc_server_descriptor_t server_info_;
  std::unique_ptr<GFSChunkServer::ChunkServerService::Stub>
      secondary_server_stub_;
};

class ChunkServerServiceImplementation final
    : public GFSChunkServer::ChunkServerService::CallbackService {
public:
  ChunkServerServiceImplementation(
      std::shared_ptr<std::vector<ChunkDescriptor>> chunk_descriptor_list,
			tcp_rpc_server_descriptor_t server_info)
      : chunk_descriptor_list_{chunk_descriptor_list},
				server_info_{server_info} {}

  grpc::ServerWriteReactor<GFSChunkServer::ChunkMetadata> *UploadChunkMetadata(
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
          Finish(grpc::Status(grpc::StatusCode::UNKNOWN, "Unexpected Failure"));
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

		MESSAGE("[LOG]: Uploading chunks metadata.");

    return new Writer(chunk_descriptor_list_);
  }

  grpc::ServerUnaryReactor *
  HeartBeat(grpc::CallbackServerContext *context,
            const GFSChunkServer::HeartBeatRequest *request,
            GFSChunkServer::HeartBeatResponse *response) override {
    MESSAGE("[LOG]: HEARTBEAT...");
    response->set_extend_lease(true);
    auto *reactor = context->DefaultReactor();
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }

  grpc::ServerUnaryReactor *
  AssignPrimary(grpc::CallbackServerContext *context,
                const GFSChunkServer::AssignPrimaryRequest *request,
                GFSChunkServer::AssignPrimaryResponse *response) override {
    std::atomic<int> counter = request->secondary_servers().size();
    bool acknowledged = false;
    std::mutex mu;
    std::condition_variable cv;

    // set lease in local in memory chunk structure
    MESSAGE("[LOG]: Setting local lease.");
    // forward the lease to the secondaries
		// MESSAGE("client ip: " << request->client_server().ip());
    // MESSAGE("client port: " << request->client_server().rpc_port());

    tcp_rpc_server_descriptor_t server_info;

    std::vector<SecondaryChunkServerController> controllers;

    for (int i = 0; i < request->secondary_servers().size(); ++i) {
      MESSAGE("[LOG]: secondary ip: " << request->secondary_servers(i).ip());
      server_info.ip = request->secondary_servers(i).ip();
      MESSAGE("[LOG]: secondary port: " << request->secondary_servers(i).rpc_port());
      server_info.rpc_port = request->secondary_servers(i).rpc_port();
      SecondaryChunkServerController &controller =
          controllers.emplace_back(server_info);
      controller.assignSecondary(counter, cv, mu, acknowledged);
    }

    std::unique_lock<std::mutex> lock(mu);
    cv.wait(lock, [&counter] { return counter == 0; });
    // responde with acknowledegement
    response->set_acknowledgment(acknowledged);
    auto *reactor = context->DefaultReactor();
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }

  grpc::ServerUnaryReactor *
  AssignSecondary(grpc::CallbackServerContext *context,
                  const GFSChunkServer::AssignSecondaryRequest *request,
                  GFSChunkServer::AssignSecondaryResponse *response) override {
    MESSAGE("[LOG]: Setting local lease (Secondar)");

    MESSAGE("[->]: client ip: " << request->client_server().ip());
    MESSAGE("[->]: client port: " << request->client_server().rpc_port());

    response->set_acknowledgment(true);
    auto *reactor = context->DefaultReactor();
    reactor->Finish(grpc::Status::OK);
    return reactor;
  }

  grpc::ServerUnaryReactor* Flush
	(
      grpc::CallbackServerContext* context,
			const GFSChunkServer::FlushRequest* request,
			GFSChunkServer::FlushResponse* response
	)
		override
	{ 
		MESSAGE("[LOG]: Request to flush.");
		if (server_info_.rpc_port == /*primary port*/ 6000) {

			std::mutex mu;
			grpc::ClientContext client_context;
			GFSChunkServer::FlushRequest req;
			GFSChunkServer::FlushResponse res;

			std::vector<std::unique_ptr<GFSChunkServer::ChunkServerService::Stub>> stubs;
			std::atomic<int> counter = stubs.size();
			stubs.emplace_back();

			std::condition_variable cv;
			bool done = false;

			for(std::unique_ptr<GFSChunkServer::ChunkServerService::Stub>& stub: stubs) {
				stub->async()->Flush(&client_context, &req, &res, [&done, &cv, &mu, &counter](grpc::Status status){
					
					bool ret = false;

					if(!status.ok()) {
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

			std::unique_lock<std::mutex> lock(mu);	
			cv.wait(lock, [&counter, &done]{return (counter == 0 and done); });
			response->set_flushed(true);
			auto* reactor = context->DefaultReactor();
			reactor->Finish(grpc::Status::OK);
			return reactor;	
		} else {
			response->set_flushed(true);
			auto* reactor = context->DefaultReactor();
			reactor->Finish(grpc::Status::OK);
			return reactor;	
		}
	}

	private:
  std::shared_ptr<std::vector<ChunkDescriptor>> chunk_descriptor_list_;
	tcp_rpc_server_descriptor_t server_info_;
};

class ChunkServer {
public:
  ChunkServer(const chunkserver_master_connection_descriptor_t &server_info)
      : chunk_descriptor_list_{std::make_shared<
            std::vector<ChunkDescriptor>>()},
        chunkserver_master_connection_info_{server_info},
        chunk_server_service_{chunk_descriptor_list_, server_info.chunk_server},
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
    std::thread tcp_thread(tcp_server_worker, this);
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

private:
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
  });

}

int main(int argc, char *argv[]) {
  if (argc < 11) {
    MESSAGE_END_EXIT(
        "USAGE: ./server --ip <ipv4-address> --rpc-port <port> --tcp-port "
        "<port> --master-ip <ipv4-address> --master-port <port> ");
  }

	MESSAGE("[LOG]: Parsing CLI arguments.");
  chunkserver_master_connection_descriptor_t
      chunkserver_master_connection_info = parse_cli_args(argv + 1, argc - 1);
	MESSAGE("[LOG]: chunkserver-masterserver connection information: ");
	MESSAGE("[->]: server ip: " << chunkserver_master_connection_info.chunk_server.ip);
	MESSAGE("[->]: server rpc port: " << chunkserver_master_connection_info.chunk_server.rpc_port);
	MESSAGE("[->]: server tcp port: " << chunkserver_master_connection_info.chunk_server.tcp_port);
	MESSAGE("[->]: master ip: " << chunkserver_master_connection_info.master.ip);
	MESSAGE("[->]: master rpc port: " << chunkserver_master_connection_info.master.rpc_port);
  // std::cout << chunkserver_master_connection_info << "\n";

  ChunkServer main_server(chunkserver_master_connection_info);
  /* Announce to the master */
	MESSAGE("[LOG]: Announcing to master server.");
  main_server.Announce();
	MESSAGE("[LOG]: Server starts waiting for rpc calls.");
  main_server.start();
}
