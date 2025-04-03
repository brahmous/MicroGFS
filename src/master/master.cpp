#include <absl/strings/str_format.h>
#include <condition_variable>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/status.h>
#include <iostream>

#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <memory>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <vector>

#include "../main.h"
#include "headers/cli-args-parser.h"
#include "generated/GFSMasterService.pb.h"
#include "generated/GFSMasterService.grpc.pb.h"
#include "../server/generated/GFSChunkServer.grpc.pb.h"

class ChunkServerController{
public:
	ChunkServerController(const ServerInfo& server_info):
		server_info_{server_info}, 
		chunk_server_stub_{
			GFSChunkServer::ChunkServerService::NewStub(
				grpc::CreateChannel(
					server_info_.my_ip_port_string(),
					grpc::InsecureChannelCredentials()
				)
			)
		}
	{
	}
	ServerInfo server_info_;
	std::unique_ptr<GFSChunkServer::ChunkServerService::Stub> chunk_server_stub_;
};

class ChunkMetadataReader: public grpc::ClientReadReactor<GFSChunkServer::ChunkMetadata> {
public:

	ChunkMetadataReader(GFSChunkServer::ChunkServerService::Stub * stub)
	{
		stub->async()->UploadChunkMetadata(&context_, &req_, this);
		StartRead(&chunk_metadata_);
		StartCall();
	}

	void OnReadDone(bool OK) override 
	{
		if (OK) {
			StartRead(&chunk_metadata_);
		}
	}

	void OnDone (const grpc::Status& s) override 
	{
		std::unique_lock<std::mutex> l(mu_);
		status_ = s;
		done_ = true;
		cv_.notify_one();
	}

	grpc::Status Await() 
	{
		std::unique_lock<std::mutex> l(mu_);
		cv_.wait(l, [this] {return done_;});
		return std::move(status_);
	}

private:
	GFSChunkServer::UploadChunkMetadataRequest req_;
	grpc::ClientContext context_;
	GFSChunkServer::ChunkMetadata chunk_metadata_;
	std::mutex mu_;
	std::condition_variable cv_;
	grpc::Status status_;
	bool done_ = false;
};

//void heartbeat_handler(GFSChunkServer::ChunkServerService::Stub * stub) {
void heartbeat_handler(ChunkServerController * controller) {
	//sleep(2);
	ChunkMetadataReader reader(controller->chunk_server_stub_.get());
	grpc::Status status = reader.Await();

	if(status.ok()) {
		std::cout << "Reader is working\n";
	} else {
		std::cout << "Reader is not working\n";
	}
	int count = 5;
	while (--count > 0) {

		grpc::ClientContext context;
		GFSChunkServer::HeartBeatRequest request;
		GFSChunkServer::HeartBeatResponse response;

		std::cout << "Sleeping.....\n";
		sleep(2);
		MESSAGE("HEARTBEAT...");
		controller->chunk_server_stub_->HeartBeat(&context, request, &response);
		MESSAGE("here");
		//MESSAGE("extend lease: " << (response.extend_lease() ? "true" : "false"));
	}
}

class GFSMasterServerServiceImplementation : public GFSMaster::ChunkServerService::CallbackService {
public:
	GFSMasterServerServiceImplementation (
		std::shared_ptr<std::vector<ChunkServerController>> chunk_servers_info,
		std::shared_ptr<std::vector<std::thread>> chunk_servers_heartbeat_threads
	): 
		chunk_servers_info_sp_{chunk_servers_info},
		chunk_servers_heartbeat_threads_sp_{chunk_servers_heartbeat_threads}
	{}

	grpc::ServerUnaryReactor* RegisterChunkServer (
					grpc::CallbackServerContext* context,
					const GFSMaster::RegisterChunkServerRequest* request,
					GFSMaster::RegisterChunkServerResponse* response
	) override 
	{
		ServerInfo server_info (request->server_info().ip(), request->server_info().port());
		ChunkServerController& controller = chunk_servers_info_sp_->emplace_back(server_info);

		chunk_servers_heartbeat_threads_sp_
			->emplace_back(std::thread(
				heartbeat_handler, &controller
			)); 

		GFSMaster::RegisterChunkServerResponse res;
		res.set_acknowledged(true);

		response->CopyFrom(res);

		auto* reactor = context->DefaultReactor();
		reactor->Finish(grpc::Status::OK);
		return reactor;
	}

	private:
	std::shared_ptr<std::vector<ChunkServerController>> chunk_servers_info_sp_;
	std::shared_ptr<std::vector<std::thread>> chunk_servers_heartbeat_threads_sp_;
};

class MasterServer {
public:
	MasterServer(const ServerInfo & server_info): 
		server_info(server_info),
		chunk_servers_controllers_{std::make_shared<std::vector<ChunkServerController>>()},
		chunk_servers_heartbeat_threads_{std::make_shared<std::vector<std::thread>>()},
		master_service_{chunk_servers_controllers_, chunk_servers_heartbeat_threads_} 
	{
		grpc::ServerBuilder builder_;
		builder_.AddListeningPort(server_info.my_ip_port_string(), grpc::InsecureServerCredentials());
		//master_service_ = GFSMasterServer(chunk_servers_descriptors_, chunk_servers_heartbeat_threads_);
		builder_.RegisterService(&master_service_);
		server_ = std::unique_ptr(builder_.BuildAndStart());
	}

	void listen() {
		server_->Wait();
	} 

	~MasterServer(){
		for(std::thread & th: *chunk_servers_heartbeat_threads_) {
			th.join();
		}
	}

private:
	/*Chunk Servers Data*/
	std::shared_ptr<std::vector<ChunkServerController>> chunk_servers_controllers_; 
	std::shared_ptr<std::vector<std::thread>> chunk_servers_heartbeat_threads_;

	/*Service offered by the master to the chunk server*/
	ServerInfo server_info;
	std::unique_ptr<grpc::Server> server_;

	/* Services */
	GFSMasterServerServiceImplementation master_service_;

	/*Service offered by the chunk server to the client*/
	// dictionary
};

int main(int argc, char * argv[])
{

	if (argc < 5 or argc > 5) {
		MESSAGE_END_EXIT("USAGE: ./server --ip <ipv4-address> --rpc-port <port> --tcp-port <port> --master-ip <ipv4-address> --master-port <port>");
	}

	ServerInfo server_info(argv+1, argc-1);
	MasterServer master(server_info);

	MESSAGE("SERVER INFO");
	MESSAGE(server_info);
	master.listen();
}

