#include <condition_variable>
#include <filesystem>
#include <google/protobuf/message.h>
#include <grpcpp/channel.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/support/status.h>
#include <memory>
#include <mutex>
#include <netdb.h>
#include <vector>

#include "../headers/main.h"
#include "./utils.h"

#include "../master/generated/GFSMasterService.grpc.pb.h"

#include "generated/GFSChunkServer.grpc.pb.h"
#include "generated/GFSChunkServer.pb.h"

struct ChunkDescriptor {
	std::string name;
	std::size_t size;
};

class ChunkServerServiceImplementation final: 
	public GFSChunkServer::ChunkServerService::CallbackService
{
public:
	ChunkServerServiceImplementation (std::shared_ptr<std::vector<ChunkDescriptor>> chunk_descriptor_list): 
		chunk_descriptor_list_{chunk_descriptor_list} 
	{}

	grpc::ServerWriteReactor< GFSChunkServer::ChunkMetadata>* 
	UploadChunkMetadata(grpc::CallbackServerContext* context,
										 	const GFSChunkServer::UploadChunkMetadataRequest* request)
	override
	{ 
		class Writer : public grpc::ServerWriteReactor<GFSChunkServer::ChunkMetadata> {
		public:
			Writer(std::shared_ptr<std::vector<ChunkDescriptor>> chunk_descriptor_list):
				chunk_descriptor_list_{chunk_descriptor_list},
				it_ {chunk_descriptor_list_->begin()}
			{
				NextWrite();
			}

			void OnWriteDone(bool ok) override
			{
				if (!ok) {
					Finish(grpc::Status(grpc::StatusCode::UNKNOWN, "Unexpected Failure"));
				}
				NextWrite();
			};

			void OnDone() override 
			{
				MESSAGE("Uploading chunk metadata completed.");
				delete this;
			};

			void OnCancel() override
			{
				MESSAGE("Uploading chunk metadata cancelled.");
			}

		private:
			void NextWrite()
			{
				if (it_ == chunk_descriptor_list_->end()) {
					Finish(grpc::Status::OK); 
					return;
				}
				descriptor.set_handle(std::atoi(it_->name.c_str()));
				descriptor.set_chunk_size(it_->size);
				++it_;
				StartWrite(&descriptor);
				//Finish(grpc::Status::OK);
				return;
			}

			GFSChunkServer::ChunkMetadata descriptor;
			std::shared_ptr<std::vector<ChunkDescriptor>> chunk_descriptor_list_;
			std::vector<ChunkDescriptor>::const_iterator it_;
		};

		return new Writer(chunk_descriptor_list_);
	}

	grpc::ServerUnaryReactor* HeartBeat(
      grpc::CallbackServerContext* context,
			const GFSChunkServer::HeartBeatRequest* request,
			GFSChunkServer::HeartBeatResponse* response)
	override
	{ 
		MESSAGE("HEARTBEAT...");
		response->set_extend_lease(true);
		auto * reactor = context->DefaultReactor();
		reactor->Finish(grpc::Status::OK);
		return reactor;
	}

	grpc::ServerUnaryReactor* AssignPrimary(
      grpc::CallbackServerContext* context,
			const GFSChunkServer::AssignPrimaryRequest* request, 
			GFSChunkServer::AssignPrimaryResponse* response)
	override
	{
		std::atomic<int> counter = 0;
		// set lease in local in memory chunk structure
		MESSAGE("Setting local lease");
		// forward the lease to the secondaries
		MESSAGE("client ip: " << request->client_server().ip());
		MESSAGE("client port: " << request->client_server().port());

		for (int i=0; i<request->secondary_servers().size(); ++i) {
			MESSAGE("secondary ip: " << request->secondary_servers(i).ip());
			MESSAGE("secondary port: " << request->secondary_servers(i).port());
		}
		// responde with acknowledegement
		//
		response->set_acknowledgment(true);
		auto* reactor = context->DefaultReactor();
		reactor->Finish(grpc::Status::OK);
		return reactor;
	}

  grpc::ServerUnaryReactor* AssignSecondary(
      grpc::CallbackServerContext* context,
			const GFSChunkServer::AssignSecondaryRequest* request,
			GFSChunkServer::AssignSecondaryResponse* response) override 
	{ 
		MESSAGE("Setting local lease");

		MESSAGE("client ip: " << request->client_server().ip());
		MESSAGE("client port: " << request->client_server().port());

		response->set_acknowledgment(true);
		auto* reactor = context->DefaultReactor();
		reactor->Finish(grpc::Status::OK);
		return reactor;
	}

private:
	std::shared_ptr<std::vector<ChunkDescriptor>> chunk_descriptor_list_;
};


class ChunkServer {
public:
	ChunkServer(const chunkserver_master_connection_descriptor_t& _server_info):
		chunk_descriptor_list_{std::make_shared<std::vector<ChunkDescriptor>>()},
		chunkserver_master_connection_info_ {_server_info},
		chunk_server_service_{chunk_descriptor_list_},
		stub_{
			GFSMaster::ChunkServerService::NewStub(
				grpc::CreateChannel(grpc_connection_string<rpc_server_descriptor_t>(chunkserver_master_connection_info_.master),
				grpc::InsecureChannelCredentials()))}
	{
		load_chunks_from_filesystem();
		grpc::ServerBuilder builder;
		builder.AddListeningPort(grpc_connection_string(chunkserver_master_connection_info_.chunk_server), grpc::InsecureServerCredentials());
		builder.RegisterService(&chunk_server_service_);
		server_ = std::unique_ptr<grpc::Server>(builder.BuildAndStart());
	}

	void Announce()
	{
		grpc::ClientContext context;
		std::mutex mu;
		std::condition_variable cv;
		bool done = false;
		bool result;

		GFSMaster::RegisterChunkServerRequest request;
		request.mutable_server_info()->set_ip(chunkserver_master_connection_info_.chunk_server.ip);
		request.mutable_server_info()->set_rpc_port(chunkserver_master_connection_info_.chunk_server.rpc_port);

		GFSMaster::RegisterChunkServerResponse response;

		stub_->async()->RegisterChunkServer(
			&context, &request, &response,
			[&result, &mu, &cv, &done, &response] (grpc::Status status) {
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
		//std::cout << "ACK2: " << response.acknowledged() << "\n";
		std::unique_lock<std::mutex> lock(mu);
		cv.wait(lock, [&done] { return done; });
	}

	void start() {
		server_->Wait();
	}

private:
	void load_chunks_from_filesystem() {
		std::filesystem::path chunks_folder {"/home/nitro/.local/share/gfs/"};
		for (auto const& directory_entry: std::filesystem::directory_iterator(chunks_folder)) {
			std::string path = directory_entry.path();
			ChunkDescriptor desc {path.substr(path.rfind("/")+1, path.size() - path.rfind("/")), directory_entry.file_size()}; 
			chunk_descriptor_list_->push_back(desc);
		}
		chunk_descriptor_list_->shrink_to_fit();
	}

private:
	std::shared_ptr<std::vector<ChunkDescriptor>> chunk_descriptor_list_;
	/* ChunkServer-Master Connection Info */
	chunkserver_master_connection_descriptor_t chunkserver_master_connection_info_;
	/* Stub */
	std::unique_ptr<GFSMaster::ChunkServerService::Stub> stub_;
	/* Services */ 
	ChunkServerServiceImplementation chunk_server_service_;
	/* Server */
	std::unique_ptr<grpc::Server> server_;

	/* std::shared_ptr<std::vector<ServerController>> */
	/* 
	 * primary queues writes when it gets a write request from the client it checks the order
	 * the secondaries don't check the oerder
	 * What's the best data structure */
};

int main(int argc, char *argv[])
{

	if (argc < 11) {
		MESSAGE_END_EXIT("USAGE: ./server --ip <ipv4-address> --rpc-port <port> --tcp-port <port> --master-ip <ipv4-address> --master-port <port> ");
	} 

	chunkserver_master_connection_descriptor_t chunkserver_master_connection_info = parse_cli_args(argv+1, argc-1);
	std::cout << chunkserver_master_connection_info << "\n";

	ChunkServer main_server(chunkserver_master_connection_info);
	/* Announce to the master */
	main_server.Announce();
	main_server.start();
}
