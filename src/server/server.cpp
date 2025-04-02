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
#include <vector>
#include "../main.h"
#include "./headers/cli-args-parser.h"
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
	{
	}

	grpc::ServerWriteReactor< GFSChunkServer::ChunkMetadata>* 
	UploadChunkMetadata(grpc::CallbackServerContext* context,
										 	const GFSChunkServer::UploadChunkMetadataRequest* request) override
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

private:
	std::shared_ptr<std::vector<ChunkDescriptor>> chunk_descriptor_list_;
};

class ChunkServer {
public:
	ChunkServer(const ServerInfo& _server_info):
		chunks_{std::make_shared<std::vector<ChunkDescriptor>>()},
		server_info {_server_info},
		service_{chunks_},
		stub_{
			GFSMaster::ChunkServerService::NewStub(
				grpc::CreateChannel(server_info.master_ip_port_string(),
				grpc::InsecureChannelCredentials()))}
	{
		load_chunks_from_filesystem();
		grpc::ServerBuilder builder;
		builder.AddListeningPort(server_info.my_ip_port_string(), grpc::InsecureServerCredentials());
		builder.RegisterService(&service_);
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
		request.mutable_server_info()->set_ip(server_info.my_ip());
		request.mutable_server_info()->set_port(server_info.my_rpc_port());

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
			chunks_->push_back(desc);
		}
		chunks_->shrink_to_fit();
	}

private:
	std::shared_ptr<std::vector<ChunkDescriptor>> chunks_;
	/* Server Info */
	ServerInfo server_info;
	/* Stub */
	std::unique_ptr<GFSMaster::ChunkServerService::Stub> stub_;
	/* Services */ 
	ChunkServerServiceImplementation service_;
	/* Server */
	std::unique_ptr<grpc::Server> server_;
};

int main(int argc, char *argv[])
{

	if (argc < 11) {
		MESSAGE_END_EXIT("USAGE: ");
	} 

	ServerInfo server_info(argv+1, argc-1);
	std::cout << server_info << "\n";

	ChunkServer server(server_info);

	server.Announce();
	server.start();
}
