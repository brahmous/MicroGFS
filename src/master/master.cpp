#include <absl/strings/str_format.h>
#include <algorithm>
#include <condition_variable>
#include <cstdint>
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
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "../headers/main.h"
#include "utils.h"
#include "generated/GFSMasterService.pb.h"
#include "generated/GFSMasterService.grpc.pb.h"
#include "../server/generated/GFSChunkServer.grpc.pb.h"

// Reader Wrapper: Reads chunk handles, size, etc.. (metadata)
class ChunkMetadataReader: public grpc::ClientReadReactor<GFSChunkServer::ChunkMetadata> {
public:

	// TODO: pass argument for btree and push into it.
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

class ChunkServerController {
public:
	ChunkServerController(const tcp_rpc_server_descriptor_t& server_info):
		server_info_{server_info},
		chunk_server_stub_{
			GFSChunkServer::ChunkServerService::NewStub(
				grpc::CreateChannel(
					grpc_connection_string<tcp_rpc_server_descriptor_t>(server_info_),
					grpc::InsecureChannelCredentials()
				)
			)
		}
	{}

	void ReadChunkMetadata()
	{
		ChunkMetadataReader reader = ChunkMetadataReader(chunk_server_stub_.get());
		grpc::Status status = reader.Await();

		if ( status.ok() ) {
			MESSAGE("Finished reading handles");
		} else {
			MESSAGE("Problems reading");
		}
	}

	void HeartBeat() 
	{
		MESSAGE("------------------ SENDING HEART BEAT ------------------");
		grpc::ClientContext context;
		GFSChunkServer::HeartBeatRequest request;
		GFSChunkServer::HeartBeatResponse response;
		MESSAGE("------------------ END SENDING HEART BEAT ------------------");
		chunk_server_stub_.get()->HeartBeat(&context, request, &response);
	}

	void AssignLease(const rpc_server_descriptor_t & client_info, 
									uint64_t handle,
									std::string write_id,
									const std::vector<tcp_rpc_server_descriptor_t>& secondary_servers_info,
									bool& acknowledged) {

		grpc::ClientContext context;
		GFSChunkServer::AssignPrimaryRequest request;

		request.set_handle(handle);
		request.set_write_id(write_id);

		request.mutable_client_server()->set_ip(client_info.ip);
		request.mutable_client_server()->set_rpc_port(client_info.rpc_port);
	
		for (const tcp_rpc_server_descriptor_t& secondary_info: secondary_servers_info) {
			GFSChunkServer::TcpRpcServerIdentifier * secondary_info_ = request.add_secondary_servers();
			secondary_info_->set_ip(secondary_info.ip);
			secondary_info_->set_tcp_port(secondary_info.tcp_port);
			secondary_info_->set_rpc_port(secondary_info.rpc_port);
		}

		GFSChunkServer::AssignPrimaryResponse response; 
		// Async RPC CALL
		chunk_server_stub_->AssignPrimary(&context, request, &response);
		acknowledged = response.acknowledgment();
	}

	tcp_rpc_server_descriptor_t server_info() {
		return server_info_;
	}

private:
	tcp_rpc_server_descriptor_t server_info_;
	std::shared_ptr<GFSChunkServer::ChunkServerService::Stub> chunk_server_stub_;
};

/* Thread that periodically sleeps and pings chunk servers for heartbeat messages */
//void heartbeat_handler(GFSChunkServer::ChunkServerService::Stub * stub) {
void heartbeat_worker(ChunkServerController * controller) {
	//MESSAGE("controller POINTER: " << controller);
	MESSAGE("hHHHHHHHHHHELLLOOJklll");
	sleep(2);
	controller->ReadChunkMetadata();
	MESSAGE("[------------------]: POSITION :[------------------]");
	int count = 100;
	while (--count > 0) {
		MESSAGE("[LOG]: Sleeping for 2 seconds.");
		sleep(2);
		MESSAGE("[LOG]: HEARTBEAT...");
		controller->HeartBeat();
		//MESSAGE("extend lease: " << (response.extend_lease() ? "true" : "false"));
	}
}

/* Service: Master RPC server to register chunk servers */
class GFSMasterServerServiceImplementation : public GFSMaster::ChunkServerService::CallbackService {
public:
	GFSMasterServerServiceImplementation (
		std::shared_ptr<std::vector<std::thread>> chunk_servers_heartbeat_threads,
		std::shared_ptr<std::vector<ChunkServerController>> chunk_servers_controllers,
		const rpc_server_descriptor_t& server_info
	): 
		chunk_servers_heartbeat_threads_sp_{chunk_servers_heartbeat_threads},
		chunk_server_controllers_{chunk_servers_controllers},
		server_info_ {server_info}
	{}

	grpc::ServerUnaryReactor* RegisterChunkServer (
					grpc::CallbackServerContext* context,
					const GFSMaster::RegisterChunkServerRequest* request,
					GFSMaster::RegisterChunkServerResponse* response
	) override 
	{
		tcp_rpc_server_descriptor_t chunk_server_info;
		chunk_server_info.ip = request->server_info().ip();
		chunk_server_info.rpc_port = request->server_info().rpc_port();
		chunk_server_info.tcp_port = request->server_info().tcp_port();

		MESSAGE("[LOG]: Chunk server requests registration. <<<<");
		MESSAGE("[->]: Server Info: ");
		MESSAGE("[->]: ip: " << chunk_server_info.ip);
		MESSAGE("[->]: rpc port: " << chunk_server_info.rpc_port);
		MESSAGE("[->]: tcp port: " << chunk_server_info.tcp_port);

		MESSAGE("[LOG]: making controller.");
		ChunkServerController& controller = chunk_server_controllers_->emplace_back(chunk_server_info);
		MESSAGE("[LOG]: Spawning Heartbeat Thread.");
		chunk_servers_heartbeat_threads_sp_->emplace_back(std::thread(heartbeat_worker, std::addressof(controller))); 
		MESSAGE("[LOG]: Heartbeat Thread Spawned. ");
		MESSAGE("[LOG]: THREAD POOL SIZE: " << chunk_servers_heartbeat_threads_sp_->size());

		GFSMaster::RegisterChunkServerResponse res;
		res.set_acknowledged(true);
		MESSAGE("[LOG]: Registration Acknowledged.");

		response->CopyFrom(res);
		auto* reactor = context->DefaultReactor();
		reactor->Finish(grpc::Status::OK);
		return reactor;
	}

  grpc::ServerUnaryReactor* Write(
      grpc::CallbackServerContext* context,
			const GFSMaster::WriteRequest* request,
			GFSMaster::WriteResponse* response
	) override
	{ 
		// Run the algorithm to select chunk servers and assign the primary passing to it the secondaries.
		MESSAGE("[LOG]: Write request received.");
		MESSAGE("[LOG]: Write received info: ");
		MESSAGE("[->]: File path: " << request->file_path());
		MESSAGE("[->]: Data size: " << request->data_size());
		MESSAGE("[->]: Offset: " << request->offset());

		bool acknowledged = false;
		uint64_t handle = 123456789;
		std::string write_id = "5s4f5s4fef4s5f45sd4f6er24s";
		
		MESSAGE("[LOG]: Write request corresponding info: ");
		MESSAGE("[->]: chunk handle: " << handle);
		MESSAGE("[->]: write id: " << write_id);

		std::vector<tcp_rpc_server_descriptor_t> secondary_servers;

		MESSAGE("[LOG]: Write-secondary server list:");

		for (std::vector<ChunkServerController>::iterator 
			it = chunk_server_controllers_->begin()+1;
			it < chunk_server_controllers_->end();
			++it) {
			secondary_servers.push_back(it->server_info());
		}

		int _log_count_ = 1; /*for logging only.*/
		for(tcp_rpc_server_descriptor_t selected_secondary_info: secondary_servers) {
			MESSAGE("[LOG]: Secondary Server: " << _log_count_);
			MESSAGE("[->]: ip: " << selected_secondary_info.ip);
			MESSAGE("[->]: tcp port: " << selected_secondary_info. tcp_port);
			MESSAGE("[->]: rpc port: " << selected_secondary_info.rpc_port);
		} 

		MESSAGE("[LOG]: Lease Assignment Process Started.");
		chunk_server_controllers_
			->front()
			.AssignLease(server_info_, handle, write_id, secondary_servers, acknowledged);
		MESSAGE("[LOG]: Lease Assignment Process ended.");

		MESSAGE("[LOG]: ACKNOWLEDGED (" << (acknowledged ? "true" : "false") << ")" );

		// TODO: fix this - run an algorithm to select primary and secondaries and send them back to the client.
		tcp_rpc_server_descriptor_t primary_info = chunk_server_controllers_->front().server_info();

		// Write converters to convert structs to rpc types
		response->mutable_primary_server()->set_ip(primary_info.ip);
		// response->mutable_primary_server()->set_ip("HEEEEEEEEEEEEEEEEELOOOOOOOOOOOOOOOOOOOOO THIS IS MY IPPPPPPPPPPPPPPP");
		response->mutable_primary_server()->set_tcp_port(primary_info.tcp_port);
		response->mutable_primary_server()->set_rpc_port(primary_info.rpc_port);

		for (const tcp_rpc_server_descriptor_t& server_info: secondary_servers) {
			GFSMaster::ChunkServerIdentifier * server_info_ = response->add_secondary_servers();
			server_info_->set_ip(server_info.ip);
			server_info_->set_tcp_port(server_info.tcp_port);
			server_info_->set_rpc_port(server_info.rpc_port);
		}

		auto* reactor = context->DefaultReactor();
		reactor->Finish(grpc::Status::OK);
		MESSAGE("[LOG]: Response (chunk location) is sent back to the client.");
		return reactor;
	}

	private:
	std::shared_ptr<std::vector<ChunkServerController>> chunk_server_controllers_;
	std::shared_ptr<std::vector<std::thread>> chunk_servers_heartbeat_threads_sp_;
	rpc_server_descriptor_t server_info_;
};

class MasterServer {
public:
	MasterServer(const rpc_server_descriptor_t & server_info): 
		server_info_(server_info),
		chunk_servers_controllers_{std::make_shared<std::vector<ChunkServerController>>()},
		chunk_servers_heartbeat_threads_{std::make_shared<std::vector<std::thread>>()},
		master_service_ {
			chunk_servers_heartbeat_threads_, 
			chunk_servers_controllers_,
			server_info_
		} 
	{
		chunk_servers_controllers_->reserve(100);
		grpc::ServerBuilder builder_;
		builder_.AddListeningPort(grpc_connection_string(server_info), grpc::InsecureServerCredentials());
		//master_service_ = GFSMasterServer(chunk_servers_descriptors_, chunk_servers_heartbeat_threads_);
		builder_.RegisterService(&master_service_);
		server_ = std::unique_ptr(builder_.BuildAndStart());
		MESSAGE("[LOG]: Master server object constructed.");
	}

	void listen()
	{
		server_->Wait();
	} 

	~MasterServer()
	{
		for(std::thread & th: *chunk_servers_heartbeat_threads_) {
			MESSAGE("[LOG]: Joining Heartbeat threads.");
			th.join();
		}
	}

private:
	/*Chunk Servers Data*/
	std::shared_ptr<std::vector<ChunkServerController>> chunk_servers_controllers_; 
	std::shared_ptr<std::vector<std::thread>> chunk_servers_heartbeat_threads_;

	/*Service offered by the master to the chunk server*/
	rpc_server_descriptor_t server_info_;
	std::shared_ptr<grpc::Server> server_;

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

	MESSAGE("[LOG]: Parsing CLI args.");
	rpc_server_descriptor_t master_server_info = parse_cli_args(argv+1, argc-1);   
	MESSAGE("[LOG]: MASTER INFO: ");
	MESSAGE("[->]: Master ip: " << master_server_info.ip);
	MESSAGE("[->]: Master rpc-port: " << master_server_info.rpc_port);


	MESSAGE("[LOG]: GFSMaster Service Construction Start: ");
	MasterServer master(master_server_info);
	MESSAGE("[LOG]: GFSMaster Service Construction end: ");
	//MESSAGE(master_server_info);
	MESSAGE("[LOG]: Master server waiting start: ");
	master.listen();
}

