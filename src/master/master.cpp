#include "master.h"
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/status.h>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <spdlog/logger.h>
#include <sstream>
#include <string>
#include <sys/types.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>
// #include <absl/strings/str_format.h>

#include "../headers/main.h"
#include "../lib/logger/logger.h"
#include "../server/generated/GFSChunkServer.grpc.pb.h"
#include "generated/GFSMasterService.grpc.pb.h"
#include "generated/GFSMasterService.pb.h"
#include "utils.h"
#include <cassert>
#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

// Reader Wrapper: Reads chunk handles, size, etc.. (metadata)
//

class ChunkServerController;

class Master {
public:
  friend class ChunkMetadataReader;

  class GFSMasterServiceImplementation
      : public GFSMaster::ChunkServerService::CallbackService {
  public:
    GFSMasterServiceImplementation(Master *_master);

    grpc::ServerUnaryReactor *RegisterChunkServer(
        grpc::CallbackServerContext *context,
        const GFSMaster::RegisterChunkServerRequest *request,
        GFSMaster::RegisterChunkServerResponse *response) override;

    grpc::ServerUnaryReactor *
    CreateFile(grpc::CallbackServerContext *context,
               const GFSMaster::CreateFileRequest *request,
               GFSMaster::CreateFileResponse *response) override;

    grpc::ServerUnaryReactor *
    Write(grpc::CallbackServerContext *context,
          const GFSMaster::WriteRequest *request,
          GFSMaster::WriteResponse *response) override;

  private:
    Master *master;
  };

  Master(const master_server_config_t &master_config);

  void listen();

  ~Master();

private:
  class chunk_server_priority_comparator {
  public:
    chunk_server_priority_comparator(
        std::map<int, ChunkServerController> *data_servers);
    bool operator()(int a, int b);

  private:
    std::map<int, ChunkServerController> *data_servers_;
  };

  static bool chunk_server_handle_prair_sort_by_ip(
      GFSNameSpace::chunk_server_handle_pair &a,
      GFSNameSpace::chunk_server_handle_pair &b);

  static void heartbeat_worker(int contoller_id, Master *master);

private:
  /*Chunk Servers Data*/
  /*TODO: Look into this*/
  std::shared_ptr<std::vector<ChunkServerController>>
      chunk_servers_controllers_;
  /*END TODO*/
  std::shared_ptr<std::vector<std::thread>> chunk_servers_heartbeat_threads_;

  rpc_server_descriptor_t server_info_;
  std::shared_ptr<grpc::Server> server_;

  /* Services */
  GFSMasterServiceImplementation master_service_;

  const unsigned long number_of_replicas;
  const unsigned long chunk_size;

  /*Name Space */
  std::unordered_map<std::string, int> file_name_to_integer_index;
  std::unordered_map<int, std::string> integer_index_to_file_name;

  std::map<int, GFSNameSpace::GFSFile> file_namespace_;
  std::map<int, ChunkServerController> data_servers_;

  /*TODO: */
  /*std::unordered_map<uint64_t, std::tuple<int, int, int>>
      chunk_to_in_file_position_;*/
  std::map<uint64_t, std::tuple<int, int, int>> chunk_to_in_file_position_;

  std::vector<int> chunk_server_ordering_;

  /*Sequence Counter*/
  std::atomic<int> file_id_counter_ = 0;
  std::atomic<int> chunk_server_id_counter_ = 0;
  std::atomic<int> write_id_counter_ = 0;
  std::atomic<uint64_t> handle_counter_ = 0;
};

void print_gfsfile(const GFSNameSpace::GFSFile &file) {

  std::ostream &out = std::cout;
  std::stringstream output;

  output << "Mode: "
         << (file.mode == GFSNameSpace::GFSFileType::NORMAL ? "Normal"
                                                            : "Atomic Append")
         << "\n";
  output << "File Size: " << file.size << "\n";

  int chunk_counter = 0;
  for (const GFSNameSpace::chunk_descriptor &chunk : file.chunks) {
    output << "###### Chunk index: " << chunk_counter << "######\n";
    output << "Chunk Size :" << chunk.size << "\n";

    output << "#### Chunk Replicas. ####\n";
    output << "Lease Host ID: " << chunk.lease.host_id << "\n";
    output << "Lease Issued At: " << chunk.lease.issued_at << "\n";

    int chunk_replica_index = 0;
    for (const GFSNameSpace::chunk_replica_descriptor &chunk_replica :
         chunk.replicas) {
      output << "## Chunk Replicas: " << chunk_replica_index << "##\n";
      output << "Replica Handle: " << chunk_replica.handle << "\n";
      output << "Host Id: " << chunk_replica.host_id << "\n";
      chunk_replica_index++;
    }
    chunk_counter++;
  }
  out << output.str() << "\n";
}

void print_chunk_to_file_map(
    const std::map<uint64_t, std::tuple<int, int, int>> &map) {
  for (auto it : map) {
    std::cout << it.first << " : file_index (" << std::get<0>(it.second)
              << "), chunk_index (" << std::get<1>(it.second)
              << "), chunk_replica_index (" << std::get<2>(it.second) << "\n";
  }
}

class ChunkServerController {
public:
  ChunkServerController(const tcp_rpc_server_descriptor_t &server_info);
  void ReadChunkMetadata(Master *master, int server_id);
  void HeartBeat();
  void AssignLease(const GFSNameSpace::write_coordinate &write_coordinates,
                   bool &acknowledged);
  tcp_rpc_server_descriptor_t server_info();

  double priority = 1.0;
  tcp_rpc_server_descriptor_t server_info_;

private:
  std::shared_ptr<GFSChunkServer::ChunkServerService::Stub> chunk_server_stub_;
};

class ChunkMetadataReader
    : public grpc::ClientReadReactor<GFSChunkServer::ChunkMetadata> {

public:
  ChunkMetadataReader(GFSChunkServer::ChunkServerService::Stub *stub,
                      Master *master, int host_id);
  void OnReadDone(bool OK) override;
  void OnDone(const grpc::Status &s) override;
  grpc::Status Await();

private:
  GFSChunkServer::UploadChunkMetadataRequest req_;
  grpc::ClientContext context_;
  GFSChunkServer::ChunkMetadata chunk_metadata_;
  std::mutex mu_;
  std::condition_variable cv_;
  grpc::Status status_;
  bool done_ = false;
  Master *master_;
  int host_id_;
};

ChunkMetadataReader::ChunkMetadataReader(
    GFSChunkServer::ChunkServerService::Stub *stub, Master *master, int host_id)
    : master_{master}, host_id_{host_id} {
  stub->async()->UploadChunkMetadata(&context_, &req_, this);
  StartRead(&chunk_metadata_);
  StartCall();
}

void ChunkMetadataReader::OnReadDone(bool OK) {
  if (OK) {
    /*
auto [file_id, chunk_index, replica_index] =
master_->chunk_to_in_file_position_.find(chunk_metadata_.handle())
->second;
    */
    int file_id;
    int chunk_index;
    int replica_index;

    std::tie(file_id, chunk_index, replica_index) =
        master_->chunk_to_in_file_position_.find(chunk_metadata_.handle())
            ->second;

    master_->file_namespace_[file_id]
        .chunks[chunk_index]
        .replicas[replica_index]
        .host_id = host_id_;

    StartRead(&chunk_metadata_);
  }
}

void ChunkMetadataReader::OnDone(const grpc::Status &s) {
  std::unique_lock<std::mutex> l(mu_);
  status_ = s;
  done_ = true;
  cv_.notify_one();
}

grpc::Status ChunkMetadataReader::Await() {
  std::unique_lock<std::mutex> l(mu_);
  cv_.wait(l, [this] { return done_; });
  return std::move(status_);
}

ChunkServerController::ChunkServerController(
    const tcp_rpc_server_descriptor_t &server_info)
    : server_info_{server_info},
      chunk_server_stub_{
          GFSChunkServer::ChunkServerService::NewStub(grpc::CreateChannel(
              grpc_connection_string<tcp_rpc_server_descriptor_t>(server_info_),
              grpc::InsecureChannelCredentials()))} {}

void ChunkServerController::ReadChunkMetadata(Master *master, int host_id) {
  ChunkMetadataReader reader =
      ChunkMetadataReader(chunk_server_stub_.get(), master, host_id);
  grpc::Status status = reader.Await();

  if (status.ok()) {
    MESSAGE("Finished reading handles");
  } else {
    MESSAGE("Problems reading");
  }
}

void ChunkServerController::HeartBeat() {
  grpc::ClientContext context;
  GFSChunkServer::HeartBeatRequest request;
  GFSChunkServer::HeartBeatResponse response;
  chunk_server_stub_.get()->HeartBeat(&context, request, &response);
}

void ChunkServerController::AssignLease(
    const GFSNameSpace::write_coordinate &write_coordinates,
    bool &acknowledged) {
  grpc::ClientContext context;
  GFSChunkServer::AssignPrimaryRequest request;
  GFSChunkServer::AssignPrimaryResponse response;

  request.set_write_id(write_coordinates.write_id);
  request.set_handle(write_coordinates.chunk_server_handle_list.front().handle);
  request.set_offset(write_coordinates.write_offset);

  request.mutable_client_server()->set_ip(
      write_coordinates.client_server_info.ip);
  request.mutable_client_server()->set_rpc_port(
      write_coordinates.client_server_info.rpc_port);

  assert(write_coordinates.chunk_server_handle_list.size() > 1 &&
         "there is only one server involved in a write");

  MAINLOG_INFO(
      "(12) primary forwards to: (ip: {}, tcp_port: {}, rpc_port: {})",
      write_coordinates.chunk_server_handle_list[1].server_info.ip,
      write_coordinates.chunk_server_handle_list[1].server_info.tcp_port,
      write_coordinates.chunk_server_handle_list[1].server_info.rpc_port);

  request.mutable_forward_to()->set_ip(
      write_coordinates.chunk_server_handle_list[1].server_info.ip);
  request.mutable_forward_to()->set_rpc_port(
      write_coordinates.chunk_server_handle_list[1].server_info.rpc_port);
  request.mutable_forward_to()->set_tcp_port(
      write_coordinates.chunk_server_handle_list[1].server_info.tcp_port);

  for (int i = 1; i < write_coordinates.chunk_server_handle_list.size(); ++i) {
    GFSChunkServer::SecondaryAndForwardServerInfo *secondary_and_forward_info =
        request.mutable_secondary_servers()->Add();

    const GFSNameSpace::chunk_server_handle_pair &secondary_server =
        write_coordinates.chunk_server_handle_list[i];

    secondary_and_forward_info->mutable_server_info()->set_ip(
        secondary_server.server_info.ip);
    secondary_and_forward_info->mutable_server_info()->set_tcp_port(
        secondary_server.server_info.tcp_port);
    secondary_and_forward_info->mutable_server_info()->set_rpc_port(
        secondary_server.server_info.rpc_port);
    secondary_and_forward_info->set_handle(secondary_server.handle);

    if (i + 1 < write_coordinates.chunk_server_handle_list.size()) {
      const GFSNameSpace::chunk_server_handle_pair &forward_server =
          write_coordinates.chunk_server_handle_list[i + 1];

      secondary_and_forward_info->mutable_forward_to()->set_ip(
          forward_server.server_info.ip);
      secondary_and_forward_info->mutable_forward_to()->set_tcp_port(
          forward_server.server_info.tcp_port);
      secondary_and_forward_info->mutable_forward_to()->set_rpc_port(
          forward_server.server_info.rpc_port);
      secondary_and_forward_info->set_forward(true);
    } else {
      secondary_and_forward_info->set_forward(false);
    }
  }
  /*
   * 1-primary
   * 1->2
   * 2->3
   * i<size-1
   * */
  MAINLOG_ERROR("(13) Assign primary call issued to primary chunk server.");
  chunk_server_stub_->AssignPrimary(&context, request, &response);
  acknowledged = response.body().acknowledgment();
}

tcp_rpc_server_descriptor_t ChunkServerController::server_info() {
  return server_info_;
}

/*PRIORIY*/

/* Thread that periodically sleeps and pings chunk servers for heartbeat
 * messages */
// void heartbeat_handler(GFSChunkServer::ChunkServerService::Stub * stub) {

/* Service: Master RPC server to register chunk servers */

/*
 * GRPC Service Class
 * */

Master::GFSMasterServiceImplementation::GFSMasterServiceImplementation(
    Master *_master)
    : master{_master} {}

grpc::ServerUnaryReactor *
Master::GFSMasterServiceImplementation::RegisterChunkServer(
    grpc::CallbackServerContext *context,
    const GFSMaster::RegisterChunkServerRequest *request,
    GFSMaster::RegisterChunkServerResponse *response) {

  MAINLOG_INFO("CHUNK SERVER REGISTERED");

  tcp_rpc_server_descriptor_t chunk_server_info;
  chunk_server_info.ip = request->server_info().ip();
  chunk_server_info.tcp_port = request->server_info().tcp_port();
  chunk_server_info.rpc_port = request->server_info().rpc_port();

  /*
   * Issue a new id for a new controller.
   * */
  int controller_id = master->chunk_server_id_counter_;
  MAINLOG_WARN(controller_id);
  ++master->chunk_server_id_counter_;
  MAINLOG_WARN(controller_id);

  master->data_servers_.insert(
      std::make_pair(controller_id, ChunkServerController(chunk_server_info)));
  master->chunk_server_ordering_.push_back(controller_id);
  master->chunk_servers_heartbeat_threads_->emplace_back(
      std::thread(heartbeat_worker, controller_id, master));

  GFSMaster::RegisterChunkServerResponse res;
  res.set_acknowledged(true);

  response->CopyFrom(res);
  auto *reactor = context->DefaultReactor();
  reactor->Finish(grpc::Status::OK);
  return reactor;
}

/*CreateFile API call*/
grpc::ServerUnaryReactor *Master::GFSMasterServiceImplementation::CreateFile(
    grpc::CallbackServerContext *context,
    const GFSMaster::CreateFileRequest *request,
    GFSMaster::CreateFileResponse *response) {

  /*TODO: Write to the permanent log.*/

  bool valid_file_description = false;

  GFSNameSpace::GFSFileType file_type;
  std::string file_path(request->path());

  switch (request->file_type()) {
  case GFSMaster::FileType::NORAML:
    file_type = GFSNameSpace::GFSFileType::NORMAL;
    break;
  case GFSMaster::FileType::ATOMIC_APPEND:
    file_type = GFSNameSpace::GFSFileType::ATOMIC_APPEND;
    break;
  default:
    response->set_status(GFSMaster::ERROR);
    response->mutable_error()->set_error_code(2000);
    response->mutable_error()->set_error_message("hello");
    break;
  }

  if (valid_file_description == true) {
    if (auto find_result = master->file_name_to_integer_index.find(file_path);
        find_result == master->file_name_to_integer_index.end()) {
      /*File Doesn't exist*/
      int file_id = (master->file_id_counter_)++;
      master->file_name_to_integer_index.insert({file_path, file_id});
      master->integer_index_to_file_name.insert({file_id, file_path});

      /*TODO: Refactor this to a constructor.*/
      GFSNameSpace::GFSFile new_file;
      new_file.mode = file_type;
      new_file.size = 0;
      new_file.chunk_size = 0 /*get this from settings*/;

      master->file_namespace_.insert({file_id, new_file});
      response->set_status(GFSMaster::Status::SUCCESS);
      response->mutable_response()->set_exists(false);
    } else {
      /*File Already exists*/
      response->set_status(GFSMaster::Status::SUCCESS);
      response->mutable_response()->set_exists(true);
    }
  }

  auto *reactor = context->DefaultReactor();
  reactor->Finish(grpc::Status::OK);
  return reactor;
}

/*Write API call*/
grpc::ServerUnaryReactor *Master::GFSMasterServiceImplementation::Write(
    grpc::CallbackServerContext *context,
    const GFSMaster::WriteRequest *request,
    GFSMaster::WriteResponse *response) {

  MAINLOG_INFO("(2) Write API call invoked");
  GFSNameSpace::write_coordinate write_coordinates;

  write_coordinates.setClient(&request->client_server_info());
  MAINLOG_INFO("(3) Setting client coordinates: client ip: {}, client tcp "
               "port: {}, client rpc port {}",
               write_coordinates.client_server_info.ip,
               write_coordinates.client_server_info.tcp_port,
               write_coordinates.client_server_info.rpc_port);

  /*Check if we have sufficient servers*/
  std::size_t write_replica_count = std::min(
      master->chunk_server_ordering_.size(), master->number_of_replicas);
  MAINLOG_INFO(
      "(4) nummber of connected chunk servers: {}, write replica count: {}",
      master->chunk_server_ordering_.size(), write_replica_count);

  if (master->chunk_server_ordering_.size() < master->number_of_replicas) {
    /*
     * - Could optimistically move on and write on a smaller number of
     * servers and at somepoint later this would be considered lost chunks
     * and re-replicated where there arenumber-of-replicas more servers. or
     * we can just respond with an error there aren't enough servers.
     *
     * */
  }

  bool found = false;
  bool acknowledged = false;

  std::string file_path{request->file_path()};
  int file_id;

  /*
   * Obtaining the file ID from the file path.
   * */

  MAINLOG_INFO("(5) Retreaving file structure from namespace ");
  if (auto find_result = master->file_name_to_integer_index.find(file_path);
      find_result != master->file_name_to_integer_index.end()) {
    found = true;
    file_id = find_result->second;
    /*TODO: pretty print GFSFile*/
  } else {
    MAINLOG_ERROR("(6) FILE NOT FOUND!!");
  }

  if (found) {
    GFSNameSpace::GFSFile &file = master->file_namespace_.find(file_id)->second;

    int chunk_index;
    write_coordinates.write_offset = request->offset() % file.chunk_size;

    MAINLOG_INFO("(6) GFSFile retrieved! file id: {}, file size: {}", file_id,
                 file.size);

    /*FIle is empty*/
    if (file.size == 0) {
      chunk_index = 0;
      /*Get new Coordinate ID*/
      write_coordinates.write_id = master->write_id_counter_++;
      MAINLOG_INFO("(7) Generating write id! write id {}",
                   write_coordinates.write_id);

      /*Heapifty to select the best chunk servers*/
      std::make_heap(master->chunk_server_ordering_.begin(),
                     master->chunk_server_ordering_.end(),
                     chunk_server_priority_comparator(&master->data_servers_));

      MAINLOG_INFO("(8) [Since new chunks are being written we choose where to "
                   "assign them using priority queue] Constructing priority "
                   "queue! selection pool size: {}",
                   master->chunk_server_ordering_.size());

      /* Put the selected servers in the write coordinates,
       * The first server is the primary*/
      for (int i = 0; i < write_replica_count; ++i) {
        write_coordinates.chunk_server_handle_list.push_back(
            {master->handle_counter_++,
             master->data_servers_.find(master->chunk_server_ordering_[i])
                 ->second.server_info_});
      }

      MAINLOG_INFO("(9) number of selected servers: {}",
                   write_coordinates.chunk_server_handle_list.size());

      assert(write_coordinates.chunk_server_handle_list.size() == 2 &&
             "write coordinates has 2 servers primary and one secondary");

      /*
      for (int i=0; i<write_replica_count; ++i) {
              std::cout << "################################### i= "<< i << "
      ##########################\n"; std::cout <<
      write_coordinates.chunk_server_handle_list[i].server_info.ip << "\n";;
              std::cout <<
      write_coordinates.chunk_server_handle_list[i].server_info.rpc_port <<
      "\n";;
      }
      */
      tcp_rpc_server_descriptor_t selected_primary =
          master->data_servers_.find(master->chunk_server_ordering_[0])
              ->second.server_info();

      MAINLOG_INFO(
          "(10) Primary server selected! ip: {}, tcp_port: {}, rpc_port: {}",
          selected_primary.ip, selected_primary.tcp_port,
          selected_primary.rpc_port);

      MESSAGE("(11) Lease Assignment process started!");
      /* Assign Lease to the primary*/
      master->data_servers_.find(master->chunk_server_ordering_[0])
          ->second.AssignLease(write_coordinates, acknowledged);

      MAINLOG_INFO("(27) Lease Assigned (ack: {})",
                   (acknowledged ? "TRUE" : "FALSE"));
    } else {
      chunk_index = file.size / file.chunk_size;
      /*we have the chunk index*/
      if (chunk_index > file.chunks.size()) {
        /* Write that skips some chunks
         * could throw an error or just issue the writes to write the chunks
         * in between.
         * */
      } else {
        /*
         * Write is within
         * */

        std::unordered_map<std::string, int> host_ip_id_map;

        /*
         * Get new Write ID
         * */
        write_coordinates.write_id = master->write_id_counter_++;

        for (const GFSNameSpace::chunk_replica_descriptor &replica_descriptor :
             file.chunks[chunk_index].replicas) {
          write_coordinates.chunk_server_handle_list.push_back(
              {replica_descriptor.handle,
               master->data_servers_.find(replica_descriptor.host_id)
                   ->second.server_info_});

          host_ip_id_map.insert(
              {master->data_servers_.find(replica_descriptor.host_id)
                   ->second.server_info_.ip,
               replica_descriptor.host_id});
        }
        std::sort(write_coordinates.chunk_server_handle_list.begin(),
                  write_coordinates.chunk_server_handle_list.begin(),
                  chunk_server_handle_prair_sort_by_ip);

        master->data_servers_
            .find(host_ip_id_map[write_coordinates.chunk_server_handle_list
                                     .begin()
                                     ->server_info.ip])
            ->second.AssignLease(write_coordinates, acknowledged);
      }
    }
  }

  if (acknowledged) {
    MAINLOG_INFO(
        "(27) Primary assigned, responding to client with server credentials");

    response->mutable_response_body()->set_write_id(write_coordinates.write_id);
    tcp_rpc_server_descriptor_t &primary =
        write_coordinates.chunk_server_handle_list.front().server_info;

    response->set_status(::GFSMaster::Status::SUCCESS);
    response->mutable_response_body()->mutable_primary_server()->set_ip(
        primary.ip);
    response->mutable_response_body()->mutable_primary_server()->set_tcp_port(
        primary.tcp_port);
    response->mutable_response_body()->mutable_primary_server()->set_rpc_port(
        primary.rpc_port);

    MAINLOG_INFO(
        "Write id: {}, primary server: [ip: {}, tcp port: {}, rpc port: {}]",
        write_coordinates.write_id, primary.ip, primary.tcp_port,
        primary.rpc_port);

    for (int i = 1; i < write_coordinates.chunk_server_handle_list.size();
         ++i) {
      auto response_secondary_server =
          response->mutable_response_body()->mutable_secondary_servers()->Add();
      tcp_rpc_server_descriptor_t &secondary_server =
          write_coordinates.chunk_server_handle_list[i].server_info;
      response_secondary_server->set_ip(secondary_server.ip);
      response_secondary_server->set_tcp_port(secondary_server.tcp_port);
      response_secondary_server->set_rpc_port(secondary_server.rpc_port);
      MAINLOG_INFO("Secondary server: [ip: {}, tcp port: {}, rpc port {}]",
                   secondary_server.ip, secondary_server.tcp_port,
                   secondary_server.rpc_port);
    }
  } else {
    MAINLOG_ERROR("(27) Primary not assigned, responding to client with error");
    response->set_status(::GFSMaster::Status::ERROR);
    response->mutable_error()->set_error_code(2000);
    response->mutable_error()->set_error_message("Lease assignment failed!");
  }

  auto *reactor = context->DefaultReactor();
  reactor->Finish(grpc::Status::OK);
  return reactor;

  /*
   * 1- Find the file in the namespace
   * 2- compute the chunk and the offset
   * 3- either get new servers or get the servers hosting the chunk
   * 4- organize forward sequence
   * 5- assign primary and secondary (write id, chunk_handle, offset)
   * 6- write ids??
   * */
  // Run the algorithm to select chunk servers and assign the primary
  // passing to it the secondaries.
}

Master::Master(const master_server_config_t &master_config)
    : server_info_(master_config.server_info),
      number_of_replicas{master_config.number_of_replicas},
      chunk_size{master_config.chunk_size},
      chunk_servers_controllers_{
          std::make_shared<std::vector<ChunkServerController>>()},
      chunk_servers_heartbeat_threads_{
          std::make_shared<std::vector<std::thread>>()},
      master_service_{this} {
  chunk_servers_controllers_->reserve(100);
  grpc::ServerBuilder builder_;
  builder_.AddListeningPort(grpc_connection_string(master_config.server_info),
                            grpc::InsecureServerCredentials());
  // master_service_ = GFSMasterServer(chunk_servers_descriptors_,
  // chunk_servers_heartbeat_threads_);
  builder_.RegisterService(&master_service_);
  server_ = std::unique_ptr(builder_.BuildAndStart());

  /*NOTE: HARD CODED DELETE LATER*/
  GFSNameSpace::GFSFile file;
  file.mode = GFSNameSpace::GFSFileType::NORMAL;
  file.size = 0;
  file.chunk_size = 4096;
  GFSNameSpace::chunk_replica_descriptor chunk1_replica1{1111111, -1};
  GFSNameSpace::chunk_replica_descriptor chunk1_replica2{3333333, -1};
  GFSNameSpace::chunk_descriptor chunk1;
  chunk1.replicas.push_back(chunk1_replica1);
  chunk1.replicas.push_back(chunk1_replica2);
  GFSNameSpace::chunk_replica_descriptor chunk2_replica1{2222222, -1};
  GFSNameSpace::chunk_replica_descriptor chunk2_replica2{4444444, -1};
  GFSNameSpace::chunk_descriptor chunk2;
  chunk2.replicas.push_back(chunk2_replica1);
  chunk2.replicas.push_back(chunk2_replica2);
  file.chunks.push_back(chunk1);
  file.chunks.push_back(chunk2);

  file_name_to_integer_index.insert(std::make_pair("testfile", 1));
  integer_index_to_file_name.insert(std::make_pair(1, "testfile"));

  chunk_to_in_file_position_.insert(
      std::make_pair(1111111, std::make_tuple(1, 0, 0)));
  chunk_to_in_file_position_.insert(
      std::make_pair(3333333, std::make_tuple(1, 0, 1)));
  chunk_to_in_file_position_.insert(
      std::make_pair(2222222, std::make_tuple(1, 1, 0)));
  chunk_to_in_file_position_.insert(
      std::make_pair(4444444, std::make_tuple(1, 1, 1)));

  file_namespace_.insert({1, std::move(file)});
  /*END NOTE*/

  int file_index = file_name_to_integer_index.find("testfile")->second;

  std::cout << "testfile\n";
  print_gfsfile(file_namespace_.find(file_index)->second);
  print_chunk_to_file_map(chunk_to_in_file_position_);
}

void Master::listen() { server_->Wait(); }

Master::~Master() {
  for (std::thread &th : *chunk_servers_heartbeat_threads_) {
    th.join();
  }
}

Master::chunk_server_priority_comparator::chunk_server_priority_comparator(
    std::map<int, ChunkServerController> *data_servers)
    : data_servers_{data_servers} {}

bool Master::chunk_server_priority_comparator::operator()(int a, int b) {
  return (*(this->data_servers_)).find(a)->second.priority <
         (*(this->data_servers_)).find(b)->second.priority;
}

bool Master::chunk_server_handle_prair_sort_by_ip(
    GFSNameSpace::chunk_server_handle_pair &a,
    GFSNameSpace::chunk_server_handle_pair &b) {
  return a.server_info.ip < b.server_info.ip;
}

void Master::heartbeat_worker(int host_id, Master *master) {
  MAINLOG_INFO("Heartbeat thread started");
  ChunkServerController &controller =
      master->data_servers_.find(host_id)->second;
  sleep(2);
  controller.ReadChunkMetadata(master, host_id);
  print_gfsfile(master->file_namespace_.find(1)->second);
  int count = 100;
  while (--count > 0) {
    sleep(2);
    controller.HeartBeat();
    // MESSAGE("extend lease: " << (response.extend_lease() ? "true" :
    // "false"));
  }
}

int main(int argc, char *argv[]) {

  GFSLogger::Logger::init();

  if (argc < 7 or argc > 7) {
    MESSAGE_END_EXIT("USAGE: ./master --ip <ipv4-address> --rpc-port <port> "
                     "--number-of-replicas <number of replicas(integer)>");
  }

  master_server_config_t master_config;

  parse_cli_args(argv + 1, argc - 1, master_config);

  Master master(master_config);
  // MESSAGE(master_server_info);
  master.listen();
}
