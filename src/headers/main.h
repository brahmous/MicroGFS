#ifndef MAIN_H
#define MAIN_H

#include <iostream>

#define MESSAGE_END_EXIT(message) std::cout << message << "\n"; exit(1)
#define MESSAGE(message) std::cout << message << "\n"

#define SERVER_IP_FLAG 				"--ip"
#define SERVER_RPC_PORT_FLAG  "--rpc-port"
#define TCP_PORT_FLAG 				"--tcp-port"
#define MASTER_IP_FLAG 				"--master-ip"
#define MASTER_PORT_FLAG 			"--master-port"
#define NUMBER_OF_REPLICAS    "--number-of-replicas"
#define CHUNK_SIZE 						"--chunk-size"

/*
 * TODO: too much duplication. refactor!
 * */

typedef struct tcp_rpc_server_descriptor_t {
	std::string ip;
	unsigned short int rpc_port;	
	unsigned short int tcp_port;
} tcp_rpc_server_descriptor_t;

typedef struct  rpc_server_descriptor_t {
	std::string ip;
	unsigned short int rpc_port;
} rpc_server_descriptor_t;

typedef struct chunkserver_master_connection_descriptor_t {
	tcp_rpc_server_descriptor_t chunk_server;
	rpc_server_descriptor_t master;
} chunkserver_master_connection_descriptor_t;

typedef struct client_master_connection_descriptor_t {
	tcp_rpc_server_descriptor_t client;
	rpc_server_descriptor_t master;
} client_master_connection_descriptor_t;

typedef struct master_server_config_t {
	rpc_server_descriptor_t server_info;
	unsigned int number_of_replicas = 0;
	unsigned int chunk_size = 0;
} master_server_config_t;

std::ostream& operator<<(std::ostream& out, const rpc_server_descriptor_t& server_info); 

#endif
