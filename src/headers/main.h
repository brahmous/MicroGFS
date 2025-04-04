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

typedef struct tcp_rpc_server_descriptor_t {
	std::string ip;
	unsigned short int rpc_port;	
	unsigned short int tcp_port;
} tcp_rpc_server_descriptor_t;

typedef struct  rpc_server_descriptor_t {
	std::string ip;
	unsigned short int rpc_port;
} server_address_t;

typedef struct chunkserver_master_connection_descriptor_t {
	tcp_rpc_server_descriptor_t chunk_server;
	rpc_server_descriptor_t master;
} chunkserver_master_connection_descriptor_t;


std::ostream& operator<<(std::ostream& out, const server_address_t& server_info); 

#endif
