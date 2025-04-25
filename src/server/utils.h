#ifndef SERVER_UTILS_H
#define SERVER_UTILS_H
#include "../headers/main.h"

template <typename T>
std::string grpc_connection_string (const T& descriptor)
{
	std::string ip (descriptor.ip);
	return ip.append(":").append(std::to_string(descriptor.rpc_port));
}

std::ostream& operator<<(std::ostream& out, const chunkserver_master_connection_descriptor_t& server_info);

chunk_server_config_t parse_cli_args(char * argv[], int size);

#endif
