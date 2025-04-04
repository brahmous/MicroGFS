#ifndef MASTER_UTILS_H
#define MASTER_UTILS_H
#include "../headers/main.h"

template <typename T>
std::string grpc_connection_string (const T& descriptor)
{
	std::string ip (descriptor.ip);
	return ip.append(":").append(std::to_string(descriptor.rpc_port));
}

std::ostream& operator<<(std::ostream& out, const rpc_server_descriptor_t& server_info);
rpc_server_descriptor_t parse_cli_args(char * argv[], int size);

#endif


