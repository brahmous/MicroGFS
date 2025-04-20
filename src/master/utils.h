#ifndef MASTER_UTILS_H
#define MASTER_UTILS_H
#include "../headers/main.h"

template <typename T>
std::string grpc_connection_string (const T& descriptor)
{
	std::string ip (descriptor.ip);
	return ip.append(":").append(std::to_string(descriptor.rpc_port));
}

void parse_cli_args(char * argv[], int size, master_server_config_t & config);

std::ostream& operator<<(std::ostream& out, const rpc_server_descriptor_t& server_info);

#endif


