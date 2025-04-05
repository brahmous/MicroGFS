#ifndef GFS_UTILS_H
#define GFS_UTILS_H
#include <string>
#include "../headers/main.h"

template <typename T>
std::string grpc_connection_string (const T& descriptor)
{
	std::string ip (descriptor.ip);
	return ip.append(":").append(std::to_string(descriptor.rpc_port));
}

std::ostream& operator<<(std::ostream& out, const client_master_connection_descriptor_t& server_info);

#endif
