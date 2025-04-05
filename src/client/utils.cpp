#include <iostream>
#include "../headers/main.h"

std::ostream& operator<<(std::ostream& out, const client_master_connection_descriptor_t& server_info)
{
	out << "\nServer Info: \n";
	out << "\tIP: " << server_info.client.ip << "\n";
	out << "\tRPC PORT: " << server_info.client.rpc_port<< "\n";
	out << "\tTCP PORT: " << server_info.client.tcp_port<< "\n";

	out << "\nMaster Info: \n";
	out << "\tIP: " << server_info.master.ip << "\n";
	out << "\tRPC PORT: " << server_info.client.rpc_port << "\n";

	return out;
}
