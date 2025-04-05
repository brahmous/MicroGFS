#include <grpcpp/client_context.h>
#include <iostream>
#include "../headers/main.h"
#include "client.h"

void MicroGFS::write(std::string file_path, unsigned int offset) {

	grpc::ClientContext context;
	GFSMaster::WriteRequest request;
	GFSMaster::WriteResponse response;

	master_stub_->Write(&context,request, &response);

	MESSAGE("PRIMARY: ");
	MESSAGE("ip: " << response.primary_server().ip());
	MESSAGE("port: " << response.primary_server().rpc_port());

	for (int i=0; i<response.secondary_servers_size(); ++i) {
		MESSAGE("SECONDARY " << i);
		MESSAGE("ip: " << response.secondary_servers(i).ip());
		MESSAGE("port: " << response.secondary_servers(i).rpc_port());
	}
}

int main(int argc, char *argv[])
{

	client_master_connection_descriptor_t connection_options;
	connection_options.master.ip = "0.0.0.0";
	connection_options.master.rpc_port = 5000;
	connection_options.client.ip = "0.0.0.0";
	connection_options.client.rpc_port = 3000;
	connection_options.client.tcp_port = 2000;

	MicroGFS client (connection_options);

	client.write("/hello", 20);

	std::cout << "Hello, World\n";
}
