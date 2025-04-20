#include <cstdlib>
#include <string>
#include <vector>

#include "../headers/main.h"

std::ostream& operator<<(std::ostream& out, const chunkserver_master_connection_descriptor_t& server_info) {

	out << "\nServer Info: \n";
	out << "\tIP: " << server_info.chunk_server.ip << "\n";
	out << "\tRPC PORT: " << server_info.chunk_server.rpc_port<< "\n";
	out << "\tTCP PORT: " << server_info.chunk_server.tcp_port<< "\n";

	out << "\nMaster Info: \n";
	out << "\tIP: " << server_info.master.ip << "\n";
	out << "\tRPC PORT: " << server_info.chunk_server.rpc_port << "\n";

	return out;
}

chunkserver_master_connection_descriptor_t  parse_cli_args(char * argv[], int size) {
	/* TODO: 
	 * Validate everything with regular expressions.
	 * Add better error handling for arguments.
	 * - Push arguments into a set and make sure that all arguments are correct
	 *   and there aren't duplicates
	 * */
	std::vector<std::string> token_stream(size);

	std::vector<std::string>::iterator it = token_stream.begin();

	for (int i=0; i<size; ++i) {
		*(it) = std::string(argv[i]);
		++it;
	}

	tcp_rpc_server_descriptor_t chunk_server;
	rpc_server_descriptor_t master;

	// std::unordered_set<std::string> set;

	for(std::vector<std::string>::const_iterator token_it = (token_stream.begin());
		token_it != token_stream.end();) 
	{
		if (*token_it == SERVER_IP_FLAG) {
			chunk_server.ip = *((++token_it)++); 			
		} else if (*token_it == SERVER_RPC_PORT_FLAG) {
			chunk_server.rpc_port = atoi(((++token_it)++)->c_str());
		} else if (*token_it == TCP_PORT_FLAG) {
			chunk_server.tcp_port= atoi(((++token_it)++)->c_str());
		} else if (*token_it == MASTER_IP_FLAG) {
			master.ip = *((++token_it)++); 			
		} else if (*token_it == MASTER_PORT_FLAG) {
			master.rpc_port= atoi(((++token_it)++)->c_str());
		} else {
			MESSAGE_END_EXIT(std::string("error parsing cli arguments:\nunknown token").append(*token_it));
		}
	}

	chunkserver_master_connection_descriptor_t server_info {chunk_server, master};
	return server_info;
}

