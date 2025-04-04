#include <cstdlib>
#include <string>
#include <unordered_set>
#include <vector>

#include "../headers/main.h"

std::ostream& operator<<(std::ostream& out, const rpc_server_descriptor_t& server_info)
{
	out << "\nMASTER INFO: \n";
	out << "\tIP: " << server_info.ip << "\n";
	out << "\tRPC PORT: " << server_info.rpc_port << "\n";
	return out;
}

rpc_server_descriptor_t parse_cli_args(char * argv[], int size)
{
	/* TODO: 
	 * 1- Validate everything with regular expressions.
	 * Add better error handling for arguments.
	 * - Push arguments into a set and make sure that all arguments are correct
	 *   and there aren't duplicates
	 * 2- Refactor all flag parsing into one class.
	 * */

	rpc_server_descriptor_t master;

	MESSAGE("parsing cli arguments...");
	std::vector<std::string> token_stream(size);

	std::vector<std::string>::iterator it = token_stream.begin();

	for (int i=0; i<size; ++i) {
		*(it) = std::string(argv[i]);
		++it;
	}

	// std::unordered_set<std::string> set;

	for(std::vector<std::string>::const_iterator token_it = (token_stream.begin());
		token_it != token_stream.end();) 
	{
		if (*token_it == SERVER_IP_FLAG) {
			master.ip = *((++token_it)++); 			
		} else if (*token_it == SERVER_RPC_PORT_FLAG) {
			master.rpc_port = atoi(((++token_it)++)->c_str());
		} else {
			MESSAGE_END_EXIT(std::string("error parsing cli arguments | \nunknown token \"").append(*token_it).append("\""));
		}
	}

	MESSAGE("Finished parsing cli arguments.");

	return master;
}
