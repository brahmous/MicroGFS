#include <cstdlib>
#include <string>
#include <unordered_set>
#include <vector>

#include "../main.h"
#include "headers/cli-args-parser.h"

std::ostream& operator<<(std::ostream& out, const ServerInfo& server_info) {

	out << "\nMaster Info: \n";
	out << "\tIP: " << server_info.my_ip() << "\n";
	out << "\tRPC PORT: " << server_info.my_rpc_port() << "\n";

	return out;
}

void ServerInfo::parse_cli_args(const std::vector<std::string>& token_stream) {
	/* TODO: 
	 * 1- Validate everything with regular expressions.
	 * Add better error handling for arguments.
	 * - Push arguments into a set and make sure that all arguments are correct
	 *   and there aren't duplicates
	 * 2- Refactor all flag parsing into one class.
	 * */

	MESSAGE("parsing cli arguments...");

	std::unordered_set<std::string> set;

	for(std::vector<std::string>::const_iterator token_it = (token_stream.begin());
		token_it != token_stream.end();) 
	{
		if (*token_it == SERVER_IP_FLAG) {
			this->server.ip = *((++token_it)++); 			
		} else if (*token_it == SERVER_RPC_PORT_FLAG) {
			this->server.rpc_port = atoi(((++token_it)++)->c_str());
		} else {
			MESSAGE_END_EXIT(std::string("error parsing cli arguments | \nunknown token \"").append(*token_it).append("\""));
		}
	}

	MESSAGE("Finished parsing cli arguments.");
}
