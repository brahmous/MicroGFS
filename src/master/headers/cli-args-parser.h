#ifndef CLI_ARGS_PARSER_H
#define CLI_ARGS_PARSER_H
#include <ostream>
#include <string>
#include <vector>
#include "../../main.h"

#define SERVER_IP_FLAG 				"--ip"
#define SERVER_RPC_PORT_FLAG  "--rpc-port"


// TODO: Cleanup.

typedef struct master_address_t {
	std::string ip;
	unsigned int rpc_port;
} master_address_t;

class ServerInfo {
public:

	ServerInfo (char * argv[], int size)
	{
		std::vector<std::string> token_stream(size);

		std::vector<std::string>::iterator token_it = token_stream.begin();

		for (int i=0; i<size; ++i) {
			*token_it = std::string(argv[i]);
			++token_it;
		}

		this->parse_cli_args(token_stream);
	}

	ServerInfo(std::string ip, unsigned int rpc_port): server{ip, rpc_port}
	{

		MESSAGE("server info constructor");

		MESSAGE( "ip: " << ip);
		MESSAGE( "port: " << rpc_port);
	}

	const std::string& my_ip() const
	{
		return this->server.ip;
	} 

	const unsigned int& my_rpc_port() const
	{
		return this->server.rpc_port;
	}

	const std::string my_ip_port_string() const {
		std::string str(my_ip());
		return str.append(":").append(std::to_string(my_rpc_port()));
	}

	friend std::ostream& operator<<(std::ostream& out, const ServerInfo& server_info); 
	
private:
	master_address_t server;
	void parse_cli_args(const std::vector<std::string>& token_stream);
};

#endif
