#ifndef CLI_ARGS_PARSER_H
#define CLI_ARGS_PARSER_H
#include <ostream>
#include <string>
#include <vector>

#define SERVER_IP_FLAG 				"--ip"
#define SERVER_RPC_PORT_FLAG  "--rpc-port"
#define TCP_PORT_FLAG 				"--tcp-port"
#define MASTER_IP_FLAG 				"--master-ip"
#define MASTER_PORT_FLAG 			"--master-port"

typedef struct server_address_t {
	std::string ip;
	unsigned short int rpc_port;	
	unsigned short int tcp_port;
} server_address_t;

typedef struct master_address_t {
	std::string ip;
	unsigned short int rpc_port;
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

	const std::string& my_ip() const
	{
		return this->server.ip;
	} 

	const std::string& master_ip() const
	{
		return this->master.ip;
	}

	const unsigned short int& my_rpc_port() const
	{
		return this->server.rpc_port;
	}

	const unsigned short int& my_tcp_port() const
	{
		return this->server.tcp_port;
	}

	const unsigned short int& master_rpc_port() const
	{
		return this->master.rpc_port;
	}

	const std::string master_ip_port_string() const {
		std::string ip(master_ip());
		return ip.append(":").append(std::to_string(master_rpc_port()));
	}

	const std::string my_ip_port_string() const {
		std::string ip(my_ip());
		return ip.append(":").append(std::to_string(my_rpc_port()));
	}

	friend std::ostream& operator<<(std::ostream& out, const ServerInfo& server_info); 
	
private:
	server_address_t server;
	master_address_t master;
	void parse_cli_args(const std::vector<std::string>& token_stream);
};

#endif
