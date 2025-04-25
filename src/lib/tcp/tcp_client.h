#ifndef CLIENT_TCP_H
#define CLIENT_TCP_H

#include "../../headers/main.h"
#include <arpa/inet.h>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define EXIT_IF_TRUE(condition, message) if (condition) {MESSAGE(message); exit(1);}

class TCPClient {

public:
	TCPClient (const tcp_rpc_server_descriptor_t& server_info): 
		socket_{socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)} 
	{
		EXIT_IF_TRUE(socket_ < 0, "socket initialization failed\n");

		memset(&server_address_, 0, sizeof(server_address_));

		server_address_.sin_family = AF_INET;
		int address_conversion_error_code = inet_pton(AF_INET, server_info.ip.c_str(), &server_address_.sin_addr.s_addr);
		EXIT_IF_TRUE(address_conversion_error_code == 0, "invalid ip address string");
		EXIT_IF_TRUE(address_conversion_error_code < 0, "address conversion failed");

		server_address_.sin_port = htons(reinterpret_cast<in_port_t>(server_info.tcp_port));
	}

	void connectToServer() {
		int connect_error_code = connect(socket_, (struct sockaddr *) &server_address_, sizeof(server_address_));
		EXIT_IF_TRUE(connect_error_code < 0, "establishing connection failed!");
	}

	void write(std::string write_id, const char * buffer, size_t buffer_size);

private:
	int socket_;
	struct sockaddr_in server_address_;
};

#endif
