#ifndef SERVER_TCP_H
#define SERVER_TCP_H

#include <cstring>
#include <functional>
#include <netinet/in.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "../../headers/main.h"

#define EXIT_IF_TRUE(condition, message) if (condition) {MESSAGE(message); exit(1);}

class TCPServer {

public:
	TCPServer (const tcp_rpc_server_descriptor_t& server_info, int max_connections = 5):
	socket_ {socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)},
	max_connections_{max_connections}
	{
		EXIT_IF_TRUE(socket_ < 0, "socket() failed");
		memset(&server_address_, 0, sizeof(server_address_));
		server_address_.sin_family = AF_INET;
		int ip_address_conversion_error_code = inet_pton(AF_INET, server_info.ip.c_str(), &server_address_.sin_addr.s_addr); 

		EXIT_IF_TRUE(ip_address_conversion_error_code == 0, "invalid ip address string");
		EXIT_IF_TRUE(ip_address_conversion_error_code < 0, "ip address conversion failed");
		server_address_.sin_port = htons(reinterpret_cast<in_port_t>(server_info.tcp_port));

		int socket_bind_error_code = bind(socket_, (struct sockaddr *) &server_address_, sizeof(server_address_));
		EXIT_IF_TRUE(socket_bind_error_code < 0, "call to bind socket failed");
	}

	void wait(std::function<void(int)> worker) {
		int socket_listen_error_code = listen(socket_, max_connections_);
		EXIT_IF_TRUE(socket_listen_error_code < 0, "socket listen failed!");

		while(true) {
			struct sockaddr_in client_address;
			socklen_t client_address_length = sizeof(client_address);
			int client_socket = accept(socket_, (struct sockaddr *) &client_address, &client_address_length);
			threads_.emplace_back(std::thread(worker, client_socket));
			// the worker needs to acknowledge to the client. get it from write id attached to data.
		}
	}

	void data_transfer_worker() {
		
	} 

	~TCPServer() {
		for(std::thread& th: threads_) {
			th.join();
		} 
		close(socket_);
	}

private:
	int socket_;
	int max_connections_;
	struct sockaddr_in server_address_;
	std::vector<std::thread> threads_;
};

#endif
