#include <iostream>

#include <grpc/grpc.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>

#include "generated/calculator.grpc.pb.h"
#include "generated/calculator.pb.h"

class CalculatorService final : public calculator::Calculator::Service
{

public:
	grpc::Status Add(grpc::ServerContext *context, const calculator::Input *input, calculator::Number *number) override
	{
		number->set_val((input->a() + input->b()));
		return grpc::Status::OK;
	}
};

int main(int argc, char *argv[])
{
	std::string server_address("0.0.0.0:50051");
	CalculatorService service;

	grpc::ServerBuilder builder;
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	builder.RegisterService(&service);
	std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
	std::cout << "Server listening on " << server_address << std::endl;
	server->Wait();
}
