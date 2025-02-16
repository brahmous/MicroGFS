#include <memory>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "generated/calculator.grpc.pb.h"
#include "generated/calculator.pb.h"

calculator::Input make_input(int a, int b)
{

    calculator::Input inp;

    inp.set_a(a);
    inp.set_b(b);

    return inp;
}

int main(int argc, char *argv[])
{

    int a = 0, b = 0;

    std::cin >> a;
    std::cin >> b;

    grpc::ClientContext context;

    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:50051", grpc::InsecureChannelCredentials());

    std::unique_ptr<calculator::Calculator::Stub> stub = calculator::Calculator::NewStub(channel);

    calculator::Input inp = make_input(a, b);
    calculator::Number result;

    grpc::Status response_status = stub->Add(&context, inp, &result);

    if (!response_status.ok())
    {
        std::cout << "Call failed\n";
        return false;
    }

    std::cout << a << " + " << b << " = " << result.val() << "\n";
}