cmake_minimum_required(VERSION 3.31)

project(MicroGFS VERSION 0.0.1 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

add_executable(server server.cpp generated/calculator.grpc.pb.cc generated/calculator.pb.cc)
add_executable(client client.cpp generated/calculator.grpc.pb.cc generated/calculator.pb.cc)

target_link_libraries(server gRPC::grpc++)
target_link_libraries(client gRPC::grpc++)

