cmake_minimum_required(VERSION 3.31)

project(MicroGFS VERSION 0.0.1 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE Debug)

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -frtti")

#add_executable(client src/client/client.cpp generated/calculator.grpc.pb.cc generated/calculator.pb.cc)
add_library(grpc_generated STATIC 
											src/client/generated/GFSClientService.grpc.pb.cc
											src/client/generated/GFSClientService.pb.cc
											src/master/generated/GFSMasterService.grpc.pb.cc
											src/master/generated/GFSMasterService.pb.cc
											src/server/generated/GFSChunkServer.grpc.pb.cc
											src/server/generated/GFSChunkServer.pb.cc) 

target_link_libraries(grpc_generated gRPC::grpc++)

add_executable(client src/client/utils.cpp src/client/client.cpp)

#add_executable(server src/server/server.cpp generated/calculator.grpc.pb.cc generated/calculator.pb.cc)
add_executable(server src/server/utils.cpp src/server/server.cpp)

# add_executable(master master.cpp generated/calculator.grpc.pb.cc generated/calculator.pb.cc)
add_executable(master src/master/utils.cpp src/master/master.cpp)
#add_executable(client src/client/client.cpp generated/calculator.grpc.pb.cc generated/calculator.pb.cc)

# add_compile_options(-frtti -g)
target_link_libraries(client gRPC::grpc++ grpc_generated)
target_link_libraries(server gRPC::grpc++ grpc_generated)
target_link_libraries(master gRPC::grpc++ grpc_generated)

#target_include_directories(server PRIVATE "/home/nitro/.local/include/")
#target_include_directories(client PRIVATE "/home/nitro/.local/include/")
#target_include_directories(master PRIVATE "/home/nitro/.local/include/")

#target_include_directories(master PUBLIC headers)
