protoc -I ./protos --grpc_out=./generated/ --plugin=protoc-gen-grpc=/home/nitro/.local/bin/grpc_cpp_plugin ./protos/GFSClientService.proto

protoc -I ./protos/ --cpp_out=./generated/ ./protos/GFSClientService.proto

