# MicroGFS

```
master -ip <ipv4-address> -p 3030 -r <ipv4-adress> -p 3030 -r <ipv4-address> -p
client -ip <ipv4-address> -p 3030 -rpcp 4040 -master-ip <ipv4-address>
server -ip <ipv4-address> -p 4040 -rpcp 4040 -master-ip -master-port
```

the client sends a request <rpc> to the master
the master responds

the client informs <rpc> all the servers that they will receive data and need to confirm to the client

– so the client receives an ip-adress of the client and two ports one for rpc and one for data transfer over tcp.

so the client informs the client and innitiates transport over tcp

the chunk server says hi to the master <rpc> the master adds the server to the list and asks for chunks <rpc> and uses secondary index <chunk-handle to name to file-struct> to update the location information for available chunks

the master is an rpc server for the client but also an rpc client for master replicas

### server:
- rpc-server for heart beat and chunk location collection with the master. The master collects data by making calls to the server.
```
service chunk_data_metadata_server {

}
```
- rpc-server and client for acknowledegement of recepetion of chunks with the client. 
```
service reminder_of_acknowledgement {

}
```
### master:
- rpc-client for heart beat and chunk location collection with the master

- rpc-server for client requests
```
service file_syste_operations {

  create,
  read,
  rename,
  delete,
  write

}
```
### client:
- rpc-server and client for acknowledegement of reception of chunks with the client
```
servcie i_received_acknoweldegement_i'll_send_the_write_command {
  
}
```
- rpc-client for client commands for the master
