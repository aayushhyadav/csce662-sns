#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce662::CoordService;
using csce662::ServerInfo;
using csce662::Confirmation;
using csce662::ID;
using csce662::ServerList;
using csce662::SynchService;
using csce662::PathAndData;

struct zNode{
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool is_master;
    bool isActive();

};

//potentially thread safe 
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of vectors containing znodes
std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};


//func declarations
int findServer(std::vector<zNode*> v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


bool zNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}


class CoordServiceImpl final : public CoordService::Service {

    private:
        void updateRoutingTable(int cluster_id, zNode* new_server, csce662::Status* status) {
            switch (cluster_id) {
                case 1:
                    cluster1.push_back(new_server);
                    v_mutex.lock();
                    clusters[cluster_id - 1] = cluster1;
                    v_mutex.unlock();
                    status->set_status(true);
                    break;

                case 2:
                    cluster2.push_back(new_server);
                    v_mutex.lock();
                    clusters[cluster_id - 1] = cluster2;
                    v_mutex.unlock();
                    status->set_status(true);
                    break;

                case 3:
                    cluster3.push_back(new_server);
                    v_mutex.lock();
                    clusters[cluster_id - 1] = cluster3;
                    v_mutex.unlock();
                    status->set_status(true);
                    break;

                default:
                    status->set_status(false);
                    break;
            }
        }

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        int cluster_id = serverinfo->clusterid();
        int synchronizer_count = 0;
        csce662::Status status;

        confirmation->set_status(false);

        std::string log_msg = "Received heartbeat from server " + std::to_string(serverinfo->serverid()) + " of cluster " + std::to_string(cluster_id);
        log(INFO, log_msg);

        v_mutex.lock();

        for (zNode* node: clusters[cluster_id - 1]) {
            if (serverinfo->type() == "synchronizer" && node->type == "synchronizer") {
                synchronizer_count++;

            } else if (serverinfo->type() == "server") {
                if (node->serverID == serverinfo->serverid()) {
                    node->last_heartbeat = getTimeNow();
                    confirmation->set_status(true);
                    break;
                }
            }
        }

        v_mutex.unlock();

        // create an entry for synchronizer
        if (serverinfo->type() == "synchronizer" && synchronizer_count < 2) {
            zNode* new_server = new zNode();
            new_server->serverID = serverinfo->serverid();
            new_server->hostname = serverinfo->hostname();
            new_server->port = serverinfo->port();
            new_server->type = serverinfo->type();
            new_server->is_master = synchronizer_count == 0 ? true : false;

            updateRoutingTable(cluster_id, new_server, &status);

            if (status.status()) {
                confirmation->set_status(true);
                confirmation->set_ismaster(new_server->is_master);
            }
        }

        return Status::OK;
    }

    //function returns the server information for requested client id
    //this function assumes there are always 3 clusters and has math
    //hardcoded to represent this.
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        int cluster_id = ((id->id() - 1) % 3) + 1;
        
        std::string log_msg = "Received request from client " + std::to_string(id->id());
        log(INFO, log_msg);

        zNode* selected_server = nullptr;

        for (zNode* node: clusters.at(cluster_id - 1)) {
            selected_server = node;
            break;
        }
        
        if (selected_server == nullptr || !selected_server->isActive()) return Status::OK;

        serverinfo->set_serverid(selected_server->serverID);
        serverinfo->set_hostname(selected_server->hostname);
        serverinfo->set_port(selected_server->port);
        serverinfo->set_type(selected_server->type);

        return Status::OK;
    }

    // registers the servers in the routing table (clusters vector)
    Status create(ServerContext* context, const PathAndData* path_and_data, csce662::Status* status) override {
        int path_token_delimiter_index = path_and_data->path().find(':');
        int data_token_delimiter_index = path_and_data->data().find(',');
        int cluster_id = std::stoi(path_and_data->data().substr(0, data_token_delimiter_index));
        int server_id = std::stoi(path_and_data->data().substr(data_token_delimiter_index + 1));
        int server_count = 0;

        std::string log_msg = "Received request from server " + std::to_string(server_id) + " of cluster " + std::to_string(cluster_id);
        log(INFO, log_msg);

        // create the cluster directory
        if (clusters[cluster_id - 1].size() == 0) {
            if(std::filesystem::create_directories("cluster" + std::to_string(cluster_id))) {
                log(INFO, "Successfully created the cluster directory")
            }
        }

        for (zNode* node: clusters[cluster_id - 1]) {
            if (node->type == "server") server_count++;

            // check if the server is restarted
            if (node->serverID == server_id) {
                v_mutex.lock();
                node->missed_heartbeat = false;
                v_mutex.unlock();
                status->set_status(true);
                return Status::OK;
            }
        }

        // create the server directory
        if(std::filesystem::create_directories("cluster" + std::to_string(cluster_id) + "/" + std::to_string(server_id))) {
            log(INFO, "Successfully created the directory to store user files")
        }

        zNode* new_server = new zNode();
        new_server->hostname = path_and_data->path().substr(0, path_token_delimiter_index);
        new_server->port = path_and_data->path().substr(path_token_delimiter_index + 1);
        new_server->serverID = server_id;
        new_server->type = "server";
        new_server->is_master = server_count == 0 ? true : false;

        updateRoutingTable(cluster_id, new_server, status);

        if (status.status()) {
            status.set_ismaster(new_server->is_master);
        }

        return Status::OK;
    }

};

void RunServer(std::string port_no){
    //start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    
    //localhost = 127.0.0.1
    std::string server_address("127.0.0.1:"+port_no);
    CoordServiceImpl service;
    
    //grpc::EnableDefaultHealthCheckService(true);
    //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;
    log(INFO, "Coordinator listening on " + server_address);

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {

    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
        switch(opt) {
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }

    std::string log_file_name = std::string("coordinator-") + port;
    google::InitGoogleLogging(log_file_name.c_str());

    log(INFO, "Logging Initialized. Coordinator starting...");
    RunServer(port);
    return 0;
}



void checkHeartbeat(){
    while(true){
        // check servers for heartbeat > 10
        // if true turn missed heartbeat = true
        // Your code below

        v_mutex.lock();

        // iterating through the clusters vector of vectors of znodes
        for (auto& c : clusters){
            for(auto& s : c){
                if(s->type != "synchronizer" && difftime(getTimeNow(),s->last_heartbeat) > 10){
                    std::cout << "missed heartbeat from server " << s->serverID << std::endl;
                    if(!s->missed_heartbeat){
                        s->missed_heartbeat = true;
                        s->last_heartbeat = getTimeNow();
                    }
                }
            }
        }

        v_mutex.unlock();

        sleep(3);
    }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

