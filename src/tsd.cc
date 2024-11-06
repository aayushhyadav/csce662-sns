/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <thread>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"


using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::Channel;
using grpc::ClientContext;
using csce662::Message;
using csce662::ListReply;
using csce662::Request;
using csce662::Reply;
using csce662::SNSService;
using csce662::PathAndData;
using csce662::Confirmation;
using csce662::ServerInfo;
using csce662::CoordService;


struct Client {
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client that has been created
std::vector<Client*> client_db;

//Vector that stores bi-directional stream corresponding to each client
std::vector<ServerReaderWriter<Message, Message>*> client_writer_streams;

//stub to invoke coordinator functions
std::unique_ptr<CoordService::Stub> stub_;

//directory to store user posts
std::string server_file_directory;

class SNSServiceImpl final : public SNSService::Service {

  private:
    std::mutex mtx;

    //constructing a string from Message object to write in the file
    std::string getMessageAsString(Message message) {
      std::string message_string("T ");

      message_string.append(google::protobuf::util::TimeUtil::ToString(message.timestamp()));
      message_string.append("\n");
      message_string.append("U ");
      message_string.append(message.username());
      message_string.append("\n");
      message_string.append("W ");
      message_string.append(message.msg());
      message_string.append("\n");

      return message_string;
    }

    void writeFileContentsToStream(std::string filename, ServerReaderWriter<Message, Message>* stream) {
      std::ifstream following_file;
      std::string cur_line;
      std::vector<std::string> tokens;
      Message message;

      following_file.open(filename);

      if (following_file.is_open()) {
        while (getline(following_file, cur_line)) {
          tokens.push_back(cur_line + "\n");
        }

        auto token = tokens.rbegin() + 1;
        std::string token_str;
        int count = 0;

        //parsing the contents read from the file in reverse direction
        //and writing the 20 latest posts into the stream
        while (token != tokens.rend() && count < 20) {
          token_str = *token;

          if (token_str[0] == 'T') {
            google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
            google::protobuf::util::TimeUtil::FromString(token_str.substr(2, token_str.length() - 3), timestamp);
            message.set_allocated_timestamp(timestamp);

          } else if (token_str[0] == 'U') {
            message.set_username(token_str.substr(2, token_str.length() - 3));

          } else if (token_str[0] == 'W') {
            message.set_msg(token_str.substr(2, token_str.length() - 2));

          } else {
            mtx.lock();
            stream->Write(message);
            mtx.unlock();
            count++;
          }
          token++;
        }

        if (count < 20) stream->Write(message);
        following_file.close();
      }
    }
  
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    
    Client* logged_in_client;

    for (Client* client: client_db) {
      if (client->username == request->username()) logged_in_client = client;
      list_reply->add_all_users(client->username);
    }

    for (Client* client: logged_in_client->client_followers) {
      list_reply->add_followers(client->username);
    }

    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {

    Client* loggedInClient = nullptr;
    Client* clientToFollow = nullptr;

    bool can_follow = true;

    for (Client* client: client_db) {
      if (client->username == request->username()) {
        loggedInClient = client;
      }
      if (client->username == request->arguments(0)) {
        clientToFollow = client;
      }
      if (loggedInClient != nullptr && clientToFollow != nullptr) {
        break;
      }
    }

    if (clientToFollow == nullptr) {
      reply->set_msg("Command failed with invalid username\n");
      
    } else if (clientToFollow == loggedInClient) {
      reply->set_msg("Input username already exists, command failed\n");

    } else {
      for (Client* client: loggedInClient->client_following) {
        if (client->username == clientToFollow->username) {
          can_follow = false;
          reply->set_msg("Input username already exists, command failed\n");
          break;
        }
      }

      if (can_follow) {
        loggedInClient->client_following.push_back(clientToFollow);
        clientToFollow->client_followers.push_back(loggedInClient);
        reply->set_msg("Command completed successfully\n");
      }
    }

    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {

    Client* loggedInClient = nullptr;
    Client* clientToUnFollow = nullptr;

    int curIndex;
    bool can_unfollow = false;

    for (Client* client: client_db) {
      if (client->username == request->username()) {
        loggedInClient = client;
      }
      if (client->username == request->arguments(0)) {
        clientToUnFollow = client;
      }
      if (loggedInClient != nullptr && clientToUnFollow != nullptr) {
        break;
      }
    }

    if (clientToUnFollow == nullptr || loggedInClient == clientToUnFollow) {
      reply->set_msg("Command failed with invalid username\n");

    } else {
      curIndex = 0;
      auto clientFollowingIterator = loggedInClient->client_following.begin();
      auto clientFollowersIterator = clientToUnFollow->client_followers.begin();

      for (Client* client: loggedInClient->client_following) {
        if (client == clientToUnFollow) {
          loggedInClient->client_following.erase(clientFollowingIterator + curIndex);
          can_unfollow = true;
          break;
        }
        curIndex++;
      }

      if (!can_unfollow) {
        reply->set_msg("Command failed with invalid username\n");
        return Status::OK;
      }

      curIndex = 0;

      for (Client* client: clientToUnFollow->client_followers) {
        if (client == loggedInClient) {
          clientToUnFollow->client_followers.erase(clientFollowersIterator + curIndex);
          break;
        }
        curIndex++;
      }

      reply->set_msg("Command completed successfully\n");
    }

    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {

    if (client_db.size() > 0) {
      for (Client* existingClient: client_db) {
        if (existingClient->username == request->username()) {
          reply->set_msg("User already exists!");
          return Status::OK;
        }
      } 
    }

    Client* newClient = new Client();
    newClient->username = request->username();
    
    client_db.push_back(newClient);
    reply->set_msg("Successfully logged in!");

    return Status::OK;
  }

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {

    Client* author = 0;
    Message message;
    std::ofstream sender_file;
    std::ofstream follower_file;

    auto meta_data = context->client_metadata().find("username");

    mtx.lock();
    for (Client* client: client_db) {
      if (meta_data->second == client->username) {
        client_writer_streams.push_back(stream);
        author = client;
        author->stream = client_writer_streams.back();
        break;
      }
    }
    mtx.unlock();

    writeFileContentsToStream(server_file_directory + "/" + author->username + "_following.txt", stream);

    while (stream->Read(&message)) {
      std::string message_string = getMessageAsString(message);
    
      sender_file.open(server_file_directory + "/" + author->username + ".txt", std::ios_base::app);
      sender_file << message_string;
      sender_file.close();

      for (Client* follower: author->client_followers) {
        mtx.lock();

        if (follower->stream != 0) follower->stream->Write(message);
        follower_file.open(server_file_directory + "/" + follower->username + "_following.txt", std::ios_base::app);
        follower_file << message_string;
        follower_file.close();
        
        mtx.unlock();
      }
    }

    return Status::OK;
  }

};

void sendHeartbeat(PathAndData path_and_data) {
  ServerInfo server_info;
  Confirmation confirmation;

  int path_token_delimiter_index = path_and_data.path().find(':');
  int data_token_delimiter_index = path_and_data.data().find(',');

  server_info.set_hostname(path_and_data.path().substr(0, path_token_delimiter_index));
  server_info.set_port(path_and_data.path().substr(path_token_delimiter_index + 1));
  server_info.set_serverid(std::stoi(path_and_data.data().substr(data_token_delimiter_index + 1)));
  server_info.set_type("server");
  server_info.set_clusterid(stoi(path_and_data.data().substr(0, data_token_delimiter_index)));
  std::string cluster_id = path_and_data.data().substr(0, data_token_delimiter_index);

  while (true) {
    ClientContext client_context;
    
    log(INFO, "Sending heartbeat to the Coordinator");
    stub_->Heartbeat(&client_context, server_info, &confirmation);
    
    sleep(5);
  }
}

// server registers with the coordinator
void connectToCoordinator(PathAndData path_and_data, std::string coordinator_ip, std::string coordinator_port) {
  auto channel = grpc::CreateChannel(coordinator_ip + ":" + coordinator_port, grpc::InsecureChannelCredentials());
  stub_ = CoordService::NewStub(channel);

  ClientContext client_context;
  csce662::Status status;

  log(INFO, "Registering with the Coordinator...");
  stub_->create(&client_context, path_and_data, &status);

  if (status.status()) {
    // spawn a new thread to send heartbeats to the coordinator
    std::thread heartbeat(sendHeartbeat, path_and_data);
    heartbeat.join();
  }
}

void RunServer(std::string port_no, std::string cluster_id, std::string server_id,
std::string coordinator_ip, std::string coordinator_port) {

  std::string server_address = "0.0.0.0:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on " + server_address);

  PathAndData path_and_data;
  path_and_data.set_path(server_address);
  path_and_data.set_data(cluster_id + "," + server_id);

  connectToCoordinator(path_and_data, coordinator_ip, coordinator_port);

  server->Wait();
}

int main(int argc, char** argv) {

  std::string port = "10000";
  std::string cluster_id = "1";
  std::string server_id = "1";
  std::string coordinator_ip = "localhost";
  std::string coordinator_port = "3010";

  int opt = 0;
  while ((opt = getopt(argc, argv, "p:c:s:h:k:")) != -1){
    switch(opt) {
      case 'p':
        port = optarg;break;
      case 'c':
        cluster_id = optarg;break;
      case 's':
        server_id = optarg;break;
      case 'h':
        coordinator_ip = optarg;break;
      case 'k':
        coordinator_port = optarg;break;
      default:
	      std::cerr << "Invalid Command Line Argument\n";
    }
  }

  server_file_directory = "server_" + cluster_id + "_" + server_id;
  
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());

  // create the directory to store user files containing their posts 
  if(std::filesystem::create_directories(server_file_directory)) {
    log(INFO, "Successfully created the directory to store user posts")
  }
  
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(port, cluster_id, server_id, coordinator_ip, coordinator_port);

  return 0;
}
