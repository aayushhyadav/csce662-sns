#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <string>
#include <unistd.h>
#include <csignal>
#include <grpc++/grpc++.h>
#include "client.h"

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using csce662::Message;
using csce662::ListReply;
using csce662::Request;
using csce662::Reply;
using csce662::SNSService;
using csce662::ID;
using csce662::ServerInfo;
using csce662::CoordService;

void sig_ignore(int sig) {
  std::cout << "Signal caught " + sig;
}

Message MakeMessage(const std::string& username, const std::string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}


class Client : public IClient
{
public:
  Client(const std::string& hname,
	 const std::string& uname,
	 const std::string& p)
    :hostname(hname), username(uname), port(p) {}

  
protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string& input);
  virtual void processTimeline();

private:
  std::string hostname;
  std::string username;
  std::string port;
  
  // You can have an instance of the client stub
  // as a member variable.
  std::unique_ptr<SNSService::Stub> stub_;

  // stub to invoke coordinator functions
  std::unique_ptr<CoordService::Stub> stub_coordinator;
  
  IReply Login();
  IReply List();
  IReply Follow(const std::string &username);
  IReply UnFollow(const std::string &username);
  void   Timeline(const std::string &username);
};


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo()
{
  // ------------------------------------------------------------
  // In this function, you are supposed to create a stub so that
  // you call service methods in the processCommand/porcessTimeline
  // functions. That is, the stub should be accessible when you want
  // to call any service methods in those functions.
  // Please refer to gRpc tutorial how to create a stub.
  // ------------------------------------------------------------
    
///////////////////////////////////////////////////////////

    //create a connection with the Coordinator
    auto coordinator_channel = grpc::CreateChannel(hostname + ":" + port, grpc::InsecureChannelCredentials());
    stub_coordinator = CoordService::NewStub(coordinator_channel);

    ClientContext client_context;
    ID id;
    ServerInfo server_info;

    id.set_id(std::stoi(username));
    stub_coordinator->GetServer(&client_context, id, &server_info);

    //create a connection with the server
    auto server_channel = grpc::CreateChannel(server_info.hostname() + ":" + server_info.port(), grpc::InsecureChannelCredentials());
    stub_ = SNSService::NewStub(server_channel);

    IReply ire = Login();

    if (ire.comm_status != SUCCESS) return -1;

//////////////////////////////////////////////////////////

    return 1;
}

IReply Client::processCommand(std::string& input)
{
  // ------------------------------------------------------------
  // GUIDE 1:
  // In this function, you are supposed to parse the given input
  // command and create your own message so that you call an 
  // appropriate service method. The input command will be one
  // of the followings:
  //
  // FOLLOW <username>
  // UNFOLLOW <username>
  // LIST
  // TIMELINE
  // ------------------------------------------------------------
  
  // ------------------------------------------------------------
  // GUIDE 2:
  // Then, you should create a variable of IReply structure
  // provided by the client.h and initialize it according to
  // the result. Finally you can finish this function by returning
  // the IReply.
  // ------------------------------------------------------------
  
  
  // ------------------------------------------------------------
  // HINT: How to set the IReply?
  // Suppose you have "FOLLOW" service method for FOLLOW command,
  // IReply can be set as follow:
  // 
  //     // some codes for creating/initializing parameters for
  //     // service method
  //     IReply ire;
  //     grpc::Status status = stub_->FOLLOW(&context, /* some parameters */);
  //     ire.grpc_status = status;
  //     if (status.ok()) {
  //         ire.comm_status = SUCCESS;
  //     } else {
  //         ire.comm_status = FAILURE_NOT_EXISTS;
  //     }
  //      
  //      return ire;
  // 
  // IMPORTANT: 
  // For the command "LIST", you should set both "all_users" and 
  // "following_users" member variable of IReply.
  // ------------------------------------------------------------

    IReply ire;
    
    std::string delimiter = " ";
    std::size_t index = input.find_first_of(delimiter);

    if (index != std::string::npos) {
      if (input.substr(0, index) == "FOLLOW") {
        ire = Follow(input.substr(index + 1, input.size()));

      } else if (input.substr(0, index) == "UNFOLLOW") {
        ire = UnFollow(input.substr(index + 1, input.size()));

      } else {
        ire.comm_status = FAILURE_INVALID;
        ire.grpc_status = Status::OK;
      }

    } else {
      if (input == "LIST") {
        ire = List();

      } else {
        // for timeline command, invoke the list function to check if the server is active or not
        // because the timeline function does not return
        ire = List();
      }
    }

    return ire;
}


void Client::processTimeline()
{
    Timeline(username);
}

// List Command
IReply Client::List() {

    IReply ire;

    ClientContext context;
    Request request;
    ListReply list_reply;

    request.set_username(username);
    Status status = stub_->List(&context, request, &list_reply);

    for (std::string username: list_reply.all_users()) {
      ire.all_users.push_back(username);
    }

    for (std::string username: list_reply.followers()) {
      ire.followers.push_back(username);
    }

    // if all_users is empty then server is not active
    ire.comm_status = (list_reply.all_users().size() == 0) ? FAILURE_UNKNOWN : SUCCESS;
    ire.grpc_status = Status::OK;

    return ire;
}

// Follow Command        
IReply Client::Follow(const std::string& username2) {

    IReply ire; 
    
    ClientContext context;
    Request request;
    Reply reply;

    request.set_username(username);
    request.add_arguments(username2);

    Status status = stub_->Follow(&context, request, &reply);

    if (reply.msg() == "Command failed with invalid username\n") {
      ire.comm_status = FAILURE_INVALID_USERNAME;

    } else if(reply.msg() == "Input username already exists, command failed\n") {
      ire.comm_status = FAILURE_ALREADY_EXISTS;

    } else if (reply.msg().length() == 0) {
      ire.comm_status = FAILURE_UNKNOWN;

    } else {
      ire.comm_status = SUCCESS;
    }

    ire.grpc_status = Status::OK;
    return ire;
}

// UNFollow Command  
IReply Client::UnFollow(const std::string& username2) {

    IReply ire;

    ClientContext context;
    Request request;
    Reply reply;

    request.set_username(username);
    request.add_arguments(username2);
    
    Status status = stub_->UnFollow(&context, request, &reply);

    if (reply.msg() == "Command failed with invalid username\n") {
      ire.comm_status = FAILURE_INVALID_USERNAME;

    } else if (reply.msg().length() == 0) {
      ire.comm_status = FAILURE_UNKNOWN;
      
    } else {
      ire.comm_status = SUCCESS;
    }

    ire.grpc_status = Status::OK;
    return ire;
}

// Login Command  
IReply Client::Login() {

    IReply ire;
  
    ClientContext context;
    Request request;
    Reply reply;

    request.set_username(username);

    Status status = stub_->Login(&context, request, &reply);
    
    if (reply.msg() == "User already exists!") {
        ire.comm_status = FAILURE_ALREADY_EXISTS;

    } else if (reply.msg().size() != 0) {
        ire.comm_status = SUCCESS;

    } else {
      ire.comm_status = FAILURE_UNKNOWN;
    }

    ire.grpc_status = Status::OK;
    return ire;
}

// Timeline Command
void Client::Timeline(const std::string& username) {

    // ------------------------------------------------------------
    // In this function, you are supposed to get into timeline mode.
    // You may need to call a service method to communicate with
    // the server. Use getPostMessage/displayPostMessage functions 
    // in client.cc file for both getting and displaying messages 
    // in timeline mode.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
    // ------------------------------------------------------------
  
    ClientContext context;
    context.AddMetadata("username", username);

    std::shared_ptr<ClientReaderWriter<Message, Message>> stream(stub_->Timeline(&context));

    std::thread writer([stream, username]() {
      while (1) {
        std::string post_message = getPostMessage();
        stream->Write(MakeMessage(username, post_message));
      }
    });


    std::thread reader([stream]() {
      Message incoming_message;
      while (stream->Read(&incoming_message)) {
        time_t timestamp_in_seconds = incoming_message.timestamp().seconds();
        displayPostMessage(incoming_message.username(), incoming_message.msg(), timestamp_in_seconds);
      }
    });

    writer.join();
    reader.join();
    Status status = stream->Finish();
    
    return;
}



//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  std::string hostname = "localhost";
  std::string username = "default";
  std::string port = "3010";
    
  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:k:")) != -1){
    switch(opt) {
    case 'h':
      hostname = optarg;break;
    case 'u':
      username = optarg;break;
    case 'k':
      port = optarg;break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }
      
  std::cout << "Logging Initialized. Client starting...";
  
  Client myc(hostname, username, port);
  
  myc.run();
  
  return 0;
}
