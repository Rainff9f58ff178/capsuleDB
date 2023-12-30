#include "asio2/asio2.hpp"
#include<iostream>


void block_read_and_send(asio2::tcp_client& client){
    std::cout<<std::endl;
    std::string query;
    std::cout << "Capsule >";
    std::getline(std::cin, query);
    query.append("\\endrequest");
    client.async_send(query);
}

int main(int argc,char** argv){
    if(argc <= 2 ){
        std::cout<<"need input ip addr and port num"<<std::endl;
        return 0;
    }
    asio2::tcp_client client;
    client.auto_reconnect(true, std::chrono::seconds(3));

    client.bind_connect([&]()
    {
        if (asio2::get_last_error())
            printf("connect failure : %d %s\n", asio2::last_error_val(), asio2::last_error_msg().c_str());
        else
            printf("connect success : %s %u\n", client.local_address().c_str(), client.local_port());


    }).bind_disconnect([]()
    {
        printf("disconnect : %d %s\n", asio2::last_error_val(), asio2::last_error_msg().c_str());
    }).bind_recv([&](std::string_view sv)
    {
        auto it = sv.find("\\endresponse");

        std::cout<<sv.substr(0,it)<<std::endl;
        block_read_and_send(client);
    });

    client.start(argv[1], argv[2], "\\endresponse");

    client.wait_stop();
}