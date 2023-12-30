// NOLINTNEXTLINE
#include "CataLog/CataLog.h"
#include "Database.h"
#include "common/Exception.h"
#include "postgres_parser.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <stack>
#include<regex>
#include<random>
#include "./src/logger/Logger.h"
#include "asio2/asio2.hpp"

using namespace std;

Logger g_logger("global.log");
bool show_info= false;
//env 

std::string random_string(std::size_t length)
{
    const std::string CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distribution(0, CHARACTERS.size() - 1);

    std::string random_string;

    for (std::size_t i = 0; i < length; ++i)
    {
        random_string += CHARACTERS[distribution(generator)];
    }

    return random_string;
}


void GenRandomTable(StardDataBase& db,int num){
    std::stringstream table_name ;
    table_name<<"random"<<num;
    if(db.hasTable(table_name.str()))
        return;
    auto create = std::format( "create table {} (colA int,colB int,colC varchar(50),colD varchar(50),colE varchar(50),colF varchar(50) ) ",table_name.str());
    std::string r;
    db.ExecuteSql(create,r);

    for(uint32_t i=0;i<num*1000;++i){
        srand(std::chrono::system_clock::now().time_since_epoch().count());
        int colA = rand() % (num*1000);
        srand(std::chrono::system_clock::now().time_since_epoch().count());
        int colB = rand() % (num*1000);
        auto colC = random_string(3);
        auto colD = random_string(3);
        auto colE = random_string(10);
        auto colF = random_string(10);
        auto insert = std::format("insert into {} values({},{},'{}','{}','{}','{}')",table_name.str(),colA,colB,colC,colD,colE,colF);
        std::string r;
        db.ExecuteSql(insert,r);
    }
}
std::string test(StardDataBase& db){
    std::stringstream ss;
    std::string s  = "select * from ( select sum(a.colA+1),avg(a.colB+1),avg(a.colC),min(b.colA),max(b.colC),sum(b.colD),a.colE,a.colF,b.colA,b.colE from (select colA as colA,colB as colB,colC as colC,colD as colD ,colE as colE,colF as colF from random10 where colA>10 ) as a inner join (select colA as colA ,colB as colB,colC as colC ,colD as colD ,colE as colE from random11 where colA >10 ) as b on a.colA=b.colA group by a.colE,a.colF,b.colA,b.colE having sum(b.colA) > 1 order by sum(a.colA+1)+1 )  final limit 1;";
    ss<<"executing ... "<<s<<std::endl;
    std::string result ; 
    if(!db.hasTable("random10")){
        GenRandomTable(db, 10);
    }
    if(!db.hasTable("random11")){
        GenRandomTable(db,11);
    }
    db.ExecuteSql(s,result);
    ss<< result;
    return ss.str();
}

void init(StardDataBase& db){
    try{
        if(!db.hasTable("database_info")){
            std::string r ; 
            db.ExecuteSql("create table database_info(database_name varchar(50),author varchar(50),github_addr varchar(50),description varchar(100))",r);
            std::string database_name = "capsuleDB";
            std::string author = "Rainff9f58ff178";
            std::string github_addr = "https://github.com/Rainff9f58ff178/capsuleDB";
            std::string description= "nice to meet you !";
            std::string god_sql = std::format("insert into database_info values('{}','{}','{}','{}');",database_name,author,github_addr,description);
            db.ExecuteSql( god_sql,r);
        }
    }catch(Exception e){

    }
}

std::string dealSql(StardDataBase&db ,std::string_view sql){
    std::string query(sql);



    show_info= true;
        try {
            if(query.find_first_not_of(' ') == std::string::npos){
                return "";
            }
            if(query == "\\test"){
                test(db);
            }
            if(strstr(query.c_str(),"\\gen")){
        
                std::string output = std::regex_replace(
                    query,
                    std::regex("[^0-9]*([0-9]+).*"),
                    std::string("$1")
                );
                int num = stoi(output);
                if(num > 1000 ){
                    throw Exception("too many rows");
                }
                GenRandomTable(db,num);
            }
            std::string result;
            db.ExecuteSql(query,result);
            return result;
        } catch (Exception e) {
            return e.what();
        } catch (char const *c) {
            return std::string(c);
        } catch (...) {
            // std::cout << "" << std::endl;
            return "unknown type exception";
        }
    return "";
}

int
main(int argc,char** argv) {
    if(argc <= 2 ){
        std::cout<<"input listenaddr and port"<<std::endl;
        return 0;
    }
    StardDataBase db("Stard");
    init(db);

    asio2::tcp_server server;
    server.bind_recv([&](std::shared_ptr<asio2::tcp_session>& session_ptr,std::string_view s){
        try{
        std::string query(s);
        auto it = query.find("\\endrequest");
        query = query.substr(0,it);
        DEBUG(std::format("recev sql : {} ",query));

        if(query == "\\st"){
            session_ptr->async_send(db.ShowTables().append("\\endresponse"));
            return;
        }
        if(strstr(query.c_str(),"\\gen")){
    
            std::string output = std::regex_replace(
                query,
                std::regex("[^0-9]*([0-9]+).*"),
                std::string("$1")
            );
            int num = stoi(output);
            if(num > 1000 ){
                throw Exception("too many rows");
            }
            std::cout<<"gening  "<<num <<" thousands rows table "<<std::endl;
            GenRandomTable(db,num);
            session_ptr->async_send("generate over.\\endresponse");
            return;
        }
        if(query=="\\test"){
            session_ptr->async_send(test(db).append("\\endresponse"));
            return;
        }

        std::string result;
            db.ExecuteSql( query,result);
            session_ptr->async_send(result.append("\\endresponse"));
        }catch(Exception e){
            DEBUG(std::string(e.what()).append("\\endresponse"));
            session_ptr->async_send(std::string(e.what()).append("\\endresponse"));
        }catch(...){
            DEBUG("Unknowed error");
            session_ptr->async_send(std::string("unknowned error .\\endresponse"));
        }
    }).bind_connect([&](std::shared_ptr<asio2::tcp_session>& session_ptr){
        DEBUG(std::format("{},{} connect ",session_ptr->remote_address(),session_ptr->remote_port()));
        std::stringstream ss;
        ss<<"Capsule Database. Created by wangxinyu .  --version 114514"<<std::endl;
        ss<<std::endl;
        ss<<"-- input \\st to show tables" <<std::endl;
        ss<<"-- input \\gen[1,2,3,4,5,6,7,8,9,10] to gen random table"<<std::endl;
        ss<<"-- input \\test to  test"<<std::endl;
        ss<<"-- only support varchar and int column"<<std::endl;
        ss<<"-- execute 'select * from database_info;' show more infomation  "<<std::endl;

        ss<<std::endl<<std::endl;
        session_ptr->async_send(ss.str().append("\\endresponse"));

    }).bind_disconnect([&](auto& session_ptr){
        DEBUG(std::format("{},{} disconnect",session_ptr->remote_address(),session_ptr->remote_port()));
    });
    

    server.start(argv[1],argv[2],"\\endrequest");

   
    while (std::getchar() != '\n');  // press enter to exit this program
    std::cout<<"exiting ..."<<std::endl;
	server.stop();
    return 0;
}