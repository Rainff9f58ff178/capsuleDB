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
    db.ExecuteSql(create);

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
        db.ExecuteSql(insert);
    }
}
void test(StardDataBase& db){
    std::string s  = "select * from ( select sum(a.colA+1),avg(a.colB+1),avg(a.colC),min(b.colA),max(b.colC),sum(b.colD),a.colE,a.colF,b.colA,b.colE from (select colA as colA,colB as colB,colC as colC,colD as colD ,colE as colE,colF as colF from random10 where colA>10 ) as a inner join (select colA as colA ,colB as colB,colC as colC ,colD as colD ,colE as colE from random11 where colA >10 ) as b on a.colA=b.colA group by a.colE,a.colF,b.colA,b.colE having sum(b.colA) > 1 order by sum(a.colA+1)+1 )  final limit 1;";
    std::cout<<"executing ... "<<s<<std::endl;
    db.ExecuteSql(s);
}

int
main(int argc,char** argv) {


    try {

        StardDataBase db("Stard");

        std::stringstream ss;
        ss<<"Capsule Local Database. Created by wangxinyu .  --version 114514"<<std::endl;
        ss<<std::endl;
        ss<<"-- input \\st to show tables" <<std::endl;
        ss<<"-- input \\q to quit"<<std::endl;
        ss<<"-- input \\gen[1,2,3,4,5,6,7,8,9,10] to gen random table"<<std::endl;
        ss<<"-- input \\test to  test"<<std::endl;
        ss<<"-- only support varchar and int column"<<std::endl;
        ss<<"-- execute 'select * from database_info;' show more infomation  "<<std::endl;

        ss<<std::endl<<std::endl;
        std::cout<<ss.str()<<std::endl;
        
        std::string query;

        try{
            if(!db.hasTable("database_info")){

                db.ExecuteSql("create table database_info(database_name varchar(50),author varchar(50),github_addr varchar(50),description varchar(100))");
                std::string database_name = "capsuleDB";
                std::string author = "Rainff9f58ff178";
                std::string github_addr = "https://github.com/Rainff9f58ff178/capsuleDB";
                std::string description= "nice to meet you !";
                std::string god_sql = std::format("insert into database_info values('{}','{}','{}','{}');",database_name,author,github_addr,description);
                db.ExecuteSql( god_sql);
            }
        }catch(Exception e){
            // return.
        }

        show_info= true;
        while (true) {
            try {

                std::cout << "Capsule >";
                std::getline(std::cin, query);
                
                if(query.find_first_not_of(' ') == std::string::npos){
                    continue;
                }
                if (query == "\\q") {
                    break;
                }
                if(query == "\\test"){
                    test(db);
                    continue;
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
                    continue;
                }
                db.ExecuteSql(query);
            } catch (Exception e) {
                    std::cout << e.what() << std::endl;
            } catch (char const *c) {
                std::cout << c << std::endl;
            } catch (...) {
                std::cout << "unknown type exception" << std::endl;
            }
        }
    } catch(Exception e){
        //do nothing
    }catch (char const *c) {
        std::cout << c << std::endl;
    } catch (...) {
        std::cout << "Unknowned errror" << std::endl;
    }
    return 0;
}