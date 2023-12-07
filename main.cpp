// NOLINTNEXTLINE
#include "CataLog/CataLog.h"
#include "Database.h"
#include "common/Exception.h"
#include "postgres_parser.hpp"
#include <cassert>
#include <iostream>
#include <string>
#include <stack>

using namespace std;

bool show_info= false;
int
main() {

    try {

        StardDataBase db("Stard");

        std::stringstream ss;
        ss<<"Capsule Local Database. Created by wangxinyu .  --version 114514"<<std::endl;
        ss<<std::endl;
        ss<<"-- input \\st to show tables" <<std::endl;
        ss<<"-- input \\q to quit"<<std::endl;
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