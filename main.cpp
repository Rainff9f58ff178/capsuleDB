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
        ss<<std::endl<<std::endl;
        std::cout<<ss.str()<<std::endl;
        
        std::string query;
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
    } catch (char const *c) {
        std::cout << c << std::endl;
    } catch (...) {
        std::cout << "Unknowned errror" << std::endl;
    }
    return 0;
}