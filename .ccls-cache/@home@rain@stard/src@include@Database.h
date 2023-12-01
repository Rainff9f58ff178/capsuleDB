

#pragma once

#include "CataLog/CataLog.h"
#include "CataLog/Schema.h"
#include "postgres_parser.hpp"
#include"Binder/Binder.h"
#include "Table/Tuple.h"
#include "BufferManager.h"
#include "Execute/ExecuteEngine.h"
#include <iomanip>
#include <sstream>
class ResultWriter{
public:
    std::stringstream ss;
    void printAll(std::vector<ChunkRef> chunks){
        WriteHeader(chunks);
        for(auto& chunk:chunks){
            WriteChunk(chunk);
        }
        std::cout<<ss.str()<<std::endl;
    }
    void WriteHeader(std::vector<ChunkRef>& chunks){
        // get the max size of a line ,stupid...
        max_size_=0;
        for(auto& chunk:chunks){
            for(auto& col : chunk->columns_){
                max_size_ = std::max((uint32_t)col->name_.size(),max_size_);
                max_size_ = std::max(col->max_char_size(),max_size_);
            }
        }
        std::vector<std::string> names;
        for(auto& col:chunks[0]->columns_){
            names.push_back(col->name_);
        }

        PrintSomething("+",ss);
        for(auto& col:names){
            PrintSomething("-",ss,max_size_);
            PrintSomething("+",ss);
        }
        ss<<std::endl;
        PrintSomething("|",ss);
        for(auto& col:names){
            ss<<std::setw(max_size_)<<col
            <<"|";
        }
        ss<<std::endl;
        PrintSomething("+",ss);
        for(auto& col:names){
            PrintSomething("-",ss,max_size_);
            PrintSomething("+",ss);
        }
        ss<<std::endl;
    }
    void PrintSomething(const std::string& s,std::stringstream& ss,uint32_t times=1){
        for(uint32_t i=0;i<times;++i){
            ss<<s;
        }
    }
    void WriteChunk(ChunkRef& chunk){
        for(uint32_t i=0;i<chunk->rows();++i){
            ss<<"|";
            for(auto& col : chunk->columns_){
                ss<<std::setw(max_size_);
                ss<<col->toString(i)<<"|";
            }
            ss<<std::endl;
            PrintSomething("+",ss);

            for(auto& col:chunk->columns_){
                PrintSomething("-",ss,max_size_);
                PrintSomething("+",ss);
            }
            ss<<std::endl;
        }
    }
    uint32_t max_size_;
};
class StardDataBase{
public:
    constexpr static int VERSION =1;
    inline int GetVersion(){
            return VERSION;
    }
    explicit StardDataBase(const std::string& db_name);
    ~StardDataBase();

    void ExecuteSql(const std::string& query);
    
private:
    bool ExecuteCreateStatement(std::unique_ptr<BoundStatement> stmt);
    bool ExecuteInsertStatement(std::unique_ptr<BoundStatement> stmt,
        std::vector<ChunkRef>& result_set,SchemaRef& schema);
    bool ExecuteSelectStatement(
        std::unique_ptr<BoundStatement> stmt,
        std::vector<ChunkRef>& result_set,
        SchemaRef& schema);

    void ShowTables();
    duckdb::PostgresParser parser_;
    FileManager* file_manager_;
    CataLog* cata_log_;

    ExecuteEngine* execute_engine_;    
    std::string db_name_;
    ResultWriter writer_;
    
};  