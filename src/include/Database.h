

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
        max_size.clear();
        max_size.resize(chunks[0]->columns(),0);

        for(auto& chunk:chunks){
            for(uint32_t i=0;i<chunk->columns_.size();++i){
                auto& col = chunk->columns_[i];
                max_size[i] = std::max((uint32_t)col->name_.size(),max_size[i]);
                max_size[i] = std::max(col->max_char_size(),max_size[i]);
            }
        }
        std::vector<std::string> names;
        for(auto& col:chunks[0]->columns_){
            names.push_back(col->name_);
        }

        PrintSomething("+",ss);
        for(uint32_t i=0;i<names.size();++i){
            PrintSomething("-",ss,max_size[i]);
            PrintSomething("+",ss);
        }
        ss<<std::endl;
        PrintSomething("|",ss);
        for(uint32_t i=0;i<names.size();++i){
            ss<<std::setw(max_size[i])<<names[i]
            <<"|";
        }
        ss<<std::endl;
        PrintSomething("+",ss);
        for(uint32_t i=0; i<names.size();++i){
            PrintSomething("-",ss,max_size[i]);
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
            for(uint32_t j=0; j< chunk->columns_.size();++j){
                ss<<std::setw(max_size[j]);
                ss<<chunk->columns_[j]->toString(i)<<"|";
            }
            ss<<std::endl;
            PrintSomething("+",ss);

            for(uint32_t j=0;j<chunk->columns_.size();++j){
                PrintSomething("-",ss,max_size[j]);
                PrintSomething("+",ss);
            }
            ss<<std::endl;
        }
    }
    std::vector<uint32_t> max_size;
};
class StardDataBase{
public:
    bool ignore_output_= true;
    constexpr static int VERSION =1;
    inline int GetVersion(){
            return VERSION;
    }
    explicit StardDataBase(const std::string& db_name);
    ~StardDataBase();

    void ExecuteSql(const std::string& query);
    
    bool hasTable(const std::string& table_name){
        for(auto& it :cata_log_->tables_){
            if(it.second->table_name_ ==table_name)
                return true;
        }
        return false;
    }
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