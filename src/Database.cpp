


#include"Database.h"
#include "Binder/Binder.h"
#include "Binder/BoundStatement.h"
#include "Binder/Statement/CreateStatement.h"
#include "CataLog/Schema.h"
#include "Optimizer/Optimizer.h"
#include "nodes/parsenodes.hpp"
#include "pg_functions.hpp"
#include <iomanip>
#include <string>
#include <type_traits>
#include "common/Exception.h"
#include "Table/Tuple.h"
#include "Planer/planer.h"
#include "Execute/core/ColumnFactory.h"
#include "Execute/core/ColumnVector.h"
extern bool show_info;
StardDataBase::StardDataBase(const std::string& db_name):db_name_(db_name){
    file_manager_ = new FileManager();
    cata_log_ = new CataLog(file_manager_,db_name_);
    execute_engine_ = new ExecuteEngine();
}
StardDataBase::~StardDataBase(){
    delete cata_log_;
    delete file_manager_;
}

void StardDataBase::ShowTables(){

    auto get_type_string=[](ColumnType type){
        switch(type){
            case  ColumnType::INT:{
                return "int";
            }
            case ColumnType::STRING:{
                return "varchar";
            }
            default:
                NOT_IMP
        }
    };

    std::stringstream ss;
    for(auto& tb_entry:cata_log_->tables_){
        ss<<"=============================="<<std::endl;
        ss<<"table_oid:"<<tb_entry.first<<std::endl
        <<"table_name:"<<tb_entry.second->table_name_<<std::endl
        <<"record:"<<tb_entry.second->table_heap_->columns_heap_[0]->metadata.total_rows<<std::endl;

        for(auto& col_heap:tb_entry.second->table_heap_->columns_heap_){
            ss<<col_heap->def_.column_name<<"-"
            <<get_type_string(col_heap->def_.col_type_)<<"-"
            <<col_heap->def_.col_length_<<std::endl;
        }
    }
    std::cout<<ss.str();
}
void StardDataBase::ExecuteSql(const std::string& query){
    if(query == "\\st"){
        ShowTables();
        return;
    }
    parser_.Parse(query);
    if(!parser_.success){
        throw Exception("SYNTAX WRONG,PARSED FAILED");         
    }
    if(parser_.parse_tree==nullptr){
        // emtry statment;
    }
    Binder binder(parser_.parse_tree,cata_log_);
    bool is_success= false;
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    SchemaRef schema{nullptr};
    
    std::chrono::time_point<std::chrono::system_clock> before = std::chrono::system_clock::now();
    auto print_result=[&,this](std::vector<ChunkRef>&& chunks){
        if(chunks.empty()){
            std::cout<<"empty set"<<std::endl;
            return ;
        }
        if(chunks[0]->rows()==0){
            std::cout<<"empty set"<<std::endl;
            return;
        }
        if(!show_info)
            return;
        std::chrono::time_point<std::chrono::system_clock> later = std::chrono::system_clock::now();
        auto second = std::chrono::duration_cast<std::chrono::microseconds>(later - before);
        writer_.ss.str("");
        writer_.printAll(std::move(chunks));
        chunks.clear();
        std::cout<<" waste time: "<< (double)((double)second.count() / (double)1000000.00000 )<<" second "<<std::endl;

    };
    for(auto* stmt : binder.statments_){
        
        auto bound_statement = binder.BindStatement(stmt);
        switch (bound_statement->type_) {
            case StatementType::CREATE_STATEMENT:{
                is_success=ExecuteCreateStatement(std::move(bound_statement));
                if(is_success==false){
                    throw Exception("Just not successful ,that fine");
                }
                auto col = std::make_shared< ColumnVector<int32_t>>("__star_database_.create_nums");
                col->data_.push_back(1);
                auto chunk = std::make_shared<Chunk>();
                chunk->appendColumn(std::move(col));
                print_result({chunk});
                break;
            }
            case StatementType::INSERT_STATEMENT:{
                std::vector<ChunkRef> result_set;
                is_success=ExecuteInsertStatement(std::move(bound_statement),result_set,schema);
                if(!is_success){
                    throw Exception("Just not successful ,that fine");
                }
                print_result(std::move(result_set));
                break;
            }
            case StatementType::SELECT_STATEMENT:{
                std::vector<ChunkRef> result_set;
                is_success = ExecuteSelectStatement(std::move(bound_statement),
                    result_set,schema);
                
                if(!is_success)
                    throw Exception("Just not successful ,that fine");
                print_result(std::move(result_set));
                break;
            }
            default:
                break;
        }
    }
    
   
}


bool StardDataBase::ExecuteCreateStatement(std::unique_ptr<BoundStatement> stmt){
    auto& create_stmt = reinterpret_cast<CreateStatement&>(*stmt);
    cata_log_->CreateTable(create_stmt.table_name_,create_stmt.columns_);
    return true;
}


bool StardDataBase::ExecuteInsertStatement(std::unique_ptr<BoundStatement> stmt,
std::vector<ChunkRef>& result_set,SchemaRef& schema){
    Planer planer(cata_log_);
    if(show_info) std::cout<<"==== Plan   Later ===="<<std::endl;
    planer.CreatePlanAndShowPlanTree(std::move(stmt));

    //optimer .
    Optimizer optimizer;
    // auto optimerzed_plan = 
    //     optimizer.OptimizerInsert(planer.plan_);

    auto optimerzed_plan =  planer.plan_;
    if(show_info) std::cout<<"==== Optimer Later ===="<<std::endl;

    planer.PreOrderTraverse(optimerzed_plan,0);

    auto context = std::make_shared<ExecuteContext>(cata_log_);
    execute_engine_->Execute(optimerzed_plan,
        context);
    DASSERT(context->physical_plan_->GetType() == ResultOperatorNode);

    // pull from result Operator
    ChunkRef chunk;
    while(true){
        auto result = context->physical_plan_->Source(chunk);
        if(result == SourceResult::FINISHED)
            break;
        DASSERT(chunk);
        result_set.push_back(std::move(chunk));
    }
    return true;
}
bool StardDataBase::ExecuteSelectStatement(
std::unique_ptr<BoundStatement> stmt,
std::vector<ChunkRef>& result_set,
SchemaRef& schema){
    
    Planer planer(cata_log_);
    std::cout<<"==== Plan   Later ===="<<std::endl;
    planer.CreatePlanAndShowPlanTree(std::move(stmt));
    Optimizer optimizer;
    std::cout<<"==== Optimer Later ===="<<std::endl;
    auto op_p =optimizer.RegularOptimize(planer.plan_);
    planer.PreOrderTraverse(op_p,0);

    auto context = std::make_shared<ExecuteContext>(cata_log_);
    execute_engine_->Execute(op_p,
        context);

    ChunkRef chunk;
    while(true){
        auto result = context->physical_plan_->Source(chunk);
        if(result == SourceResult::FINISHED)
            break;
        DASSERT(chunk);
        result_set.push_back(std::move(chunk));
    }

    return true;
}