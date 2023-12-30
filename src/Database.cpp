


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
#include "Logger.h"
extern bool show_info;
StardDataBase::StardDataBase(const std::string& db_name):db_name_(db_name){
    file_manager_ = new FileManager();
    cata_log_ = new CataLog(file_manager_,db_name_);
    execute_engine_ = new ExecuteEngine();
}
StardDataBase::~StardDataBase(){
    delete execute_engine_;
    delete cata_log_;
    delete file_manager_;
}

std::string StardDataBase::ShowTables(){

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
    return ss.str();
}
void StardDataBase::ExecuteSql(const std::string& query, std::string& result){
    DEBUG(std::format("Execute Sql : {}",query));
    if(query == "\\st"){
        result = ShowTables();
        return;
    }
    parser_.Parse(query);
    if(!parser_.success){
        throw Exception(std::format("sytax error near in \"{}\" ",std::string(query.data()+ parser_.error_location)));         
    }
    if(parser_.parse_tree==nullptr){
        // emtry statment;
        return;;
    }
    Binder binder(parser_.parse_tree,cata_log_);
    bool is_success= false;
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    SchemaRef schema{nullptr};
    
    std::chrono::time_point<std::chrono::system_clock> before = std::chrono::system_clock::now();
    auto print_result=[&,this](std::vector<ChunkRef>&& chunks){
        std::chrono::time_point<std::chrono::system_clock> later = std::chrono::system_clock::now();
        auto second = std::chrono::duration_cast<std::chrono::microseconds>(later - before);
        uint32_t total_rows = 0;
        uint32_t chunk_num = chunks.size();
        for(auto& c:chunks){
            total_rows+=c->rows();
        }
        writer_.ss.str("");
        if(chunks.empty()){
            writer_.ss<<"empty set"<<std::endl;
            writer_.ss<<total_rows << " record  in: "<< (double)((double)second.count() / (double)1000000.00000 )<<" second "<<std::endl;
            result = std::move(writer_.ss.str());
            return ;
        }
        if(chunks[0]->rows()==0){
            writer_.ss<<"empty set"<<std::endl;
            writer_.ss<<total_rows << " record  in: "<< (double)((double)second.count() / (double)1000000.00000 )<<" second"<<std::endl;
            result = std::move(writer_.ss.str());
            return;
        }
        writer_.printAll(std::move(chunks));
        chunks.clear();
        writer_.ss<<total_rows << " record  in: "<< (double)((double)second.count() / (double)1000000.00000 )<<" second "<<chunk_num<<" chunk"<<std::endl;
        result = std::move(writer_.ss.str());

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
            case StatementType::EXPLAIN_STATEMENT:{
                ExecuteExplainStatement(std::move(bound_statement),result);
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
    planer.CreatePlan(std::move(stmt));
    //optimer .
    Optimizer optimizer;
    auto optimerzed_plan  = optimizer.RegularOptimize(planer.plan_);
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
    planer.CreatePlan(std::move(stmt));
    Optimizer optimizer;
    auto op_p =optimizer.RegularOptimize(planer.plan_);

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
void StardDataBase::ExecuteExplainStatement(std::unique_ptr<BoundStatement> stmt,std::string& result){
    std::stringstream ss;
    auto* explain_stmt = down_cast<ExplainStatement*>(stmt.get());
    Planer planer(cata_log_);
    planer.CreatePlan(std::move(explain_stmt->stmt_));
    ss<<"==== Plan   Later ===="<<std::endl;
    ss<<planer.ShowPlanTree(planer.plan_);
    ss<<"==== Optimer Later ===="<<std::endl;
    Optimizer optimizer;
    auto op_p =optimizer.RegularOptimize(planer.plan_);
    ss<<planer.ShowPlanTree(op_p);
    auto context = std::make_shared<ExecuteContext>(cata_log_);
    execute_engine_->ExecuteExplain(op_p,context,ss);
    result = std::move(ss.str());
}