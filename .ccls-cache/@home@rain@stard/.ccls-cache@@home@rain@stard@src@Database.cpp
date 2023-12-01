


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
    std::cout<<std::setw(20)<<"table_oid"
        <<std::setw(20)<<"table_name"
        <<std::setw(20)<<"column"<<std::endl;
    for(auto& tb_entry:cata_log_->tables_){
        std::cout<<std::setw(20)<<tb_entry.first;
        auto& tb_catalog = tb_entry.second;
        std::cout<<std::setw(20)<<tb_catalog->table_name_;
        for(auto& col:tb_catalog->schema_.columns_){
            std::cout<<std::setw(20)<<col.name_;
        }
        std::cout<<std::endl;
    }
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
    std::vector<Tuple> result_set;
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    SchemaRef schema{nullptr};
    auto out_put_result = [&]{
        std::chrono::time_point<std::chrono::system_clock> ealapse = std::chrono::system_clock::now();
        auto sec = std::chrono::duration_cast<std::chrono::microseconds>(ealapse - now);
        if(is_success && !result_set.empty()){
            this->writer_.WriteHeader(schema,result_set);
            for(auto& tuple: result_set){
                writer_.WriteRow(tuple,schema);
            }
        }
        std::cout<<"Execute time waste: "<<sec.count()<<" microseconds ealapse"<<std::endl;
    };
    for(auto* stmt : binder.statments_){
        
        auto bound_statement = binder.BindStatement(stmt);
        switch (bound_statement->type_) {
            case StatementType::CREATE_STATEMENT:{
                is_success=ExecuteCreateStatement(std::move(bound_statement));
                if(is_success==false){
                    throw Exception("Just not successful ,that fine");
                }
                schema = std::shared_ptr<Schema>(new Schema());
                schema->columns_.push_back(Column{"__star_database_.create_nums",0});
                Tuple t({1},*schema);
                result_set.push_back(std::move(t));
                out_put_result();
                break;
            }
            case StatementType::INSERT_STATEMENT:{
                is_success=ExecuteInsertStatement(std::move(bound_statement),result_set,schema);

                if(!is_success){
                    throw Exception("Just not successful ,that fine");
                }
                out_put_result();
                break;
            }
            case StatementType::SELECT_STATEMENT:{
                is_success = ExecuteSelectStatement(std::move(bound_statement),
                    result_set,schema);
                
                if(!is_success)
                    throw Exception("Just not successful ,that fine");
                out_put_result();
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
std::vector<Tuple>& result_set,SchemaRef& schema){
    Planer planer(cata_log_);
    std::cout<<"==== Plan   Later ===="<<std::endl;
    planer.CreatePlanAndShowPlanTree(std::move(stmt));

    //optimer .
    Optimizer optimizer;
    auto optimerzed_plan = 
        optimizer.OptimizerInsert(planer.plan_);

    std::cout<<"==== Optimer Later ===="<<std::endl;

    planer.PreOrderTraverse(optimerzed_plan,0);

    
    execute_engine_->Execute(optimerzed_plan,
        std::make_shared<ExecuteContext>(cata_log_,&result_set));

    schema = optimerzed_plan->GetOutPutSchema();    
    return true;
}
bool StardDataBase::ExecuteSelectStatement(
std::unique_ptr<BoundStatement> stmt,
std::vector<Tuple>& result_set,
SchemaRef& schema){
    
    Planer planer(cata_log_);
    std::cout<<"==== Plan   Later ===="<<std::endl;
    planer.CreatePlanAndShowPlanTree(std::move(stmt));


    Optimizer optimizer;
    optimizer.OptimizeSeqScanNodeSelectList(planer.plan_);
    std::cout<<"==== Optimer Later ===="<<std::endl;

    planer.PreOrderTraverse(planer.plan_,0);

    execute_engine_->Execute(planer.plan_,
        std::make_shared<ExecuteContext>(cata_log_,&result_set));

    schema = planer.plan_->GetOutPutSchema();
    return true;
}