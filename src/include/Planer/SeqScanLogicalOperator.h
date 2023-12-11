#pragma once
#include "Planer/LogicalOperator.h"
#include "Expressions/LogicalExpression.h"
class SeqScanLogicalOperator:public LogicalOperator{
    static constexpr const OperatorType type_ = OperatorType::SeqScanOperatorNode;
public:

    SeqScanLogicalOperator(std::string table_name,std::optional<std::string> alias,table_oid_t table_oid,
    std::vector<LogicalOperatorRef >child):
        LogicalOperator(std::move(child)),
        table_name_(std::move(table_name)),alias_name_(std::move(alias)),
        table_oid_(table_oid){}

    virtual OperatorType GetType() override{
        return type_;
    }   
    virtual void PrintDebug() override{
        LogicalOperator::PrintDebug();
        if(!pridicator_.empty()){
            std::cout<<" | filter(preorder) : ";
            for(auto& f : pridicator_){
                std::cout<<f->toString();
            }
        }else 
            std::cout<<" | filter(NONE) ";
    }

   
    COPY_PLAN_WITH_CHILDREN(SeqScanLogicalOperator);

    std::string table_name_;
    table_oid_t table_oid_;

    std::optional<std::string> alias_name_;

    std::vector<column_idx_t> interested_columns_;
    std::vector<LogicalExpressionRef> pridicator_;
};