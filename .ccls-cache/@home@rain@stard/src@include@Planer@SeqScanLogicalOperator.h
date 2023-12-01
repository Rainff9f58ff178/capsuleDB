#pragma once
#include "Planer/LogicalOperator.h"
#include "Expressions/LogicalExpression.h"
class SeqScanLogicalOperator:public LogicalOperator{
    static constexpr const OperatorType type_ = OperatorType::SeqScanOperatorNode;
public:

    SeqScanLogicalOperator(std::string table_name,table_oid_t table_oid,
    std::vector<LogicalOperatorRef >child):
        LogicalOperator(std::move(child)),
        table_name_(std::move(table_name)),
        table_oid_(table_oid){}

    virtual OperatorType GetType() override{
        return type_;
    }   
    virtual void PrintDebug() override{
        LogicalOperator::PrintDebug();
        if(filter_pridicater_){
            std::cout<<" | filter(preorder) : ";
            filter_pridicater_->PrintDebug();
        }else 
            std::cout<<" | filter(NONE) ";
    }

   
    COPY_PLAN_WITH_CHILDREN(SeqScanLogicalOperator);

    std::string table_name_;
    table_oid_t table_oid_;


    // TODO(wxy) :make  filter in Seq sacn node.
    LogicalExpressionRef filter_pridicater_{nullptr};
};