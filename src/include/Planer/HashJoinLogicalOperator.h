#pragma once
#include "Planer/LogicalOperator.h"
#include "Expressions/LogicalExpression.h"
#include "Binder/TableRef/BoundJoinTable.h"



class HashJoinLogicalOperator:public LogicalOperator{


    static constexpr const OperatorType type_ = OperatorType::HashJoinOperatorNode;
public:
    
    HashJoinLogicalOperator(std::vector<LogicalOperatorRef> children,
    LogicalExpressionRef condition,JoinType type):LogicalOperator(std::move(children)),
        condition_(std::move(condition)),j_type_(type){

        
    }


    COPY_PLAN_WITH_CHILDREN(HashJoinLogicalOperator);
    OperatorType GetType() override{
        return type_;
    }


    void PrintDebug() override{
        LogicalOperator::PrintDebug();
        std::cout<<" condition :"<<condition_->toString();
    }

    LogicalExpressionRef condition_;
    JoinType j_type_;
    std::vector<LogicalExpressionRef> pridicators_; //  pushed down filter.
};
