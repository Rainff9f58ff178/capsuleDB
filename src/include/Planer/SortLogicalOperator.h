#pragma once
#include "Planer/LogicalOperator.h"
#include "Expressions/LogicalExpression.h"


class SortLogicalOperator:public LogicalOperator{
    static constexpr const OperatorType type_=
        OperatorType::SortOperatorNode;
public:
    SortLogicalOperator(std::vector<LogicalOperatorRef> child,
    std::vector<std::pair<OrderByType,LogicalExpressionRef>> order_by_exprs):order_bys_(std::move(order_by_exprs)),
        LogicalOperator(std::move(child)){}


    virtual OperatorType GetType() override{
        return  type_;
    }


    void PrintDebug() override{
        LogicalOperator::PrintDebug();
        std::cout<<" sort: ";
        for(auto& order_by : order_bys_){
            std::cout<<order_by.second->toString()<<" "<< (order_by.first==OrderByType::ASC ? "ASC" : "DESC");
        }
    }
    std::vector<std::pair<OrderByType,LogicalExpressionRef>> order_bys_;
    COPY_PLAN_WITH_CHILDREN(SortLogicalOperator);
};