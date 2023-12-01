#pragma once


#include "Expressions/LogicalExpression.h"
#include "Planer/LogicalOperator.h"



class ValuesLogicalOperator:public LogicalOperator{
public:
    static constexpr const OperatorType type_ = OperatorType::ValuesOperatorNode;
    ValuesLogicalOperator(std::vector<std::vector<LogicalExpressionRef>>
        all_values)
        :LogicalOperator({}),
        all_values_(std::move(all_values)){}

    virtual OperatorType GetType() override{
        return type_;
    }

    


    COPY_PLAN_WITH_CHILDREN(ValuesLogicalOperator);
    std::vector<std::vector<LogicalExpressionRef>> all_values_;
};