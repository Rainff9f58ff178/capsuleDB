#pragma once
#include "Planer/LogicalOperator.h"
#include "Expressions/LogicalExpression.h"
class FilterLogicalOperator:public LogicalOperator{
public:
    static constexpr const OperatorType type_ =
         OperatorType::FilterOperatorNode;
        
    
    FilterLogicalOperator(
    std::vector<LogicalOperatorRef> child,
    LogicalExpressionRef pridicator):
    LogicalOperator(std::move(child)),
    pridicator_(std::move(pridicator)){}

    
    COPY_PLAN_WITH_CHILDREN(FilterLogicalOperator)
    
    virtual OperatorType GetType() override{
        return type_;
    }
    LogicalExpressionRef pridicator_;
};
