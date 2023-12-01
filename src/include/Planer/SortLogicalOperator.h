#pragma once
#include "Planer/LogicalOperator.h"



class SortLogicalOperator:public LogicalOperator{
    static constexpr const OperatorType type_=
        OperatorType::SortOperatorNode;
public:
    SortLogicalOperator(std::vector<LogicalOperatorRef> child,
    SchemaRef schme,SchemaRef table_schame):
        LogicalOperator(std::move(child),std::move(schme),
        std::move(table_schame)){}


    virtual OperatorType GetType() override{
        return  type_;
    }


    COPY_PLAN_WITH_CHILDREN(SortLogicalOperator);
};