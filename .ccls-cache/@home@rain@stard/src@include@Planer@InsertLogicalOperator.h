#pragma once
#include "Expressions/LogicalExpression.h"
#include "Planer/LogicalOperator.h"
#include "common/type.h"


class InsertLogicalOperator : public LogicalOperator{
    static constexpr const OperatorType type_ = OperatorType::InsertOperatorNode;
public:

    InsertLogicalOperator(
        std::vector<LogicalOperatorRef> children,
        InsertStatementType type,table_oid_t inserted_table)
        :LogicalOperator(std::move(children)),
        insert_type_(type),
        inserted_table_(inserted_table){}
    virtual OperatorType GetType() override{
        return type_;
    }
    
    
    COPY_PLAN_WITH_CHILDREN(InsertLogicalOperator)
    table_oid_t inserted_table_{UINT32_MAX};
    InsertStatementType insert_type_;
};