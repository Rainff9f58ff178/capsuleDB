#pragma once
#include "CataLog/Schema.h"
#include "Expressions/LogicalExpression.h"
#include "Planer/LogicalOperator.h"

/*
this Node should be the topest node.use to materilize Tuple
*/
class MaterilizeLogicaloperator:public LogicalOperator{
public:
    static constexpr const OperatorType type_= OperatorType::MaterilizeOperatorNode;

    MaterilizeLogicaloperator(
        std::vector<LogicalOperatorRef> children):
            LogicalOperator(children){
                
            }

    ~MaterilizeLogicaloperator()=default;
    virtual OperatorType GetType() override{
        return type_;
    }        
    COPY_PLAN_WITH_CHILDREN(MaterilizeLogicaloperator);



};