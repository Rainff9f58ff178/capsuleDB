#pragma once

/*
    
*/
#include "Planer/LogicalOperator.h"
#include "Expressions/LogicalExpression.h"

class Optimizer{
public:
    Optimizer()=default;
    ~Optimizer()=default;


    LogicalOperatorRef RegularOptimize(LogicalOperatorRef plan){
        auto _p = FilterPushDown(plan);
        return _p;
    }

    LogicalOperatorRef 
    OptimizerInsert(LogicalOperatorRef plan);

    LogicalOperatorRef
    OptimizerInsertInternal(LogicalOperatorRef plan);

    LogicalOperatorRef 
    FilterPushDown(LogicalOperatorRef plan);

   
};