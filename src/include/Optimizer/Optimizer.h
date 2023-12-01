#pragma once

/*
    
*/
#include "Planer/LogicalOperator.h"
#include "Expressions/LogicalExpression.h"

class Optimizer{
public:
    Optimizer()=default;
    ~Optimizer()=default;


    /*
        this function optimize when the root node is 
    Insert Node.
        if the Insert is Insert values statment,it should 
    eliminate the Materilize node,Insert Get Values from 
    his Values Child Node .
        if The insert is Insert select statment,it should 
    not eliminate Materilize node ,insert node get all 
    row that need inserted node from that.
    */
    LogicalOperatorRef 
    OptimizerInsert(LogicalOperatorRef plan);

    LogicalOperatorRef
    OptimizerInsertInternal(LogicalOperatorRef plan);

    LogicalOperatorRef 
    FilterPridicatorDownPushToSeqScanNode(LogicalOperatorRef plan);

   
};