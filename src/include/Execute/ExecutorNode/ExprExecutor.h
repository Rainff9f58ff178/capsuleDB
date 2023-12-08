#pragma once
#include "Expressions/LogicalExpression.h"




class ExprExecutor{
public:
    ExprExecutor(LogicalExpressionRef& expr,ChunkRef& ref):expr_(expr),chunk_(ref){

    }

    ColumnRef execute(const std::string& col);

    LogicalExpressionRef expr_;
    ChunkRef chunk_;
};