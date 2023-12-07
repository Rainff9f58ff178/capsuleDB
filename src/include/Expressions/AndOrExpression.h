#pragma once
#include "Expressions/LogicalExpression.h"

enum LogicType {Invalied=0,And,Or};

class AndOrExpression:public LogicalExpression{
     static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::AndOrExpr;
public:


    virtual ValueUnion Evalute(ChunkRef* chunk, uint32_t idx) override{
        
    }




    virtual void PrintDebug() override{
        std::cout<<"AndOr Expr ";
        LogicalExpression::PrintDebug();
    }
private:
    LogicType l_type_{Invalied};
};