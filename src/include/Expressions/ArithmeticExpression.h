#pragma once

#include "Expressions/LogicalExpression.h"

enum ArithmeticType { Invalid=0,Add,Minus};

class ArithmeticExpression:public LogicalExpression{
     static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::ArithmeticExpr;
public:
   
    virtual ValueUnion Evalute(Chunk* chunk,Chunk* new_chunk) override{

    }

    virtual void PrintDebug() override{
        std::cout<<"Aritehmetic Expr ";
        LogicalExpression::PrintDebug();
    }
private:
    ArithmeticType a_type_{Invalid};
};