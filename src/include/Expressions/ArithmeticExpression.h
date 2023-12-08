#pragma once

#include "Expressions/LogicalExpression.h"

enum ArithmeticType { Invalid=0,Add,Minus};

class ArithmeticExpression:public LogicalExpression{
     static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::ArithmeticExpr;
public:
   
    virtual ValueUnion Evalute(ChunkRef* chunk,uint32_t idx) override{
        NOT_IMP
    }


    std::string toString() override{
        auto left = children_[0]->toString();
        auto op =" ";
        if(a_type_ == ArithmeticType::Add){
            op = " + ";
        }else if(a_type_ == ArithmeticType::Minus){
            op = " - ";
        }
        auto right = children_[0]->toString();

        return left.append(op).append(right);
    }

    ColumnType GetReturnType() override{
        return ColumnType::INT;
    }

private:
    ArithmeticType a_type_{Invalid};
};