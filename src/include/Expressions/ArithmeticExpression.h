#pragma once

#include "Expressions/LogicalExpression.h"

enum ArithmeticType { Invalid=0,Add,Minus};

class ArithmeticExpression:public LogicalExpression{
     static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::ArithmeticExpr;
public:
   
    ArithmeticExpression(std::vector<LogicalExpressionRef> child,ArithmeticType type):LogicalExpression(std::move(child)),a_type_(type){}



    LogicalExpressionType GetType() override{
        return type_;
    }
    virtual ValueUnion Evalute(ChunkRef* chunk,uint32_t idx) override{
        auto l_val = children_[0]->Evalute(chunk,idx);
        auto r_val = children_[1]->Evalute(chunk,idx);
        if(a_type_ == ArithmeticType::Add){
            return l_val+r_val;
        }else if(a_type_ == ArithmeticType::Minus){
            return l_val-r_val;
        }
        return ValueUnion(0);
    }


    std::string toString() override{
        auto left = children_[0]->toString();
        auto op =" ";
        if(a_type_ == ArithmeticType::Add){
            op = " + ";
        }else if(a_type_ == ArithmeticType::Minus){
            op = " - ";
        }
        auto right = children_[1]->toString();

        return left.append(op).append(right);
    }

    ColumnType GetReturnType() override{
        auto l_type = children_[0]->GetReturnType();
        auto r_type = children_[1]->GetReturnType();
        if(l_type!=r_type)
            throw Exception("You can't do Arithmetic in two difference expression");
        
        return l_type;
    }

private:
    ArithmeticType a_type_{Invalid};
};