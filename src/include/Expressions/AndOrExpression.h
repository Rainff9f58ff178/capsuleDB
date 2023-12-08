#pragma once
#include "Expressions/LogicalExpression.h"

enum LogicType {Invalied=0,And,Or};

class AndOrExpression:public LogicalExpression{
     static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::AndOrExpr;
public:


    virtual ValueUnion Evalute(ChunkRef* chunk, uint32_t idx) override{
        NOT_IMP;
    }


    std::string toString() override{
        auto left = children_[0]->toString();
        auto op = " ";
        if(l_type_ == LogicType::And){
            op = " and ";
        }else if(l_type_ == LogicType::Or){
            op = " or ";
        }
        auto right = children_[1]->toString();
        return  left.append(op).append(right);
    }

    ColumnType GetReturnType() override{
        return ColumnType::INT;
    }
private:
    LogicType l_type_{Invalied};
};