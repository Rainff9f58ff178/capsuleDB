#pragma once

#include "Expressions/LogicalExpression.h"



class ConstantValueExpression:public LogicalExpression{
    static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::ConstantExpr;
public:
    explicit ConstantValueExpression(ValueUnion&& val)
        :LogicalExpression({}),
        val_(std::move(val)){}

    ~ConstantValueExpression() = default;
  
    virtual ValueUnion Evalute(ChunkRef* chunk,uint32_t idx) override{
        return val_;
    }


    virtual void PrintDebug() override{
        std::cout<<"ConstantvalueExpr ";
    }
private:
    ValueUnion val_;
};