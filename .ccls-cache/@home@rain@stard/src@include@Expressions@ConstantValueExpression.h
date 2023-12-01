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
  
    virtual ValueUnion Evalute(DataChunk* chunk, uint32_t data_idx_in_this_chunk) override{
        return val_;
    }
    virtual bool EvaluteJoin(DataChunk* left_chunk, DataChunk* right_chunk, uint32_t data_idx_in_this_chun) override{
        return false;
    }

    virtual ValueUnion Evalute(std::vector<bool>& bitmap, TableCataLog *table) override{
        return val_;
    }
    virtual void PrintDebug() override{
        std::cout<<"ConstantvalueExpr ";
    }
private:
    ValueUnion val_;
};