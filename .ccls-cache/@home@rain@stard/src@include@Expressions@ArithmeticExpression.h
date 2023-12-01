#pragma once

#include "Expressions/LogicalExpression.h"

enum ArithmeticType { Invalid=0,Add,Minus};

class ArithmeticExpression:public LogicalExpression{
     static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::ArithmeticExpr;
public:
   
    virtual ValueUnion Evalute(DataChunk* chunk, uint32_t data_idx_in_this_chunk) override{

    }
    virtual bool EvaluteJoin(DataChunk* left_chunk, DataChunk* right_chunk, uint32_t data_idx_in_this_chun) override{

    }
    virtual ValueUnion Evalute(std::vector<bool>& bitmap, TableCataLog *table) override{
        throw NotImplementedException("Not imp");
    }
    virtual void PrintDebug() override{
        std::cout<<"Aritehmetic Expr ";
        LogicalExpression::PrintDebug();
    }
private:
    ArithmeticType a_type_{Invalid};
};