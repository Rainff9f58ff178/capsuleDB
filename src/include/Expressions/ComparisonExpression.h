#pragma once

#include "Expressions/LogicalExpression.h"


enum ComparisonType {GreaterThan,LesserThan,GreaterEqualThan,LesserEquanThan,Equal};

class ComparisonExpression:public LogicalExpression{
    static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::ComparsionExpr;
public:

    
    ComparisonExpression(std::vector<LogicalExpressionRef> child,ComparisonType type)
        :LogicalExpression(std::move(child)),cp_type_(type){}
    
    virtual ValueUnion Evalute(Chunk* chunk, Chunk* new_chunk) override{
        
    }

    virtual void PrintDebug() override{
        std::cout<<"ComparisonExpr ";
        LogicalExpression::PrintDebug();
    }
private:
    ComparisonType cp_type_;

};