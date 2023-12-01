#pragma once

#include "Expressions/LogicalExpression.h"


enum ComparisonType {GreaterThan,LesserThan,GreaterEqualThan,LesserEquanThan,Equal};

class ComparisonExpression:public LogicalExpression{
    static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::ComparsionExpr;
public:

    
    ComparisonExpression(std::vector<LogicalExpressionRef> child,ComparisonType type)
        :LogicalExpression(std::move(child)),cp_type_(type){}
    
    virtual ValueUnion Evalute(DataChunk* chunk, uint32_t data_idx_in_this_chunk) override{

    }
    virtual bool EvaluteJoin(DataChunk* left_chunk, DataChunk* right_chunk, uint32_t data_idx_in_this_chun) override{

    }

    /*
        this fucniton use to get bitmap,bitmap size equal the size of 
    TotalObj ,include deleted obj.
    */
    virtual ValueUnion Evalute(std::vector<bool>& bitmap, TableCataLog *table) override{
        for(uint32_t i=0;i<bitmap.size();++i){
            auto left = children_[0]->Evalute(bitmap,table);
            auto right = children_[1]->Evalute(bitmap,table);
        }
        return 1;
    }
    virtual void PrintDebug() override{
        std::cout<<"ComparisonExpr ";
        LogicalExpression::PrintDebug();
    }
private:
    ComparisonType cp_type_;

};