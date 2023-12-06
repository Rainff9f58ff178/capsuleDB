


#pragma once
#include "CataLog/TableCataLog.h"
#include "Expressions/LogicalExpression.h"

class ColumnValueExpression:public LogicalExpression{
     static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::ColumnValueExpr;
public:
   
    explicit ColumnValueExpression(uint32_t col_id,std::string col_name,int left_or_right)
        :LogicalExpression({}),col_idx_(col_id),col_name_(std::move(col_name)),
        left_or_right_(left_or_right){}
    
    virtual ValueUnion Evalute(DataChunk* chunk, uint32_t data_idx_in_this_chunk) override{

    }
    virtual bool EvaluteJoin(DataChunk* left_chunk, DataChunk* right_chunk, uint32_t data_idx_in_this_chun) override{

    }


    virtual ValueUnion Evalute(std::vector<bool>& bitmap, TableCataLog *table) override{
        if(first_evalute_){
            cache_ = table->GetDataChunk(col_idx_);
            assert(bitmap.size() == cache_.size());
            first_evalute_=false;
        }

        return cache_[offset_++];
    }
    virtual void PrintDebug() override{
        std::cout<<"ColunmValueExpr ";
    }
private:
    //use in join node. 0 left,1 right
    int left_or_right_;
    /*
        this is the idx in targetlist
    select colA,colB from test_1 where colC=1;
    */
    uint32_t col_idx_;
    std::string col_name_;
    bool first_evalute_{true};
    std::vector<ValueUnion> cache_;
    uint32_t offset_{0};
};
