


#pragma once
#include "CataLog/TableCataLog.h"
#include "Expressions/LogicalExpression.h"

class ColumnValueExpression:public LogicalExpression{
     static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::ColumnValueExpr;
public:
   

    LogicalExpressionType GetType() override{
        return type_;
    }
   void collect_column(std::vector<Column>& cols)override{
        if(std::find(cols.begin(),cols.end(),column_info_)==cols.end())
            cols.push_back(column_info_);
   }
    explicit ColumnValueExpression(uint32_t col_id,Column col,int left_or_right)
        :LogicalExpression({}),col_idx_(col_id),column_info_(std::move(col)),
        left_or_right_(left_or_right){}
    

    ColumnType GetReturnType() override{
        return column_info_.type_;
    }


    // select ... where colA <coC = colB+ 1;
    virtual ValueUnion Evalute(ChunkRef* chunk, uint32_t idx) override{
        auto col = chunk->get()->getColumnByname(column_info_.name_);
        return col->ValueAt(idx);
    }
    virtual ValueUnion EvaluteJoin(ChunkRef* l_chunk,ChunkRef* r_chunk,
        uint32_t l_idx,uint32_t r_idx) override{
            
        auto l_col = l_chunk->get()->getColumnByname(column_info_.name_);
        auto r_col = r_chunk->get()->getColumnByname(column_info_.name_);
        if(l_col && r_col){
            throw Exception("hash join ,left child and right child have same col name?");
        }
        if(l_col){
            return l_col->ValueAt(l_idx);
        }else if(r_col){
            return  r_col->ValueAt(r_idx);
        }
        UNREACHABLE;
    }
    std::string toString()override{
        return  column_info_.name_;
    }

    const Column& column_info(){
        return  column_info_;
    }
private:
    //use in join node. 0 left,1 right
    int left_or_right_;
    uint32_t col_idx_; // idx in table schema.
    Column column_info_;
    bool first_evalute_{true};
    std::vector<ValueUnion> cache_;
};
