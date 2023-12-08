


#pragma once
#include "CataLog/TableCataLog.h"
#include "Expressions/LogicalExpression.h"

class ColumnValueExpression:public LogicalExpression{
     static constexpr const LogicalExpressionType type_=
        LogicalExpressionType::ColumnValueExpr;
public:
   


   void collect_column(std::vector<Column>& cols)override{
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

    std::string toString()override{
        return  column_info_.name_;
    }

private:
    //use in join node. 0 left,1 right
    int left_or_right_;
    uint32_t col_idx_; // idx in table schema.
    Column column_info_;
    bool first_evalute_{true};
    std::vector<ValueUnion> cache_;
};
