#pragma once

#include "Binder/BoundExpression.h"
#include "common/commonfunc.h"
#include <format>
#include<vector>
#include<string>


class BoundColumnRef:public BoundExpression{

public:
    explicit BoundColumnRef(std::vector<std::string> column,ColumnType type):
        BoundExpression(ExpressionType::COLUMN_REF),
        column_(std::move(column)),col_type_(type){
            DASSERT(column_.size() >=2);
        };
    BoundColumnRef()=default;
    ~BoundColumnRef()=default;

     std::string ToString()const  override{
        return std::format("{}",join(column_,"."));
    }
    inline std::string table_name() const{
        return column_[0];
    }
    bool HasAgg() override{
        return false;
    }
    std::unique_ptr<BoundExpression> Copy()override{
        return std::make_unique<BoundColumnRef>(column_,col_type_);
    }
    ColumnType GetReturnType() const override{
        return col_type_;
    }
    std::vector<std::string> column_;
    ColumnType col_type_;
};