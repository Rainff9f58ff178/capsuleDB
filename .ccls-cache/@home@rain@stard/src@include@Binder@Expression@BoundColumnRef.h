#pragma once

#include "Binder/BoundExpression.h"
#include "common/commonfunc.h"
#include <format>
#include<vector>
#include<string>


class BoundColumnRef:public BoundExpression{

public:
    explicit BoundColumnRef(std::vector<std::string> column):
        BoundExpression(ExpressionType::COLUMN_REF),
        column_(std::move(column)){
            DASSERT(column_.size() >=2);
        };
    BoundColumnRef()=default;
    ~BoundColumnRef()=default;

    inline const std::string ToString()const {
        return std::format("{}",join(column_,"."));
    }
    inline std::string table_name() const{
        return column_[0];
    }

    std::vector<std::string> column_;
};