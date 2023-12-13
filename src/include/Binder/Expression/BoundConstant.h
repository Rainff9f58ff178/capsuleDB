#pragma once

#include"Binder/BoundExpression.h"
#include "nodes/nodeFuncs.hpp"
#include "common/type.h"
#include "common/Value.h"
//only support int for now;

class BoundConstant : public BoundExpression{
public:
    explicit BoundConstant(ValueUnion&& val):
        BoundExpression(ExpressionType::CONSTANT),value_(std::move(val)){

        }

    BoundConstant():BoundExpression(ExpressionType::CONSTANT),value_(0){
        
    }
    ~BoundConstant()=default;

    std::unique_ptr<BoundExpression> Copy()override{
        return std::make_unique<BoundConstant>(std::move(value_.clone()));
    }
    std::string ToString() const override{
        if(value_.type_==ValueType::TypeInt){
            return std::to_string(value_.val_.num_);
        }else if(value_.type_ == ValueType::TypeString){
            return std::string(value_.val_.data_,value_.value_len_);
        }
        UNREACHABLE;
    }
    bool HasAgg() override{
        return false;
    }

    ValueUnion value_;
};