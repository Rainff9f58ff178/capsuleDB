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

    ValueUnion value_;
};