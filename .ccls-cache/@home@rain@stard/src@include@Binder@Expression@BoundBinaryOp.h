#pragma once

#include "Binder/BoundExpression.h"







class BoundBinaryOp:public BoundExpression{

public:
    explicit BoundBinaryOp(std::string name,
        std::unique_ptr<BoundExpression> larg,
        std::unique_ptr<BoundExpression> rarg):
            BoundExpression(ExpressionType::BINARY_OP),
            larg_(std::move(larg)),
            rarg_(std::move(rarg)),
            operator_name_(std::move(name)){}

    BoundBinaryOp()=default;
    ~BoundBinaryOp()=default;



    std::string operator_name_;
    std::unique_ptr<BoundExpression> larg_;
    std::unique_ptr<BoundExpression> rarg_;
};