#pragma once
#include "Binder/BoundExpression.h"






class BoundUnaryOp:public BoundExpression{
public:
    
    explicit BoundUnaryOp(std::string name,
        std::unique_ptr<BoundExpression> args):
        BoundExpression(ExpressionType::UNARY_OP),
        operator_name_(std::move(name)),
        args_(std::move(args)){}
    
    BoundUnaryOp()=default;
    ~BoundUnaryOp()=default;


    std::string operator_name_;
    std::unique_ptr<BoundExpression> args_;

};





