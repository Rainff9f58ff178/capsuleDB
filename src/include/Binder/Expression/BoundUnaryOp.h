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

    std::unique_ptr<BoundExpression> Copy()override{
        return std::make_unique<BoundUnaryOp>(operator_name_,args_->Copy());
    }

    std::string ToString() const override{
        return  operator_name_+args_->ToString();
    }
    bool HasAgg() override{
        return  args_->HasAgg();
    }
    ColumnType GetReturnType() const override{
        return args_->GetReturnType();
    }
    std::string operator_name_;
    std::unique_ptr<BoundExpression> args_;

};





