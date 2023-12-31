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

    std::unique_ptr<BoundExpression> Copy() override{
        auto r  = std::make_unique<BoundBinaryOp>(operator_name_,larg_->Copy(),rarg_->Copy());
        r ->alias_ = alias_;
        return r;

    }
    
    std::string ToString() const override {
        return larg_->ToString()+operator_name_+rarg_->ToString();
    }
    bool HasAgg() override{
        auto l = larg_->HasAgg();
        auto r = rarg_->HasAgg();
        if(l || r)
            return true;
        return false;
    }
    ColumnType GetReturnType()const override{
        auto l = larg_->GetReturnType();
        auto r = rarg_->GetReturnType();
        if(l != r){
            throw Exception("two type mismatch expression can't do binary operation.");
        }
        return l;
    }
    
    

    std::string operator_name_;
    std::unique_ptr<BoundExpression> larg_;
    std::unique_ptr<BoundExpression> rarg_;
};