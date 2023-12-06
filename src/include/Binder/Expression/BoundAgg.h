#pragma once

#include "Binder/BoundExpression.h"






class BoundAgg:public BoundExpression{
public:

    explicit BoundAgg(std::string agg_name,bool is_distinct,
        std::unique_ptr<BoundExpression> args):
        BoundExpression(ExpressionType::AGG_CALL),
        agg_name_(std::move(agg_name)),
        is_distinct_(is_distinct),
        args_(std::move(args)){}
    
    BoundAgg()=default;
    ~BoundAgg()=default;

    std::unique_ptr<BoundExpression> Copy()override{
        return std::make_unique<BoundAgg>(agg_name_,is_distinct_,args_->Copy());
    }
    std::string agg_name_;
    bool is_distinct_;
    std::unique_ptr<BoundExpression> args_;
};