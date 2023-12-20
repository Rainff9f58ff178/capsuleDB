#pragma once
#include "Binder/BoundExpression.h"





class BoundStar:public BoundExpression{
public:
    explicit BoundStar():BoundExpression(ExpressionType::STAR){}
    ~BoundStar()=default;
    std::string ToString() const override{
        return "*";
    }

    std::unique_ptr<BoundExpression> Copy() override{
        auto r =  std::make_unique<BoundStar>();
        r->alias_ = alias_;
        return r;
    }
    bool HasAgg() override{
        return false;
    }
    ColumnType GetReturnType() const override{
        UNREACHABLE;
    }
};