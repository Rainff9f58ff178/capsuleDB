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
        return std::make_unique<BoundStar>();
    }
    bool HasAgg() override{
        return false;
    }
    ColumnType GetReturnType() const override{
        UNREACHABLE;
    }
};