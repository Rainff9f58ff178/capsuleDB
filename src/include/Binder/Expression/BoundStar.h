#pragma once
#include "Binder/BoundExpression.h"





class BoundStar:public BoundExpression{
public:
    explicit BoundStar():BoundExpression(ExpressionType::STAR){}
    ~BoundStar()=default;

    std::unique_ptr<BoundExpression> Copy() override{
        return std::make_unique<BoundStar>();
    }
};