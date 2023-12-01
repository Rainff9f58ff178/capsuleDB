#pragma once
#include "Binder/BoundExpression.h"





class BoundStar:public BoundExpression{
public:
    explicit BoundStar():BoundExpression(ExpressionType::STAR){}
    ~BoundStar()=default;

};