#pragma once

#include "Binder/BoundExpression.h"
#include <memory>
#include <string>






class BoundAlias:public BoundExpression{
public:
    explicit BoundAlias(std::unique_ptr<BoundExpression> child,
        std::string alias):BoundExpression(ExpressionType::ALIAS),
        alias_(std::move(alias)),
        child_expression_(std::move(child)){}
    BoundAlias()=default;
    ~BoundAlias()=default;






    std::string alias_;
    std::unique_ptr<BoundExpression> child_expression_;

};