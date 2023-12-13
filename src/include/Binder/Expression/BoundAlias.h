#pragma once

#include "Binder/BoundExpression.h"
#include <memory>
#include <string>






class BoundAlias:public BoundExpression{
public:
    explicit BoundAlias(std::unique_ptr<BoundExpression> child,
        std::string alias):BoundExpression(ExpressionType::ALIAS),
        alias_(std::move(alias)),
        child_expression_(std::move(child)){
            throw Exception("depricate");
        }
    BoundAlias()=default;
    ~BoundAlias()=default;


    std::unique_ptr<BoundExpression> Copy() override{
        return std::make_unique<BoundAlias>(child_expression_->Copy(),alias_);
    }
    std::string ToString() const override{
        throw Exception("depricate");
    }

    

    std::string alias_;
    std::unique_ptr<BoundExpression> child_expression_;

};