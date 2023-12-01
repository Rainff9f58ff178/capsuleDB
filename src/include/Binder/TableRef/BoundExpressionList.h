#pragma once

#include "Binder/BoundTabRef.h"
#include "Binder/BoundExpression.h"
#include "nodes/primnodes.hpp"



// insert into table values(1,2),(2,3),this si values clause






class BoundExpressionList:public BoundTabRef{
public:

    explicit BoundExpressionList(
        std::vector<std::vector<std::unique_ptr<BoundExpression>>> values,
        std::string identifier
    ):BoundTabRef(TableReferenceType::EXPRESSION_LIST),
        values_(std::move(values)),
        identifier_(std::move(identifier)){}
    
    explicit BoundExpressionList(
        std::vector<std::vector<std::unique_ptr<BoundExpression>>> values
    ):BoundTabRef(TableReferenceType::EXPRESSION_LIST),
        values_(std::move(values)){}
    
    BoundExpressionList()=default;
    ~BoundExpressionList()= default;


    std::vector<std::vector<std::unique_ptr<BoundExpression>>> 
        values_;
    
    std::string identifier_;
};


