#pragma once



#include"common/type.h"
#include<vector>
#include"Binder/BoundExpression.h"
class BoundOrderBy{
public:
    BoundOrderBy(OrderByType type,std::unique_ptr<BoundExpression> exprs):order_type_(type),
    expr_(std::move(exprs)){

    }

    OrderByType order_type_;
    std::unique_ptr<BoundExpression> expr_;

};