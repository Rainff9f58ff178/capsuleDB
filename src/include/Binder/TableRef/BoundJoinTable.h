

#pragma once



#include "Binder/BoundTabRef.h"
#include "Binder/BoundExpression.h"
#include <concepts>
enum JoinType{
    INVALID=0,
    INNER,
    LEFT,
    RIGHT,
    OUTER
};


class BoundJoinTable:public BoundTabRef{
public:

    explicit BoundJoinTable(JoinType jtype,
        std::unique_ptr<BoundTabRef> l_table,
        std::unique_ptr<BoundTabRef> r_table,
        std::unique_ptr<BoundExpression> condition):
            BoundTabRef(TableReferenceType::JOIN),
            j_type_(jtype),
            l_table_(std::move(l_table)),
            r_table_(std::move(r_table)),
            condition_(std::move(condition)){}

    BoundJoinTable()=default;
    ~BoundJoinTable()=default;


    JoinType j_type_{INVALID};
    std::unique_ptr<BoundTabRef> l_table_;
    std::unique_ptr<BoundTabRef> r_table_;
    std::unique_ptr<BoundExpression> condition_;

};