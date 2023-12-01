#pragma once
#include "Binder/BoundTabRef.h"







class BoundCrossTable:public BoundTabRef{
public:
    
    explicit BoundCrossTable(std::unique_ptr<BoundTabRef> l_table,
        std::unique_ptr<BoundTabRef> r_table):
            BoundTabRef(TableReferenceType::CROSS_PRODUCT),
            l_table_(std::move(l_table)),
            r_table_(std::move(r_table)){}


    BoundCrossTable()= default;
    ~BoundCrossTable()= default;




    std::unique_ptr<BoundTabRef> l_table_;
    std::unique_ptr<BoundTabRef> r_table_;

};