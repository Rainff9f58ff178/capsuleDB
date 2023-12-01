#pragma once
#include "Binder/BoundTabRef.h"





class BoundCTETable:public BoundTabRef{
public:

    explicit BoundCTETable(std::string cte_name,
        std::string alias):
            BoundTabRef(TableReferenceType::CTE),
            cte_name_(std::move(cte_name)),
            alias_(std::move(alias)){}

    BoundCTETable() =default;
    ~BoundCTETable()= default;



    std::string cte_name_;
    std::string alias_;

};
