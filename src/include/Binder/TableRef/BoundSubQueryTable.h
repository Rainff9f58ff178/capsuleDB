#pragma once
#include "Binder/BoundTabRef.h"
#include "Binder/BoundStatement.h"
#include <string>





class BoundSubQueryTable:public BoundTabRef{
public:
    explicit BoundSubQueryTable(
        std::vector<std::vector<std::string>>&& select_list,
        std::unique_ptr<BoundStatement> sub_query_statement,
        std::string alias
    ):BoundTabRef(TableReferenceType::SUBQUERY),
    select_list_(std::move(select_list)),
    sub_query_statement_(std::move(sub_query_statement)),
    alias_(std::move(alias)){}

    BoundSubQueryTable()=default;
    ~BoundSubQueryTable()=default;


    std::unique_ptr<BoundStatement> sub_query_statement_;
    std::vector<std::vector<std::string>> select_list_;
    std::string alias_;
};


