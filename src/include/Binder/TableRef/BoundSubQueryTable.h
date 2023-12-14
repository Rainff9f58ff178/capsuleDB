#pragma once
#include "Binder/BoundTabRef.h"
#include "Binder/BoundStatement.h"
#include <string>

#include "CataLog/Schema.h"
#include "Binder/Statement/SelectStatement.h"



class BoundSubQueryTable:public BoundTabRef{
public:
    explicit BoundSubQueryTable(
        SchemaRef select_list,
        std::unique_ptr<SelectStatement> sub_query_statement,
        std::string alias
    ):BoundTabRef(TableReferenceType::SUBQUERY),
    select_list_(std::move(select_list)),
    sub_query_statement_(std::move(sub_query_statement)),
    alias_(std::move(alias)){}

    BoundSubQueryTable()=default;


    std::unique_ptr<SelectStatement> sub_query_statement_;
    
    SchemaRef select_list_;
    std::string alias_;
};


