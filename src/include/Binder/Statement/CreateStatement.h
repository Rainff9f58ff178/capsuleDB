#pragma once
#include "Binder/BoundStatement.h"
#include "CataLog/Column.h"
#include "Tableheap/TableHeap.h"




class CreateStatement:public BoundStatement{
public:

    explicit CreateStatement(std::string table_name,
        std::vector<ColumnDef> cols):
            BoundStatement(StatementType::CREATE_STATEMENT),
            table_name_(std::move(table_name)),
            columns_(std::move(cols)){}

    
    CreateStatement()=default;


    std::string table_name_;
    std::vector<ColumnDef> columns_;

};