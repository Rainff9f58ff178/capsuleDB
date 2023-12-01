
#pragma  once
#include "Binder/BoundStatement.h"
#include "Binder/Statement/SelectStatement.h"
#include "Binder/TableRef/BoundBaseTable.h"
#include <memory>




class InsertStatement:public BoundStatement{
public:

    explicit InsertStatement(
        std::unique_ptr<BoundBaseTableRef> table_inserted,
        std::unique_ptr<SelectStatement> values_inserted,
        InsertStatementType type
    ):BoundStatement(StatementType::INSERT_STATEMENT),
        table_inserted_(std::move(table_inserted)),
        values_inserted_(std::move(values_inserted)),
        insert_type_(type){}

    InsertStatement()=default;
    virtual ~InsertStatement()=default;


    std::unique_ptr<BoundBaseTableRef> table_inserted_;

    std::unique_ptr<SelectStatement> values_inserted_;

    InsertStatementType insert_type_;
};
