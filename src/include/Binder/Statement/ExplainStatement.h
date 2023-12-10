#pragma once
#include "Binder/BoundStatement.h"

#include<memory>

class ExplainStatement : public BoundStatement{
public:
    explicit ExplainStatement( std::unique_ptr<BoundStatement> stmt):BoundStatement(StatementType::EXPLAIN_STATEMENT),stmt_(std::move(stmt)){}
    std::unique_ptr<BoundStatement> stmt_;
};