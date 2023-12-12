

#pragma once
#include "Binder/BoundExpression.h"
#include "Binder/BoundStatement.h"
#include "CataLog/CataLog.h"
#include "CataLog/Column.h"
#include "nodes/nodes.hpp"
#include "nodes/parsenodes.hpp"
#include "nodes/pg_list.hpp"
#include "postgres_parser.hpp"
#include <memory>
#include <vector>
#include "Tableheap/TableHeap.h"
#include <format>
#include "Binder/Expression/BoundAgg.h"
#include "Binder/Expression/BoundAlias.h"
#include "Binder/Expression/BoundConstant.h"
#include "Binder/Expression/BoundColumnRef.h"
#include "Binder/Expression/BoundUnaryOp.h"
#include "Binder/Expression/BoundBinaryOp.h"
#include "Binder/Expression/BoundStar.h"
#include "Binder/BoundOrderBy.h"
#include "Binder/Statement/CreateStatement.h"
#include "Binder/Statement/InsertStatement.h"
#include "Binder/Statement/SelectStatement.h"
#include "Binder/TableRef/BoundBaseTable.h"
#include "Binder/TableRef/BoundCrossTable.h"
#include "Binder/TableRef/BoundCTETable.h"
#include "Binder/TableRef/BoundExpressionList.h"
#include "Binder/TableRef/BoundJoinTable.h"
#include "Binder/TableRef/BoundSubQueryTable.h"
#include "Binder/Statement/ExplainStatement.h"

class Binder{
public:
    Binder(duckdb_libpgquery::PGList* parsed_tree,CataLog* cata_log);

    std::unique_ptr<BoundStatement>
    BindStatement(duckdb_libpgquery::PGNode* stmt);

    std::unique_ptr<CreateStatement>
    BindCreate(duckdb_libpgquery::PGCreateStmt* stmt);

    std::unique_ptr<InsertStatement>
    BindInsert(duckdb_libpgquery::PGInsertStmt* stmt);

    std::unique_ptr<SelectStatement>
    BindSelect(duckdb_libpgquery::PGSelectStmt* stmt);
    
    std::unique_ptr<ExplainStatement>
    BindExplain(duckdb_libpgquery::PGExplainStmt* stmt);


    std::unique_ptr<BoundTabRef>
    BindFrom(duckdb_libpgquery::PGList* from);

    std::unique_ptr<BoundTabRef>
    BindTable(duckdb_libpgquery::PGNode* node);

    std::unique_ptr<BoundTabRef>
    BindRangeVar(duckdb_libpgquery::PGRangeVar* range_var);

    std::unique_ptr<BoundTabRef>
    BindJoinExpr(duckdb_libpgquery::PGJoinExpr* jon_expr);

    std::unique_ptr<BoundTabRef>
    BindRangeSubSelect(duckdb_libpgquery::PGRangeSubselect* sub_query);

    std::unique_ptr<BoundTabRef>
    BindSubSelect(duckdb_libpgquery::PGSelectStmt* stmt,
    std::string alias);

    std::unique_ptr<BoundExpression>
    BindWhere(duckdb_libpgquery::PGNode* node);


    std::unique_ptr<BoundExpressionList>
    BindValueList(duckdb_libpgquery::PGList* list);

    std::vector<std::unique_ptr<BoundExpression>>
    BindExpressionLists(duckdb_libpgquery::PGList* list);

    std::vector<std::unique_ptr<BoundExpression>>
    BindSelectList(duckdb_libpgquery::PGList* list);
    
    std::unique_ptr<BoundExpression>
    BindExpression(duckdb_libpgquery::PGNode* node);

    std::unique_ptr<BoundExpression>
    BindConstant(duckdb_libpgquery::PGAConst* node);

    auto ResolveColumn(const BoundTabRef &scope,std::vector<std::string> &col_name)
    -> std::unique_ptr<BoundExpression>;

    // this is BindCOolDefiniation ,used in BindCreate
    ColumnDef
    BindColumnDef(duckdb_libpgquery::PGColumnDef* col_def);

    std::unique_ptr<BoundExpression>
    ResolveColumnInternal(const BoundTabRef& scope,std::vector<std::string>& col_names);
    /*
        this fucntion used in Bind colref in sql.

        select colA from test_1; 
        colA is a BoundColumnRef;
    */
    std::unique_ptr<BoundExpression>
    BindColumnRef(duckdb_libpgquery::PGColumnRef* col_ref);

    std::unique_ptr<BoundExpression>
    BindStarRef(duckdb_libpgquery::PGAStar* star);

    std::unique_ptr<BoundExpression>
    BindFuncCall(duckdb_libpgquery::PGFuncCall* func);

    std::unique_ptr<BoundExpression>
    BindResTarget(duckdb_libpgquery::PGResTarget* target);

    std::unique_ptr<BoundExpression>
    BindExpr(duckdb_libpgquery::PGAExpr* expr);
    
    std::unique_ptr<BoundExpression>
    BindBoolExpr(duckdb_libpgquery::PGBoolExpr* bool_expr);
    
    std::vector<std::unique_ptr<BoundExpression>>
    BindGroupBy(duckdb_libpgquery::PGList* group_list);

    std::unique_ptr<BoundExpression>
    BindHaving(duckdb_libpgquery::PGNode* ndoe);

    std::unique_ptr<BoundExpression>
    BindLimitCount(duckdb_libpgquery::PGNode* node);
    std::unique_ptr<BoundExpression>
    BindLimitOffset(duckdb_libpgquery::PGNode* node);

    std::vector<std::unique_ptr<BoundOrderBy>>
    BindSort(duckdb_libpgquery::PGList* node);

    std::unique_ptr<BoundBaseTableRef>
    BindBaseTable(std::string table_name,std::optional<std::string> alias);
    


    //Get all ColumnExpression of this table.
    std::vector<std::unique_ptr<BoundExpression>>
    GetAllColumnExpr(BoundTabRef* table);
    std::vector<duckdb_libpgquery::PGNode*> statments_;
    
    CataLog* cata_log_;

    BoundTabRef* scope_{nullptr};
private:
    uint32_t universe_id_{0};
};