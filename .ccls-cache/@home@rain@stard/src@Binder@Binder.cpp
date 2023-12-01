#include"Binder/Binder.h"
#include "Binder/BoundExpression.h"
#include "Binder/Expression/BoundConstant.h"
#include "Binder/TableRef/BoundBaseTable.h"
#include "common/type.h"
#include "nodes/nodes.hpp"
#include "nodes/pg_list.hpp"
#include <cstddef>
#include <memory>
#include <vector>
#include "nodes/parsenodes.hpp"
#include"common/Exception.h"
#include "CataLog/Column.h"
#include "Binder/Statement/CreateStatement.h"
#include "Binder/Statement/InsertStatement.h"
#include "Binder/Statement/SelectStatement.h"

Binder::Binder(duckdb_libpgquery::PGList* parsed_tree,CataLog* cata_log){
    for(auto entry=parsed_tree->head;entry!=nullptr;entry=entry->next){
        statments_.push_back(
            reinterpret_cast<duckdb_libpgquery::PGNode*>(
                    entry->data.ptr_value));
    }
    cata_log_ = cata_log;
}



std::unique_ptr<BoundStatement> Binder::BindStatement(duckdb_libpgquery::PGNode* stmt){
    switch (stmt->type) {
        case duckdb_libpgquery::T_PGRawStmt:
           return BindStatement(reinterpret_cast<duckdb_libpgquery::PGRawStmt *>(stmt)->stmt);
        case duckdb_libpgquery::T_PGCreateStmt:
            return BindCreate(reinterpret_cast<duckdb_libpgquery::PGCreateStmt *>(stmt));
        case duckdb_libpgquery::T_PGInsertStmt:
            return BindInsert(reinterpret_cast<duckdb_libpgquery::PGInsertStmt *>(stmt));
        case duckdb_libpgquery::T_PGSelectStmt:
            return BindSelect(reinterpret_cast<duckdb_libpgquery::PGSelectStmt *>(stmt));
        // case duckdb_libpgquery::T_PGExplainStmt:
        // return BindExplain(reinterpret_cast<duckdb_libpgquery::PGExplainStmt *>(stmt));
        // case duckdb_libpgquery::T_PGDeleteStmt:
        // return BindDelete(reinterpret_cast<duckdb_libpgquery::PGDeleteStmt *>(stmt));
        // case duckdb_libpgquery::T_PGUpdateStmt:
        // return BindUpdate(reinterpret_cast<duckdb_libpgquery::PGUpdateStmt *>(stmt));
        // case duckdb_libpgquery::T_PGIndexStmt:
        // return BindIndex(reinterpret_cast<duckdb_libpgquery::PGIndexStmt *>(stmt));
        // case duckdb_libpgquery::T_PGVariableSetStmt:
        // return BindVariableSet(reinterpret_cast<duckdb_libpgquery::PGVariableSetStmt *>(stmt));
        // case duckdb_libpgquery::T_PGVariableShowStmt:
        // return BindVariableShow(reinterpret_cast<duckdb_libpgquery::PGVariableShowStmt *>(stmt));
        default:
            throw Exception("Binder not support stmt type for now");
    }

}

ColumnDef
Binder::BindColumnDef(duckdb_libpgquery::PGColumnDef* col_def){
    std::string name;
    if(col_def->colname!=nullptr){
        return ColumnDef(col_def->colname,0,ColumnType::INT);
    }
    throw Exception("Bi`nd columnDef failed");
}
std::unique_ptr<CreateStatement> Binder::BindCreate(duckdb_libpgquery::PGCreateStmt* stmt){
    std::string table_name = stmt->relation->relname;
    std::vector<ColumnDef> columns;
    uint32_t idx=0;
    for(auto entry = stmt->tableElts->head;entry!=nullptr;entry=entry->next,idx++){
        auto node =
             reinterpret_cast<duckdb_libpgquery::PGNode* > (entry->data.ptr_value);
        switch (node->type) {
            case duckdb_libpgquery::T_PGColumnDef:{
                auto column_ref = 
                    reinterpret_cast<duckdb_libpgquery::PGColumnDef*>(entry->data.ptr_value);
                auto column = BindColumnDef(column_ref);
                column.col_idx_ = idx;
                columns.push_back(std::move(column));
                break;
            }
            case duckdb_libpgquery::T_PGConstraint:
                throw NotImplementedException("Not support Constraint for now");
                break;
            default:
                throw NotImplementedException("Column type not support for now");
        }
    }

    if(columns.empty()){
        throw Exception("Columns empty");
    }
    return std::make_unique<CreateStatement>(std::move(table_name),columns);
}


std::unique_ptr<InsertStatement> Binder::BindInsert(duckdb_libpgquery::PGInsertStmt* stmt){
    auto table = BindBaseTable(stmt->relation->relname,std::nullopt);
    auto select_stmt =  BindSelect(reinterpret_cast<duckdb_libpgquery::PGSelectStmt*>(stmt->selectStmt));    
    InsertStatementType type;
    if(reinterpret_cast<duckdb_libpgquery::PGSelectStmt*>(stmt->selectStmt)->valuesLists!=nullptr)
        type=INSERT_VALUES;
    else
        type= INSERT_SELECT;
    return std::unique_ptr<InsertStatement>(new InsertStatement(
        std::move(table),std::move(select_stmt),type));
}

std::unique_ptr<BoundBaseTableRef>
Binder::BindBaseTable(std::string table_name,std::optional<std::string> alias){
    auto table_info  = cata_log_->GetTable(table_name);

    if(table_info ==nullptr)
        throw Exception("Not Find This Table");

    return std::unique_ptr<BoundBaseTableRef>(
        new BoundBaseTableRef(std::move(table_name),table_info->table_oid_,std::move(alias),table_info->GetSchema()));
}

std::unique_ptr<SelectStatement> Binder::BindSelect(duckdb_libpgquery::PGSelectStmt* stmt){
    
    if(stmt->valuesLists!=nullptr){
        auto all_values  = BindValueList(stmt->valuesLists);
        auto value_list_name = std::format("__values#{}",universe_id_++);
        all_values->identifier_ =value_list_name;
        
        std::vector<std::unique_ptr<BoundExpression>> col_exprs_;
        for(uint32_t i = 0;i<all_values->values_[0].size();++i){
            std::vector<std::string> col
                {value_list_name,std::format("{}",i)};
            col_exprs_.push_back(std::make_unique<BoundColumnRef>(std::move(col)));
        }
        return std::unique_ptr<SelectStatement>(new SelectStatement(std::move(all_values), 
            std::move(col_exprs_),
            std::make_unique<BoundExpression>(),{},std::make_unique<BoundExpression>(),
            std::make_unique<BoundExpression>(),false));
    }

    if(stmt->withClause!=nullptr){
        throw NotImplementedException("Not Support CTE for now");
    }

    auto table =BindFrom(stmt->fromClause);
    scope_ = table.get();

    if(stmt->targetList == nullptr)
        throw Exception("select list couldn't be empty");

    auto select_list = BindSelectList(stmt->targetList);

    auto where = std::make_unique<BoundExpression>();
    if(stmt->whereClause!=nullptr){
        where = BindWhere(stmt->whereClause);  
    }

    auto group_by = std::vector<std::unique_ptr<BoundExpression>>();
    if(stmt->groupClause!=nullptr){
        group_by = BindGroupBy(stmt->groupClause);
    }
    auto having  = std::make_unique<BoundExpression>();
    if(stmt->havingClause!=nullptr)
        having = BindHaving(stmt->havingClause);

    // limit 

    auto limit = std::make_unique<BoundExpression>();
    if(stmt->limitCount)
        limit = BindLimitCount(stmt->limitCount);
    // offset 

    auto limit_offset = std::make_unique<BoundExpression>();
    if(stmt->limitOffset)
        limit_offset = BindLimitCount(stmt->limitOffset);
    // sort 
    
    auto sort = std::vector<std::unique_ptr<BoundExpression>>();
    if(stmt->sortClause)
        sort = BindSort(stmt->sortClause);

    return std::unique_ptr<SelectStatement>(
        new SelectStatement(std::move(table),std::move(select_list),std::move(where)
        ,std::move(group_by),std::move(having),std::move(limit),false)
    );

}
std::unique_ptr<BoundExpression>
Binder::BindHaving(duckdb_libpgquery::PGNode* node){
    throw NotImplementedException("Not Support hving");
}
std::vector<std::unique_ptr<BoundExpression>>
Binder::BindSort(duckdb_libpgquery::PGList* list){
    throw NotImplementedException("Not support sort for now");
}

std::unique_ptr<BoundExpression>
Binder::BindLimitCount(duckdb_libpgquery::PGNode* node){
    throw NotImplementedException("Not support limit for now");
}

std::vector<std::unique_ptr<BoundExpression>>
Binder::BindGroupBy(duckdb_libpgquery::PGList* list){
    throw NotImplementedException("Not support group by");
}



std::unique_ptr<BoundExpression>
Binder::BindWhere(duckdb_libpgquery::PGNode* node){
    return BindExpression(node);
}

std::vector<std::unique_ptr<BoundExpression>>
Binder::BindSelectList(duckdb_libpgquery::PGList* target_list){
    bool is_star_expression = false;
    std::vector<std::unique_ptr<BoundExpression>> exprs;
    for(auto entry=target_list->head;
        entry!=nullptr;entry=entry->next){
        
        auto* node = reinterpret_cast<duckdb_libpgquery::PGNode*>(
            entry->data.ptr_value);
        
        auto expr = BindExpression(node);
        if(expr->GetType() == ExpressionType::STAR){
            if(!exprs.empty()){
                throw Exception("You can't select * and other expression save time");
            }
            is_star_expression= true;
            exprs = GetAllColumnExpr(scope_);
        }else{
            if(is_star_expression)
                throw Exception("You can't select * and other expression save time");

            exprs.push_back(std::move(expr));
        }
    }
    return exprs;
}
std::vector<std::unique_ptr<BoundExpression>>
Binder::GetAllColumnExpr(BoundTabRef* table){
    switch(table->type_){
        case TableReferenceType::BASE_TABLE:{
            std::vector<std::unique_ptr<BoundExpression>> v;
            auto* base_table = reinterpret_cast<BoundBaseTableRef*>(table);
            auto* t_c = cata_log_->GetTable(base_table->table_id_);
            for(auto& col :t_c->schema_.columns_){
                v.push_back(std::unique_ptr<BoundColumnRef>(
                    new BoundColumnRef({base_table->table_name_,col.name_})));
            }
            return v;
        }
        default:
            break;
    }
    throw NotImplementedException("Not Support for now in GetAllColumnExpr");
}

std::unique_ptr<BoundTabRef>
Binder::BindFrom(duckdb_libpgquery::PGList* from){

    if(from->length>1){
        throw NotImplementedException("Not support cross table");
    }
    
    auto node = reinterpret_cast<duckdb_libpgquery::PGNode*>
            (from->head->data.ptr_value);
    
    return BindTable(node);
}


std::unique_ptr<BoundTabRef>

Binder::BindTable(duckdb_libpgquery::PGNode* node){
    switch(node->type){
        case duckdb_libpgquery::T_PGRangeVar:{
            auto range_var = 
                reinterpret_cast<duckdb_libpgquery::PGRangeVar*>(node);
            return BindRangeVar(range_var);
        }
        case duckdb_libpgquery::T_PGJoinExpr:{
            auto join_expr = reinterpret_cast<duckdb_libpgquery::PGJoinExpr*>(node);
            return BindJoinExpr(join_expr);
        }
        case duckdb_libpgquery::T_PGRangeSubselect:{
            auto* sub_query  = 
                reinterpret_cast<duckdb_libpgquery::PGRangeSubselect*>(node);
            return BindRangeSubSelect(sub_query);
        }
        default:
            break;
    }
    throw Exception("Unreachable");
}
std::unique_ptr<BoundTabRef>
Binder::BindRangeSubSelect(duckdb_libpgquery::PGRangeSubselect* query){
    if(query->lateral)
        throw NotImplementedException("Not Support lateral in subquery");


    auto* sub = reinterpret_cast<duckdb_libpgquery::PGSelectStmt*>(query->subquery);
    if(query->alias)
        return BindSubSelect(sub,query->alias->aliasname);

    return BindSubSelect(sub,
    (std::format("__unname_sub_quer.{}",this->universe_id_++)));


}
std::unique_ptr<BoundTabRef>
Binder::BindSubSelect(duckdb_libpgquery::PGSelectStmt* stmt,std::string alias){
    std::vector<std::vector<std::string>> select_list;
    auto sub =  BindSelect(stmt);
    for(auto& item:sub->select_list_){
        switch(item->type_){
            case ExpressionType::COLUMN_REF:{
                auto& c_ref = reinterpret_cast<BoundColumnRef&>(*item);
                select_list.push_back(c_ref.column_);
                break;
            }
            case ExpressionType::ALIAS:{
                auto& alias = reinterpret_cast<BoundAlias&>(*item);
                select_list.push_back({alias.alias_});
            }
            default:
                select_list.push_back({std::format("__sub_query_item_.{}",universe_id_++)});
        }
    }

    return std::unique_ptr<BoundSubQueryTable>(
        new BoundSubQueryTable(std::move(select_list),std::move(sub),std::move(alias))
    );
}

std::unique_ptr<BoundTabRef>
Binder::BindRangeVar(duckdb_libpgquery::PGRangeVar* range_var){
    // here should check if this range_var stand for CTE ,but 
    // if()

    if(range_var->alias!=nullptr)
        return BindBaseTable(range_var->relname,std::make_optional(range_var->alias->aliasname));

    return BindBaseTable(range_var->relname,std::nullopt);
}

std::unique_ptr<BoundTabRef>
Binder::BindJoinExpr(duckdb_libpgquery::PGJoinExpr* join_exrp){
    
    throw NotImplementedException("Not Support Join Expre");
}



/**
 * @brief this function Bind "insert into values(1,2),(2,3)" which "(1,2),(2,3)"
 * 
 * @param list 
 * @return std::vector<std::vector<std::unique_ptr<BoundExpression>>> 
 */
std::unique_ptr<BoundExpressionList>
Binder::BindValueList(duckdb_libpgquery::PGList* list){
    std::vector<std::vector<std::unique_ptr<BoundExpression>>> all_values;
    for(auto entry = list->head; entry!=nullptr;entry=entry->next){
        auto pg_list = static_cast<duckdb_libpgquery::PGList*>(entry->data.ptr_value);
        auto values = BindExpressionLists(pg_list);
        all_values.push_back(std::move(values));
    }

    return std::make_unique<BoundExpressionList>(std::move(all_values));
}


/**
 * @brief this function bind "insert into values(1,2),(2,3)" which "(1.2)""
 * 
 * @param list 
 * @return std::vector<std::unique_ptr<BoundExpression>> 
 */
std::vector<std::unique_ptr<BoundExpression>>
Binder::BindExpressionLists(duckdb_libpgquery::PGList* list){
    
    std::vector<std::unique_ptr<BoundExpression>> values;
    for(auto entry = list->head;entry!=nullptr;entry=entry->next){
        auto pg_node = static_cast<duckdb_libpgquery::PGNode*>(entry->data.ptr_value);
        auto expr = BindExpression(pg_node);
        values.push_back(std::move(expr));
    }
    return values;
}


std::unique_ptr<BoundExpression>
Binder::BindExpression(duckdb_libpgquery::PGNode* node){
    switch (node->type) {
        case duckdb_libpgquery::T_PGAConst:
            return BindConstant(reinterpret_cast<duckdb_libpgquery::PGAConst*>(node));
        case duckdb_libpgquery::T_PGColumnRef:{
            auto* col_ref = reinterpret_cast<duckdb_libpgquery::PGColumnRef*>(node);
            return BindColumnRef(col_ref);
        }
        case duckdb_libpgquery::T_PGAStar:{
            auto* star = reinterpret_cast<duckdb_libpgquery::PGAStar*>(node);
            return BindStarRef(star);
        }
        case duckdb_libpgquery::T_PGFuncCall:{
            auto* func_call = reinterpret_cast<duckdb_libpgquery::PGFuncCall*>(node);
            return BindFuncCall(func_call);
        }
        case duckdb_libpgquery::T_PGResTarget:{
            auto* res_target = reinterpret_cast<duckdb_libpgquery::PGResTarget*>(node);
            return BindResTarget(res_target);
        }
        case duckdb_libpgquery::T_PGAExpr:{
            auto expr = reinterpret_cast<duckdb_libpgquery::PGAExpr*>(node);
            return BindExpr(expr);
        } 
        case duckdb_libpgquery::T_PGBoolExpr:{
            auto bool_expr   = reinterpret_cast<duckdb_libpgquery::PGBoolExpr*>(node);
            return BindBoolExpr(bool_expr);
        }
        default:
            break;
    }
    throw Exception("BindExpression error ,UnSupport node type");
}

std::unique_ptr<BoundExpression>
Binder::BindColumnRef(duckdb_libpgquery::PGColumnRef* col_ref){
    auto* fields = col_ref->fields;
    auto* head_node = reinterpret_cast<duckdb_libpgquery::PGNode*>(
        fields->head->data.ptr_value);
    switch(head_node->type){
        case duckdb_libpgquery::T_PGString:{
            std::vector<std::string> col_name;
            for(auto entry = fields->head;entry!=nullptr;entry=entry->next){
                col_name.push_back(
                    reinterpret_cast<duckdb_libpgquery::PGValue*>(entry->data.ptr_value)->val.str);
            }
            
            return ResolveColumn(*scope_,col_name);
        }
        case duckdb_libpgquery::T_PGAStar:{
            auto* star = reinterpret_cast
            <duckdb_libpgquery::PGAStar*>(head_node);
            return BindStarRef(star);
        }
        default:
            break;
    }
    throw Exception("Unreachable");
}


auto Binder::ResolveColumn(const BoundTabRef &scope,std::vector<std::string> &col_name)
    -> std::unique_ptr<BoundExpression> {
  auto expr = ResolveColumnInternal(scope, col_name);
  if (!expr) {
    throw Exception(std::format("column {} not found", join(col_name, ".")));
  }
  return expr;
}

std::unique_ptr<BoundExpression>
Binder::ResolveColumnInternal(const BoundTabRef& scope,
std::vector<std::string>& col_names){
    switch(scope.type_){
        case TableReferenceType::BASE_TABLE:{
            auto& base =
                 dynamic_cast<const BoundBaseTableRef&>(scope);
            if(col_names.size()==1){
                col_names.insert(col_names.begin(),base.table_name_);
                return std::unique_ptr<BoundColumnRef>(
                    new BoundColumnRef(col_names)
                );
            }
        }
        default:
            break;
    }
    throw NotImplementedException("Not impement resolve other column name");
}


std::unique_ptr<BoundExpression>
Binder::BindStarRef(duckdb_libpgquery::PGAStar* star){
    return std::unique_ptr<BoundStar>(
        new BoundStar()
    );
}
std::unique_ptr<BoundExpression>
Binder::BindFuncCall(duckdb_libpgquery::PGFuncCall* func){
    throw NotImplementedException("Not Support Agg");
}

std::unique_ptr<BoundExpression>
Binder::BindResTarget(duckdb_libpgquery::PGResTarget* target){
    auto expr = BindExpression(target->val);
    if(expr == nullptr)
        throw Exception("BindResTarget Error");
    if(target->name!=nullptr){
        return std::unique_ptr<BoundAlias>(
            new BoundAlias(std::move(expr),target->name)
        );
    }
    return expr;
}

std::unique_ptr<BoundExpression>
Binder::BindExpr(duckdb_libpgquery::PGAExpr* expr){
    
    auto name = std::string((reinterpret_cast<duckdb_libpgquery::PGValue *>
    (expr->name->head->data.ptr_value))->val.str);

    auto larg = std::make_unique<BoundExpression>();
    auto rarg = std::make_unique<BoundExpression>();

    if(expr->lexpr!=nullptr)
        larg = BindExpression(expr->lexpr);
    if(expr->rexpr!=nullptr)
        rarg = BindExpression(expr->rexpr);

    if(larg && rarg){
        return std::unique_ptr<BoundExpression>(
            new BoundBinaryOp(std::move(name),std::move(larg),std::move(rarg))
        );
    }

    if(!larg  && rarg){
        throw NotImplementedException("Not implement unary op");
    }
    throw Exception("unsupported AExpr: left == null while right != null");
}
std::unique_ptr<BoundExpression>
Binder::BindBoolExpr(duckdb_libpgquery::PGBoolExpr* expr){
    throw NotImplementedException("Not support bool expr");
}


std::unique_ptr<BoundExpression>
Binder::BindConstant(duckdb_libpgquery::PGAConst* node){
    D_assert(node);

  const auto &val = node->val;
  switch (val.type) {
    case duckdb_libpgquery::T_PGInteger: {
      if (val.val.ival > INT32_MAX ){
        throw Exception("out of range ,max value is INT32_MAX");
      }
      if(val.val.ival < INT32_MIN){
        throw Exception("out of range ,min value is INT32_min");
      }
      return std::make_unique<BoundConstant>(ValueUnion(val.val.ival));
    }
    case duckdb_libpgquery::T_PGString: {
        return std::make_unique<BoundConstant>(ValueUnion(std::string(val.val.str)));
    }
    case duckdb_libpgquery::T_PGNull: {
      return std::make_unique<BoundConstant>(ValueUnion("NULL"));
    }
    default:
      break;
  }
  
  throw Exception(std::format("unsupported pg value: {}","UNKNOWN"));
    
}
