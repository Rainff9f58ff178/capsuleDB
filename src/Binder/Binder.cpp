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
#include "Binder/Statement/ExplainStatement.h"
#include "Binder/BoundOrderBy.h"
#include "common/commonfunc.h"

Binder::Binder(duckdb_libpgquery::PGList* parsed_tree,CataLog* cata_log){
    for(auto entry=parsed_tree->head;entry!=nullptr;entry=entry->next){
        statments_.push_back(
            reinterpret_cast<duckdb_libpgquery::PGNode*>(
                    entry->data.ptr_value));
    }
    cata_log_ = cata_log;
}


std::unique_ptr<ExplainStatement> Binder::BindExplain(duckdb_libpgquery::PGExplainStmt* stmt){
    if(stmt->query ==nullptr)
        throw Exception("can't explain empty");
    
    auto query = BindStatement(stmt->query);
    return std::unique_ptr<ExplainStatement>(
        new ExplainStatement(std::move(query))
    );
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
        case duckdb_libpgquery::T_PGExplainStmt:
        return BindExplain(reinterpret_cast<duckdb_libpgquery::PGExplainStmt *>(stmt));
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
    if(col_def->colname == nullptr)
        throw Exception("Column name couldnt be empty");

    auto s = std::string(
        (reinterpret_cast<duckdb_libpgquery::PGValue *>(
            col_def->typeName->names->tail->data.ptr_value)->val.str));
    
    if(s == "int4"){
        return ColumnDef(col_def->colname,0,ColumnType::INT,4);
    }else if(s == "varchar"){
        auto exprs = BindExpressionLists(col_def->typeName->typmods);
        const auto &varchar_max_length_val =
             dynamic_cast<const BoundConstant &>(*exprs[0]);
        return ColumnDef(col_def->colname,0,ColumnType::STRING,varchar_max_length_val.value_.num_);
    }

    throw Exception("Not support column type");
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
                column.column_name = table_name +"."+column.column_name;
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
    return std::make_unique<CreateStatement>(std::move(table_name),std::move(columns));
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
            col_exprs_.push_back(std::make_unique<BoundColumnRef>(std::move(col),ColumnType::UNKOWN));
        }
        return std::unique_ptr<SelectStatement>(new SelectStatement(std::move(all_values), 
            std::move(col_exprs_),
            std::make_unique<BoundExpression>(),{},std::make_unique<BoundExpression>(),
            std::make_unique<BoundExpression>(),std::make_unique<BoundExpression>(),{},false));
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
    
    auto sort = std::vector<std::unique_ptr<BoundOrderBy>>();
    if(stmt->sortClause)
        sort = BindSort(stmt->sortClause);

    return std::unique_ptr<SelectStatement>(
        new SelectStatement(std::move(table),std::move(select_list),std::move(where)
        ,std::move(group_by),std::move(having),std::move(limit),std::move(limit_offset),std::move(sort),false)
    );

}
std::unique_ptr<BoundExpression>
Binder::BindHaving(duckdb_libpgquery::PGNode* node){
    throw NotImplementedException("Not Support hving");
}
std::vector<std::unique_ptr<BoundOrderBy>>
Binder::BindSort(duckdb_libpgquery::PGList* list){
    std::vector<std::unique_ptr<BoundOrderBy>> order_bys ;
    for(auto node = list->head; node!= nullptr;node = node->next){

        auto temp = reinterpret_cast<duckdb_libpgquery::PGNode *>(node->data.ptr_value);
        if (temp->type == duckdb_libpgquery::T_PGSortBy) {
        OrderByType type;
        auto sort = reinterpret_cast<duckdb_libpgquery::PGSortBy *>(temp);
        auto target = sort->node;
        if (sort->sortby_dir == duckdb_libpgquery::PG_SORTBY_DEFAULT) {
            type = OrderByType::ASC;
        } else if (sort->sortby_dir == duckdb_libpgquery::PG_SORTBY_ASC) {
            type = OrderByType::ASC;
        } else if (sort->sortby_dir == duckdb_libpgquery::PG_SORTBY_DESC) {
            type = OrderByType::DESC;
        } else {
            throw NotImplementedException("unsupport order by type");
        }
        auto order_expression = BindExpression(target);
            order_bys.emplace_back(std::make_unique<BoundOrderBy>(type, std::move(order_expression)));
        } else {
        throw NotImplementedException("unsupported order by node");
        }
    }
    return  order_bys;
}

std::unique_ptr<BoundExpression>
Binder::BindLimitOffset(duckdb_libpgquery::PGNode* node){
    return BindExpression(node);
}
std::unique_ptr<BoundExpression>
Binder::BindLimitCount(duckdb_libpgquery::PGNode* node){
    return BindExpression(node);
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
                auto col_name = col.name_;
                if(base_table->alias_name_){
                    col_name = ChangeTableName(col_name,*base_table->alias_name_);
                }
                v.push_back(std::unique_ptr<BoundColumnRef>(
                    new BoundColumnRef(  split(col_name,".") ,col.type_)));
            }
            return v;
        }
        case TableReferenceType::JOIN:{
            std::vector<std::unique_ptr<BoundExpression>> v;
            auto* join_table = reinterpret_cast<BoundJoinTable*>(table);
            auto l_exprs = GetAllColumnExpr(join_table->l_table_.get());
            auto r_exprs = GetAllColumnExpr(join_table->r_table_.get());

            v.insert(v.end(),std::make_move_iterator(l_exprs.begin()),
                std::make_move_iterator(l_exprs.end()));
            v.insert(v.end(),std::make_move_iterator(r_exprs.begin()),
                std::make_move_iterator(r_exprs.end())) ;
            
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
    JoinType join_type;
    switch (join_exrp->jointype){
        case duckdb_libpgquery::PGJoinType::PG_JOIN_INNER:{
            join_type = JoinType::INNER;
            break;
        }
        // case duckdb_libpgquery::PGJoinType::PG_JOIN_LEFT:{
        //     join_type = JoinType::LEFT;
        // }
        default:{
            throw Exception("Not Support Join Type");
        }
    }

    auto left_table = BindTable(join_exrp->larg);
    auto right_table = BindTable(join_exrp->rarg);
    auto join = std::make_unique<BoundJoinTable>(join_type,std::move(left_table),std::move(right_table));
    scope_ = join.get();
    auto expr = BindExpression(join_exrp->quals);
    join->condition_ = std::move(expr);
    return join;
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
            
            auto col =  ResolveColumn(*scope_,col_name);
            if(!col)
                throw Exception(std::format("{} not found in this table",col_name[0]));
            return col;
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
    if(col_names.size() >= 3){
        throw Exception("Not Support col name three part parse");
    }
    switch(scope.type_){
        case TableReferenceType::BASE_TABLE:{
            auto& base =
                 dynamic_cast<const BoundBaseTableRef&>(scope);
            if(col_names.size()==1){
                auto col_name = base.table_name_+"."+col_names[0];
                auto* tb = cata_log_->GetTable(base.table_id_);
                auto _column_ = tb->GetColumnFromSchema(col_name);
                if(!_column_.has_value())
                    return  nullptr;
                auto _col_names = col_names;
                _col_names.insert(_col_names.begin(),base.getTableNameRef());
                return std::unique_ptr<BoundColumnRef>(
                    new BoundColumnRef(_col_names,_column_->type_)
                );
            }
            // col_names.size() > 1.
            auto& table_name = col_names[0];
            if(table_name != base.getTableNameRef())
                return nullptr;
            

            auto col_name = base.table_name_ + "." + col_names[1];
            auto* tb = cata_log_->GetTable(base.table_id_);
            auto _col = tb->GetColumnFromSchema(col_name);
            if(!_col.has_value())
                return  nullptr;
            return std::unique_ptr<BoundColumnRef>(new BoundColumnRef(col_names,_col->type_));
            
        }
        case TableReferenceType::JOIN:{
            // select t1.colA ,colB from t1 inner join t2 on t1.colA = t2.colB;
            // ambiguous
            auto& join_table = dynamic_cast<const BoundJoinTable&>(scope);
            
            auto left_expr = ResolveColumnInternal(*join_table.l_table_,col_names);
            auto right_expr = ResolveColumnInternal(*join_table.r_table_,col_names);

            if(left_expr && right_expr){
                throw Exception( std::format("{} is ambiguous",join(col_names,".") ));
            }
            if(!left_expr)
                return  right_expr;
            return left_expr;
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
