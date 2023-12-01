#include "Planer/planer.h"
#include "Binder/BoundExpression.h"
#include "Binder/BoundStatement.h"
#include "Binder/BoundTabRef.h"
#include "Binder/Expression/BoundColumnRef.h"
#include "Binder/Expression/BoundConstant.h"
#include "Binder/Expression/BoundAgg.h"
#include "Binder/Expression/BoundAlias.h"
#include "Binder/Expression/BoundBinaryOp.h"
#include "Binder/Expression/BoundUnaryOp.h"
#include "Binder/Statement/InsertStatement.h"
#include "Binder/Statement/SelectStatement.h"
#include "Binder/TableRef/BoundBaseTable.h"
#include "Binder/TableRef/BoundExpressionList.h"
#include "CataLog/Schema.h"
#include "Expressions/LogicalExpression.h"
#include "Planer/LogicalOperator.h"
#include "Planer/ValuesLogicalOperator.h"
#include "Planer/SeqScanLogicalOperator.h"
#include "Planer/FilterLogicalOperator.h"
#include "common/Exception.h"
#include "common/commonfunc.h"
#include <format>
#include <new>
#include <queue>



Planer::Planer(CataLog* cata_log):cata_log_(cata_log){

}
void Planer::AddColumnFromExpression(BoundExpression& expr,SchemaRef& schema){
    switch(expr.type_){
//   INVALID = 0,    /**< Invalid expression type. */
//   CONSTANT = 1,   /**< Constant expression type. */
//   COLUMN_REF = 3, /**< A column in a table. */
//   TYPE_CAST = 4,  /**< Type cast expression type. */
//   FUNCTION = 5,   /**< Function expression type. */
//   AGG_CALL = 6,   /**< Aggregation function expression type. */
//   STAR = 7,       /**< Star expression type, will be rewritten by binder and won't appear in plan. */
//   UNARY_OP = 8,   /**< Unary expression type. */
//   BINARY_OP = 9,  /**< Binary expression type. */
//   ALIAS = 10,     /**< Alias expression type. */

        BINARY_OP:{
            auto& binary_expr = down_cast<BoundBinaryOp&>(expr);
            AddColumnFromExpression(*binary_expr.larg_,schema);
            AddColumnFromExpression(*binary_expr.rarg_,schema);
            break;
        }
        COLUMN_REF:{
            auto& col_expr = down_cast<BoundColumnRef&>(expr);
            schema->AddColumn(col_expr.ToString());
        }

        default:
            break;
    }
}
void Planer::SetInputOutputSchemaInternal(SchemaRef last_input,LogicalOperatorRef op
,SelectStatement& expr){
    switch(op->GetType()){
        case MaterilizeOperatorNode:{
            auto& materialzie = op->Cast<MaterilizeLogicaloperator&>();
            auto select_list = GetSelectListSchema(expr.select_list_);
            materialzie.SetInputSchema(select_list);
            materialzie.SetOutputSchema(select_list);
            SetInputOutputSchemaInternal(select_list,op->children_[0],expr);
            break;
        }
        case FilterOperatorNode:{
            auto& filter = op->Cast<FilterLogicalOperator&>();
            auto input = std::make_shared<Schema>(*last_input);
            AddColumnFromExpression(*expr.where_,input);
            filter.SetOutputSchema(last_input);
            filter.SetInputSchema(input);
            SetInputOutputSchemaInternal(last_input, filter.children_[0],expr);
            break;
        }
        case ValuesOperatorNode:{
            op->SetOutputSchema(last_input);
            op->SetInputSchema(last_input);
            break;
        }
        case SeqScanOperatorNode:{
            op->SetInputSchema(last_input);
            op->SetOutputSchema(last_input);
            break;
        };
        default:
            throw Exception("Not Impl yet");
    }
}
void Planer::SetInputOutputSchema(SelectStatement& stmt,
LogicalOperatorRef plan_){
    SetInputOutputSchemaInternal(nullptr,plan_,stmt);
}


void Planer::SetInputOutputSchema(InsertStatement& stmt,LogicalOperatorRef plan){
    DASSERT(plan->GetType() == InsertOperatorNode);
    auto& insert = plan->Cast<InsertLogicalOperator&>();
    auto select_list = GetSelectListSchema(stmt.values_inserted_->select_list_);
    insert.SetInputSchema(std::make_shared<Schema>(*select_list));
    auto insert_sc = std::make_shared<Schema>();
    insert_sc->AddColumn("__capsule_db_insert__");
    insert.SetOutputSchema(insert_sc);

    SetInputOutputSchema(*stmt.values_inserted_,plan->children_[0]);


}
void Planer::CreatePlan(std::unique_ptr<BoundStatement> stmt){
    switch (stmt->type_) {
        case StatementType::INSERT_STATEMENT:{
            auto& insert_stmt =
                 dynamic_cast<InsertStatement&>(*stmt);
            plan_ = PlanInsert(insert_stmt);
            SetInputOutputSchema(insert_stmt,plan_);
            break;
        }
        case StatementType::SELECT_STATEMENT:{
            auto& select_stmt = 
                dynamic_cast<SelectStatement&>(*stmt);
            plan_ = PlanSelect(select_stmt);
            SetInputOutputSchema(select_stmt,plan_);
        }
        default:
            break;
    }   
}
void 
Planer::CreatePlanAndShowPlanTree(std::unique_ptr<BoundStatement> stmt){
    CreatePlan(std::move(stmt));
    uint8_t depth=0;
    PreOrderTraverse(plan_,depth);
}
void
Planer::PreOrderTraverse(LogicalOperatorRef plan,int depth){
    PrintSpace(8*depth);
    PrintOperatorName(plan.get());
    std::cout<<std::endl;
    for(auto& child:plan->children_){
        PreOrderTraverse(child,depth+1);
    }
}

/*
    this function get all column that seq scan node needed.
*/
SchemaRef 
Planer::GetSelectListSchema(const std::vector<std::unique_ptr<BoundExpression>>& stmt){
    std::vector<std::string> cols_names;
    for(auto& v:stmt){
        auto cols = GetSelectListSchemaInternal(v);
        cols_names.insert(cols_names.end(),cols.begin(),cols.end());
    }
    return std::shared_ptr<Schema>(
        new Schema(cols_names)
    );
}

std::vector<std::string>
Planer::GetSelectListSchemaInternal(const std::unique_ptr<BoundExpression>& expr){
    std::vector<std::string> v;
    switch(expr->GetType()){
        case ExpressionType::CONSTANT:{
            return v;
        }   
        case ExpressionType::COLUMN_REF:{
            auto& col_ref = dynamic_cast<BoundColumnRef&>(*expr);
            auto s = join(col_ref.column_,".");
            v.push_back(std::move(s));
            return v;
        }
        case ExpressionType::TYPE_CAST:{
            throw NotImplementedException("How to Get cast? in GetSelectList");
            break;
        }
        case ExpressionType::FUNCTION:{
            throw NotImplementedException("How to Get Function? in GetSelectList");
            break;
        }   
        case ExpressionType::AGG_CALL:{
            auto& bound_agg = dynamic_cast<BoundAgg&>(*expr);
            v = GetSelectListSchemaInternal(bound_agg.args_);
            return v;
        }
        case ExpressionType::STAR:{
            return v;
        }      
        case ExpressionType::UNARY_OP:{
            auto& u_op = dynamic_cast<BoundUnaryOp&>(*expr);
            return GetSelectListSchemaInternal(u_op.args_);
        }   
        case ExpressionType::BINARY_OP:{
            auto& b_op = dynamic_cast<BoundBinaryOp&>(*expr);
            auto v1 = GetSelectListSchemaInternal(b_op.larg_);
            auto v2 = GetSelectListSchemaInternal(b_op.rarg_);
            v.insert(v.end(),v1.begin(),v1.end());
            v.insert(v.end(),v2.begin(),v2.end());
            return v;
        }
        case ExpressionType::ALIAS:{
            auto& alias = dynamic_cast<BoundAlias&>(*expr);
            return GetSelectListSchemaInternal(alias.child_expression_);
            break;
        }    
        default:
            break;
    }
}

void 
Planer::GetAllColNameFromTableRef(const BoundTabRef& table_ref,
std::map<std::string,Schema>& map ){
    switch(table_ref.type_){
        case TableReferenceType::EXPRESSION_LIST:{
            auto& expr_list = down_cast<const BoundExpressionList&>(table_ref);
            auto size = expr_list.values_.size();
            for(uint32_t i=0;i<size;++i){
                auto s = std::format("{}.{}","__values#{}",i);
                map["__values_insert#"].AddColumn(s);
            }
            break;
        }
        case TableReferenceType::BASE_TABLE:{
            auto& base = down_cast<const BoundBaseTableRef&>(table_ref);
            map[base.table_name_].Merge(base.schema_);
            break;
        }
        default:
            break;
    }

}
LogicalOperatorRef
Planer::PlanSelect(const SelectStatement& stmt){   
    //bind CTE:step>
    LogicalOperatorRef plan = nullptr;

    scope_select_col_names_ = GetSelectListSchema(stmt.select_list_);

    //Bind table 
    switch (stmt.from_->type_) {
        case TableReferenceType::EMPTY:{
            plan =  std::shared_ptr<ValuesLogicalOperator>(
                new ValuesLogicalOperator({})
            );
        }
        default:
            plan = PlanTableRef(*stmt.from_);
            break;
    }
    
    GetAllColNameFromTableRef(*stmt.from_,table_schema_);
    
    // plan where 
    if(!stmt.where_->isInvalid()){
        // plan 
        auto [_1,where_expr] = PlanExpression(*stmt.where_,{plan});
        plan = std::shared_ptr<FilterLogicalOperator>(
            new FilterLogicalOperator({plan},std::move(where_expr))
        );
    }

    // plan Agg
    if(!stmt.group_by_.empty() || !stmt.having_->isInvalid()){
        throw NotImplementedException("Not Implement AggCall");
    }
    // plan distinct 
    if(stmt.is_distinct_){
        throw NotImplementedException("Not support distinct");
    }
    // plan order by
    // plan limit 
    if(!stmt.limit_->isInvalid()){
        throw Exception("Not Support Limit for now");
    }

    plan = std::shared_ptr<MaterilizeLogicaloperator>(
        new MaterilizeLogicaloperator({plan})
    );
    return plan;
}





LogicalOperatorRef
Planer::PlanInsert(const InsertStatement& stmt){
    auto select  = 
         PlanSelect(*stmt.values_inserted_);
    
    auto insert_schema = std::shared_ptr<Schema>(
            new Schema({Column("star_db.inserted",0)}));
    auto insert_plan = std::shared_ptr<InsertLogicalOperator>(
        new InsertLogicalOperator({select},stmt.insert_type_,
            stmt.table_inserted_->table_id_)
    );
    insert_plan->SetOutputSchema(insert_schema);
    return insert_plan;
}



LogicalExpressionRef
Planer::PlanConstant(const BoundConstant& constant_expr){
    
    return std::make_shared<ConstantValueExpression>(
        constant_expr.value_.clone());
}

std::tuple<std::string,LogicalExpressionRef>
Planer::PlanExpression(const BoundExpression& expr,
std::vector<LogicalOperatorRef> children){
    switch (expr.type_) {
        case ExpressionType::CONSTANT:{
            auto& constant_expr =
                 dynamic_cast<const BoundConstant&>(expr);
            return {UNKNOWNED_NAME,PlanConstant(constant_expr)};
        }
        case ExpressionType::COLUMN_REF:{
            auto& column_expr
             = dynamic_cast<const BoundColumnRef&>(expr);
             return PlanColumn(column_expr,children);
        }
        case ExpressionType::BINARY_OP:{
            auto& binary_op = 
                dynamic_cast<const BoundBinaryOp&>(expr);
            return {UNKNOWNED_NAME,PlanBinaryOp(binary_op,children)};
        }
        case ExpressionType::UNARY_OP:{

        }
        default:
            break;
    }
    throw
     Exception("Not Supprot Plan this LogicalExpression for now");
}
LogicalExpressionRef 
Planer::PlanBinaryOp(const BoundBinaryOp& binary_op,
std::vector<LogicalOperatorRef> child){
    auto [_1,lexpr] = PlanExpression(*binary_op.larg_,child);
    auto [_2,rexpr] = PlanExpression(*binary_op.rarg_,child);

    ComparisonType type_;
    auto get_compare_type = [&](){
        if(binary_op.operator_name_=="=")
            {type_ = ComparisonType::Equal; return;}
        else if(binary_op.operator_name_==">=")
            { type_ = ComparisonType::GreaterEqualThan; return;}
        else if(binary_op.operator_name_==">")
            {type_ = ComparisonType::GreaterThan; return;}
        else if(binary_op.operator_name_=="<=")
            {type_ = ComparisonType::LesserEquanThan;return;}
        else if(binary_op.operator_name_=="<")
            {type_ = ComparisonType::LesserThan;return;}
        throw NotImplementedException("Not Support this comparator type");
    };
    get_compare_type();
    return std::shared_ptr<ComparisonExpression>(
        new ComparisonExpression({lexpr,rexpr},type_)
    );
}
std::tuple<std::string,LogicalExpressionRef>
Planer::PlanColumn(const BoundColumnRef& col_ref,
const std::vector<LogicalOperatorRef>& children){
    /*
        this funtion will return a ColumnValueExpression,
    which is uesd to get a value that some column in row_id 
    (evalute function),FIlter Node , Materilize Node ,Join Node,
    Filter Node: Filter some row that not satisfy,it should be a 
    ComparisionExpression,which has two child , if "colA > 1",
    left child is ColumnVlaueExpression,right child is 
    ConstantValueExpression.
    Materilize Node :use to Materilize and ouput tuples to result_set

    */
    if(children.size()==1){
        //Filter Node ,Materilize Node take this branch
        //All the output Schema except materilze 
        // if children is Value node ,his schema should be :
        // "__values...,__values.."
        
        auto& schema = table_schema_[col_ref.table_name()];

        auto col_name = col_ref.ToString();
        bool found=false;

        for(auto& col :schema.columns_){
            if(col.name_ == col_name){
                if(found)
                    throw Exception("found same col_name");
                found=true;
            }
        }
        uint32_t idx = schema.GetColumnIdx(col_name);
        if(idx==UINT32_MAX)
            throw Exception("Not Found This col when PlanColumn");
        return {col_name,std::shared_ptr<ColumnValueExpression>(
            new ColumnValueExpression(idx,0)
        )};
    }
    throw NotImplementedException("Not support in PlanColumn");
}


LogicalOperatorRef
Planer::PlanTableRef(const BoundTabRef& expr){
    switch (expr.type_) {
        case TableReferenceType::BASE_TABLE:{
            auto& base_table = 
                dynamic_cast<const BoundBaseTableRef&>(expr);
            return PlanBaseTable(base_table);
            break;
        }
        case TableReferenceType::JOIN:{
            break;
        }
        case TableReferenceType::EXPRESSION_LIST:{
            auto& expr_list = 
                dynamic_cast<const BoundExpressionList&>(expr);
            return PlanExpressionList(expr_list);
            break;
        }
        default:
            break;
    }
    throw Exception("Not support Create Plan of this Table for now");
}
LogicalOperatorRef
Planer::PlanBaseTable(const BoundBaseTableRef& base_table){
    return std::shared_ptr<SeqScanLogicalOperator>(
        new SeqScanLogicalOperator(base_table.table_name_,
        base_table.table_id_,{})
    );
}

LogicalOperatorRef
Planer::PlanExpressionList(const BoundExpressionList& expr_list){
    std::vector<std::vector<LogicalExpressionRef>> all_values;
    for(auto& row : expr_list.values_){
        std::vector<LogicalExpressionRef> values;
        for(auto& val: row){
            auto[_1, const_expr] = 
                PlanExpression(*val,{});
            values.push_back(std::move(const_expr));
        }
        all_values.push_back(std::move(values));
    }
    std::vector<Column> cols;
    const auto& row = all_values[0].size();
    for(uint32_t i=0;i<row;++i){
        auto col_name = std::format("{}.{}",expr_list.identifier_,i);
        cols.push_back(Column(col_name,i*4));
    }
    auto schema = 
        std::shared_ptr<Schema>(new Schema(std::move(cols)));

    return std::shared_ptr<ValuesLogicalOperator>(new  
        ValuesLogicalOperator(std::move(all_values)));

}