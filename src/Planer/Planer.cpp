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
            schema->AddColumn(col_expr.ToString(),col_expr.col_type_);
        }

        default:
            break;
    }
}

void Planer::LoadColumn(const Column& col,LogicalOperatorRef op){
    if(op->GetOutPutSchema()->exist(col)){
        return;
    }
    // output schema hasn't !
    if(op->GetInputSchema()->exist(col)){
        // if input has already exist ,just need add to output schema.
        op->GetOutPutSchema()->AddColumn(col);
        return;
    }
    // input schema and output schame both hasn't

    // let all child node load this column.
    for(auto& child:op->children_){
        LoadColumn(col,child);
    }
    if(op->GetType()!=SeqScanOperatorNode)
        CHEKC_THORW(op->children_[0]->GetOutPutSchema()->exist(col));
    
    op->SetInputSchema(op->children_[0]->GetOutPutSchema()->Copy());
    op->GetOutPutSchema()->AddColumn(col);
}

void Planer::SetInputOutputSchemaInternal(LogicalOperatorRef op){
    switch(op->GetType()){
        case MaterilizeOperatorNode:{
            SetInputOutputSchemaInternal(op->children_[0]);
            auto& node = op->Cast<MaterilizeLogicaloperator>();            
            node.SetInputSchema(op->children_[0]->GetOutPutSchema());
            std::vector<Column> final_output_cols;
            for(auto& expr : node.final_select_list_expr_){
                auto final_col_name = expr->toString();
                // select colA+1 ,colB > colC from test_1 . 
                auto type = expr->GetReturnType();
                final_output_cols.push_back(Column(final_col_name,type));
            }
            auto output_schema = std::make_shared<Schema>();
            output_schema->AddColumns(final_output_cols);

            node.SetOutputSchema(output_schema);

            break;
        }
        case FilterOperatorNode:{
            SetInputOutputSchemaInternal(op->children_[0]);
            auto& node = op->Cast<FilterLogicalOperator>();

            auto& select_list = select_list_queue_.front();
            std::vector<Column> filter_cols;
            for(auto& expr : node.pridicator_){
                std::vector<Column> _f_c;
                expr->collect_column(_f_c);
                filter_cols.insert(filter_cols.end(),_f_c.begin(),_f_c.end());
            }
            // check if columns that this node need exist in child output_schema .
            // if not exist , let child node load this column.
            for(auto& col:filter_cols){
                LoadColumn(col,node.children_[0]);
            }
            node.SetInputSchema(node.children_[0]->GetOutPutSchema()->Copy());
            //final erase columns that not in final select_list.
            auto output_schema = eraseSurplusColumn(select_list,op);
            node.SetOutputSchema(output_schema);
            break;
        }
        case ValuesOperatorNode:{
            // to the end . input schema already seted.
            auto& node = op->Cast<ValuesLogicalOperator>();
            node.SetOutputSchema(node.GetInputSchema()->Copy());
            break;
        }
        case SeqScanOperatorNode:{
            auto& node = op->Cast<SeqScanLogicalOperator>();
            auto& col_name = node.GetInputSchema()->columns_[0].name_;
            auto* tb_catalog = cata_log_->GetTable(node.table_name_);
            auto* col_heap = tb_catalog->GetColumnHeapByName(col_name);
            if(col_heap->get()->metadata.total_rows==0)
                throw Exception("empty set");
            auto& select_list = select_list_queue_.front();
            auto output_schema = eraseSurplusColumn(select_list,op);
            node.SetOutputSchema(output_schema);
            break;
        };
        case InsertOperatorNode:{
            SetInputOutputSchemaInternal(op->children_[0]);
            auto& node = op->Cast<InsertLogicalOperator>();
            auto child_schema = op->children_[0]->GetOutPutSchema();
            auto* tb  = cata_log_->GetTable (node.inserted_table_);
            auto& table_schema = tb->schema_;
            if(table_schema.columns_.size() != child_schema->columns_.size()){
                throw Exception(std::format("insert values mismatch,actuclly {},need {}",child_schema->columns_.size(),table_schema.columns_.size()));
            }
            for(uint32_t i=0;i<table_schema.columns_.size();++i){
                auto& a = table_schema.columns_[i];
                auto& b = child_schema->columns_[i];
                if(a.type_!= b.type_){
                    throw Exception(
                        std::format(
                            "insert values type mismatch in {} nd values. Should be {},actuclly {}," 
                            ,i,ColumnTypeToString(a.type_),ColumnTypeToString(b.type_) ) );
                }
            }
            node.SetInputSchema(op->children_[0]->GetOutPutSchema()->Copy());
            auto output_schema = std::make_shared<Schema>();
            output_schema->AddColumn("__capsule_db_insert_",ColumnType::STRING);
            node.SetOutputSchema(output_schema);
            break;
        }
        default:{
            NOT_IMP                
        }
    }
}
void Planer::SetInputOutputSchema(
LogicalOperatorRef plan_){
    SetInputOutputSchemaInternal(plan_);
}


void Planer::CreatePlan(std::unique_ptr<BoundStatement> stmt){
    switch (stmt->type_) {
        case StatementType::INSERT_STATEMENT:{
            auto& insert_stmt =
                 dynamic_cast<InsertStatement&>(*stmt);
            plan_ = PlanInsert(insert_stmt);
            SetInputOutputSchema(plan_);
            break;
        }
        case StatementType::SELECT_STATEMENT:{
            auto& select_stmt = 
                dynamic_cast<SelectStatement&>(*stmt);
            plan_ = PlanSelect(select_stmt);
            SetInputOutputSchema(plan_);
        }
        default:
            break;
    }   
}
void 
Planer::CreatePlanAndShowPlanTree(std::unique_ptr<BoundStatement> stmt){
    CreatePlan(std::move(stmt));
    uint8_t depth=0;
    if(show_info) PreOrderTraverse(plan_,depth);
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


void 
Planer::GetAllColNameFromTableRef(const BoundTabRef& table_ref,
std::map<std::string,Schema>& map ){
    switch(table_ref.type_){
        case TableReferenceType::EXPRESSION_LIST:{
            auto& expr_list = down_cast<const BoundExpressionList&>(table_ref);
            auto size = expr_list.values_.size();
            for(uint32_t i=0;i<size;++i){
                auto s = std::format("{}.{}","__values#{}",i);
                map["__values_insert#"].AddColumn(s,UNKOWN);
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
    LogicalOperatorRef plan = nullptr;
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
    auto final_select_list_exprs = std::vector<LogicalExpressionRef>();
    for(auto& expr : stmt.select_list_){
        auto [_1,pr] =  PlanExpression(*expr,{plan});
        final_select_list_exprs.push_back(std::move(pr));
    }
    SchemaRef select_list_column = std::make_shared<Schema>();
    
    std::vector<Column> _cols;
    for(auto& expr:final_select_list_exprs){
        std::vector<Column> cols;
        expr->collect_column(cols);
        _cols.insert(_cols.end(),cols.begin(),cols.end());
    }
    select_list_column->AddColumns(_cols);
    select_list_queue_.push_back(std::move(select_list_column));


    if(!stmt.where_->isInvalid()){
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
    
    plan->Cast<MaterilizeLogicaloperator>().final_select_list_expr_ = std::move(final_select_list_exprs);
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
        auto& names = context_.planings_[col_ref.table_name()]->column_names_;
        
        auto col_name = col_ref.ToString();
        bool found=false;
        for(auto& col: names->columns_){
            if(col.name_ == col_name){
                if(found)
                    throw Exception("found same col_name");
                found=true;
            }
        }

        auto idx = context_.planings_[col_ref.table_name()]->TryGetColumnidx(col_ref.ToString());
        context_.planings_[col_ref.table_name()]->AddInterestedColumn(col_ref.ToString());
        auto col_info = context_.planings_[col_ref.table_name()]->column_names_->getColumnByname(col_ref.ToString());

        if(!col_info.has_value())
            throw Exception("Not found this column");

        return {col_name,std::shared_ptr<ColumnValueExpression>(
            new ColumnValueExpression(idx,col_info.value(),0)
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
    
    auto table = std::shared_ptr<SeqScanLogicalOperator>(
        new SeqScanLogicalOperator(base_table.table_name_,
        base_table.table_id_,{})
    );

    table->SetInputSchema(std::make_shared<Schema>());
    context_.planings_[base_table.table_name_] =
        std::make_unique<TablePlan>(
            base_table.table_name_,cata_log_->GetTable(base_table.table_name_)->GetSchemaRef(),table->input_schema_);

    context_.table_name_queue_.push_back(table->table_name_);
    return table;
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

    // check if all_values type equal;
    std::vector<ValueType> example;
    for(uint32_t y = 0;y<all_values[0].size();++y){
        example.push_back(all_values[0][y]->Evalute(nullptr,0).type_);
    }
    for(uint32_t row = 1;row<all_values.size();++row){
        for(uint32_t y = 0;y<all_values[0].size();++y){
            if(all_values[row][y]->Evalute(nullptr,0).type_!=example[y]){
                throw  Exception(std::format(
        "Type mismatch in {} row {} col ",row,y 
                ));
            }
        }
    }



    std::vector<Column> cols;
    const auto size = all_values[0].size();
    for(uint32_t i=0;i<size;++i){
        auto col_name = std::format("{}.{}",expr_list.identifier_,i);
        auto&& col = Column(col_name,i*4);
        auto val = all_values[0][i]->Evalute(nullptr,0);
        if(val.type_ == TypeInt){
            col.type_ = ColumnType::INT;
        }else {
            col.type_ = ColumnType::STRING;
        }
        cols.push_back(col);
    }
    auto schema = 
        std::shared_ptr<Schema>(new Schema(std::move(cols)));

    auto values = std::shared_ptr<ValuesLogicalOperator>(new  
        ValuesLogicalOperator(std::move(all_values)));

    values->SetInputSchema(schema);
    
    context_.planings_["__values#0"] = std::make_unique<TablePlan>("__values#0",schema,std::make_shared<Schema>());
    return values;

}