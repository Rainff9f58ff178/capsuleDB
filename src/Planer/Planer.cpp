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
#include "Binder/TableRef/BoundJoinTable.h"
#include "Binder/TableRef/BoundSubQueryTable.h"
#include "CataLog/Schema.h"
#include "Expressions/LogicalExpression.h"
#include "Planer/LogicalOperator.h"
#include "Planer/ValuesLogicalOperator.h"
#include "Planer/SeqScanLogicalOperator.h"
#include "Planer/FilterLogicalOperator.h"
#include "Planer/HashJoinLogicalOperator.h"
#include "Planer/LimitLogicalOperator.h"
#include "Planer/SortLogicalOperator.h"
#include "Planer/AggregateLogicalOperator.h"
#include "Planer/SubqueryMaterializeLogicalOperator.h"
#include "Planer/MaterilizeLogicaloperator.h"
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

Planer::LoadResult Planer::LoadColumn(const Column& col,LogicalOperatorRef op){
    if(op->GetOutPutSchema()->exist(col)){
        return LoadResult::LoadSucc;
    }
    if(op->GetType() == SubqueryMaterializeOperatorNode){
        return LoadResult::LoadFailed;
    }
    if(op->GetType() == AggOperatorNode){
        return LoadResult::LoadFailed;
    }
    // output schema hasn't !
    if(op->GetInputSchema()->exist(col)){
        // if input has already exist ,just need add to output schema.
        op->GetOutPutSchema()->AddColumn(col);
        return LoadResult::LoadSucc;
    }
    // input schema and output schame both hasn't

    // let all child node load this column.
    CHEKC_THORW(op->children_.size()<=2);
    if(op->children_.size() == 1){
        auto result = LoadColumn(col,op->children_[0]);
        if(result== LoadResult::LoadSucc){
            op->GetInputSchema()->AddColumn(col);
            op->GetOutPutSchema()->AddColumn(col);
        }
        return  result;
    }
    if(op->children_.size() == 2){
        auto l_result = LoadColumn(col,op->children_[0]);
        auto r_result = LoadColumn(col,op->children_[1]);

        if(l_result ==LoadResult::LoadSucc || r_result==LoadResult::LoadSucc){
            op->GetInputSchema()->AddColumn(col);
            op->GetOutPutSchema()->AddColumn(col);
            return  LoadResult::LoadSucc;
        }
        return  LoadResult::LoadFailed;
    }
    // no child . SeqScan node. this col not in this table.
    CHEKC_THORW(op->GetType()==SeqScanOperatorNode)
    CHEKC_THORW(!op->input_schema_->exist(col));
    return  LoadResult::LoadFailed;
}

void Planer::SetInputOutputSchemaInternal(LogicalOperatorRef op){
    switch(op->GetType()){
        case MaterilizeOperatorNode:{
            SetInputOutputSchemaInternal(op->children_[0]);
            auto& node = op->Cast<MaterilizeLogicaloperator>();            
            node.SetInputSchema(op->children_[0]->GetOutPutSchema());
            std::vector<Column> final_output_cols;

            for(auto& expr : node.final_select_list_expr_){
                std::vector<Column> cols;
                expr->collect_column(cols);
                for(auto& col : cols){
                    if(LoadColumn(col,op->children_[0]) == LoadResult::LoadFailed){
                        throw Exception(std::format("Load column {} failed,Maybe you try to load a expression that not in group by ?",col.name_));
                    }
                }
                auto final_col_name = expr->toString();
                // select colA+1 ,colB > colC from test_1 . 
                auto type = expr->GetReturnType();
                final_output_cols.push_back(Column(final_col_name,type));
            }
            auto output_schema = std::make_shared<Schema>();
            output_schema->AddColumns(final_output_cols);

            node.SetOutputSchema(output_schema);


            select_list_queue_.pop_front();
            break;
        }
        case SubqueryMaterializeOperatorNode:{
            SetInputOutputSchemaInternal(op->children_[0]);
            auto& node = op->Cast<SubqueryMaterializeLogicalOperator>();
            std::vector<Column> final_output_cols;
            for(auto& expr : node.final_select_list_expr_){
                std::vector<Column> cols;
                expr->collect_column(cols);
                for(auto& col : cols){
                    if(LoadColumn(col,op->children_[0]) == LoadResult::LoadFailed){
                        throw Exception(std::format("Load column {} failed,Maybe you try to load a expression that not in group by ?",col.name_));
                    }
                }
                auto final_col_name = expr->toString();
                auto type = expr->GetReturnType();
                final_output_cols.push_back(Column(final_col_name,type));
            }
            node.SetInputSchema(op->children_[0]->GetOutPutSchema()->Copy());
            node.SetOutputSchema(node.select_list_);
            select_list_queue_.pop_front();
            break;
        }
        case AggOperatorNode:{
            SetInputOutputSchemaInternal(op->children_[0]);
            auto& node = op->Cast<AggregateLogicalOperator>();
            std::vector<Column> cols_this_node_need;
            for(auto& expr : node.group_bys_){
                std::vector<Column> cols;
                expr->collect_column(cols);
                cols_this_node_need.insert(cols_this_node_need.end(),cols.begin(),cols.end());
            }
            for(auto& col : cols_this_node_need){
                auto r = LoadColumn(col,op->children_[0]);
                if(r == LoadResult::LoadFailed){
                    throw Exception(std::format("Load Column {} Failed ,Maybe you try to load a expression that not in group by ?",col.name_));
                }
            }
            // Load columns aggs need.
            for(auto& agg:node.aggs_){
                cols_this_node_need.clear();
                for(auto& expr : agg.args_expr_){
                    std::vector<Column> cols;
                    expr->collect_column(cols);
                    cols_this_node_need.insert(cols_this_node_need.end(),cols.begin(),cols.end());
                }
                for(auto& col : cols_this_node_need){
                    auto r = LoadColumn(col,op->children_[0]);
                    if(r == LoadResult::LoadFailed){
                        throw Exception(std::format("Load Column {} Failed,Maybe you try to load a expression that not in group by ?", col.name_));
                    }
                }
            }

            //set output_schema.
            auto output_schema = std::make_shared<Schema>();
            for(auto& agg : node.aggs_){
                // for now , just all agg set to int.
                output_schema->AddColumn(Column(agg.agg_result_name,ColumnType::INT));
            }
            for(auto& col : node.group_by_cols_){
                output_schema->AddColumn(col);
            }
            
            node.SetInputSchema(op->children_[0]->GetOutPutSchema()->Copy());
            node.SetOutputSchema(output_schema);
            break;
        }
        case SortOperatorNode:{
            SetInputOutputSchemaInternal(op->children_[0]);
            auto& node = op->Cast<SortLogicalOperator>();
            for(auto& order_by : node.order_bys_){
                auto expr = order_by.second;
                std::vector<Column> cols;
                expr->collect_column(cols);
                for(auto& col : cols){
                    auto result = LoadColumn(col,op->children_[0]);
                    if(result== LoadResult::LoadFailed){
                        throw Exception(std::format("Load column {} failed,Maybe you try to load a expression that not in group by?",col.name_));
                    }
                }
            }
            node.SetInputSchema(node.children_[0]->GetOutPutSchema()->Copy());
            auto& select_list = select_list_queue_.front();
            auto output_schema = eraseSurplusColumn(select_list,op);
            node.SetOutputSchema(output_schema);
            break;
        }
        case LimitOperatorNode:{
            SetInputOutputSchemaInternal(op->children_[0]);
            auto& node = op->Cast<LimitLogicalOperator>();
            if(node.limit_ ==0 && node.offset_ ==0)
                throw Exception("Empty Set");
            

            node.SetInputSchema(op->children_[0]->GetOutPutSchema()->Copy());
            node.SetOutputSchema(node.GetInputSchema()->Copy());
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
                auto r = LoadColumn(col,node.children_[0]);
                if(r == LoadResult::LoadFailed){
                    throw Exception(std::format("Load column {} failed,Maybe you try to load a expression that not in group by?",col.name_));
                }
            }
            node.SetInputSchema(node.children_[0]->GetOutPutSchema()->Copy());
            //final erase columns that not in final select_list.
            auto output_schema = eraseSurplusColumn(select_list,op);
            node.SetOutputSchema(output_schema);
            break;
        }
        case HashJoinOperatorNode:{
            SetInputOutputSchemaInternal(op->children_[0]);
            SetInputOutputSchemaInternal(op->children_[1]);

            auto& node = op->Cast<HashJoinLogicalOperator>();
            auto& select_list = select_list_queue_.front();
            std::vector<Column> cols_this_node_need;
            CHEKC_THORW(node.condition_);
            
            // join operator left children tree is deeper tree, right child is build port.
            // if like this
            // " select * from test_1 inner join test_2 on test_1.colA + test_2.colA = test_2.colB+test_1.colA" 
            // is unexecutable,because build port of hash join can't get chunk came form "test_2" 
            // so that can't build hash table. nestLoopJoin is able to deal thsi situation
            // but im not implent it.

        
            node.condition_->collect_column(cols_this_node_need);
            for(auto& col : cols_this_node_need){
                auto l = LoadColumn(col,node.children_[0]);
                auto r = LoadColumn(col,node.children_[1]);
                if(l==LoadResult::LoadFailed && r==LoadResult::LoadFailed){
                    throw Exception(std::format("{} load failed",col.name_));
                }
            }
            auto l_output_schema = node.children_[0]->GetOutPutSchema()->Copy();
            auto r_output_schema = node.children_[1]->GetOutPutSchema()->Copy();
            l_output_schema->Merge(*r_output_schema);
            auto input = l_output_schema;
            node.SetInputSchema(input);
            auto output_schema = eraseSurplusColumn(select_list,op);
            node.SetOutputSchema(output_schema);
            // "explain select test_1.colA from test_1 inner join test_1 as b on b.colA + b.colB = test_1.colA;"
            // in this situation. 
            // build port on oppsite of probe port . need exchange it .


            // select * from random1 as a inner join random2 as b on a.colA = b.colA inner join random3 as c on  c.coLA = a.colA + b.colA ;
            auto build_output = node.children_[1]->GetOutPutSchema();
            std::vector<Column> __cols;
            node.condition_->children_[1]->collect_column(__cols);

            bool diff = false;
            bool exchange =false;
            for(auto& cols : __cols){
                if(!build_output->exist(cols)){
                    diff = true;
                    break;
                }
            }
            
            if(!CheckHashJoinCondition(node.condition_) || diff) exchange = true;
            if(exchange){
                auto l_condition = node.condition_->children_[0];
                auto r_condition = node.condition_->children_[1];
                node.condition_->children_[0]= r_condition;
                node.condition_->children_[1]= l_condition;
                DEBUG("exchange condition ");
            }
            
            if(!CheckHashJoinCondition(node.condition_))
                throw  Exception("Join Condition build port can't include column that different table,because Hashjoin doesnt support it .NestLoopJoin can,but Not Impl");
            // __cols.clear();
            // node.condition_->children_[0]->collect_column(__cols);
            // ac_table_name = getTableNameFromColName(__cols[0].name_);
            // if(ac_table_name != l_table_name){
            //     throw Exception("Build port must exist left table col,because i didn't impl nestloop join");
            // }

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
            auto col_name = std::string();
            if(node.GetInputSchema()->columns_.empty()){
                throw Exception("Whole table scan hasn't one column, i admit your query is fatal.");
            }

            col_name = node.GetInputSchema()->columns_[0].name_;
            
            if(node.alias_name_){
                // replace col_name to real col_name.
                auto cols = split(col_name,".");
                cols[0] = node.table_name_;
                col_name  = join(cols,".");
            }

            auto* tb_catalog = cata_log_->GetTable(node.table_name_);
            auto* col_heap = tb_catalog->GetColumnHeapByName(col_name);
            if(col_heap->get()->metadata.total_rows==0)
                throw Exception("empty set");
            auto& select_list = select_list_queue_.front();
            auto output_schema = eraseSurplusColumn(select_list,op);
            node.SetOutputSchema(output_schema);
            break;
        }
 
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
Planer::PreOrderTraverse(LogicalOperatorRef plan,int depth,std::stringstream& ss){
    PrintSpace(8*depth,ss);
    ss << PrintOperatorName(plan.get());
    ss<<std::endl;
    for(auto& child:plan->children_){
        PreOrderTraverse(child,depth+1,ss);
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

AggregateEntry Planer::GenerateAgg(const BoundAgg& a,LogicalOperatorRef child){
    std::string agg_result_name = a.ToString();
    AggregationType type;
    if(a.agg_name_ == "count_start"){
        type = AggregationType::CountStarAggregate;
    }else if(a.agg_name_ == "min"){
        type = AggregationType::MinAggregate;
    }else if(a.agg_name_ == "max"){
        type = AggregationType::MaxAggregate;
    }else if(a.agg_name_ =="count"){
        type = AggregationType::CountAggregate;
    }else if(a.agg_name_ == "avg"){
        type =AggregationType::AvgAggregate;
    }else if(a.agg_name_ =="sum"){
        type = AggregationType::SumAggregate;
    }
    else {
        NOT_IMP
    }
    std::vector<LogicalExpressionRef> args;
    for(auto& expr : a.args_){
        auto [_1,p] = PlanExpression(*expr,{child});
        args.push_back(p);
    }
    return AggregateEntry{std::move(agg_result_name),type,a.alias_,std::move(args)};
}

void Planer::AddAggToContext(const BoundExpression& expr,std::vector<AggregateEntry>& agg_ety,std::vector<Column>&& cols,LogicalOperatorRef child){
    switch(expr.type_){
        case ExpressionType::AGG_CALL:{
            auto& agg = down_cast<const BoundAgg&>(expr);
            if(context_.agg_map_.find(agg.ToString()) == context_.agg_map_.end()){
                context_.agg_map_[agg.ToString()] = 
                    std::shared_ptr<ColumnValueExpression>(new ColumnValueExpression(998,Column(agg.ToString(),ColumnType::DOUBLE),0));
                context_.agg_map_[agg.ToString()]->alias_ = agg.alias_;
            }
            auto it = std::find_if(agg_ety.begin(),agg_ety.end(),[&,this](AggregateEntry& _agg){
                if(_agg.agg_result_name == agg.ToString()){
                    return true;
                }
                return false;
            });
            if(it != agg_ety.end()){
                return;
            }
            agg_ety.emplace_back(GenerateAgg(agg,child));
            break;;
        }
        case ExpressionType::ALIAS:{
            UNREACHABLE;
        }
        case ExpressionType::BINARY_OP:{
            auto& binary_expr = down_cast<const BoundBinaryOp&>(expr);
            AddAggToContext(*binary_expr.larg_,agg_ety,std::move(cols),child);
            AddAggToContext(*binary_expr.rarg_,agg_ety,std::move(cols),child);
            break;
        }
        case ExpressionType::COLUMN_REF:{
            auto& col = down_cast<const BoundColumnRef&> (expr);
            cols.push_back(Column(col.ToString(),col.col_type_));
            break;
        }
        case ExpressionType::UNARY_OP:{
            auto& unary_expr = down_cast<const BoundUnaryOp&>(expr);
            AddAggToContext(*unary_expr.args_,agg_ety,std::move(cols),child);
            break;
        }
        case ExpressionType::CONSTANT:{
            break;
        }
        case ExpressionType::STAR:{
            throw Exception("Not Support star with agg");
        }
        default:
            NOT_IMP;
    }
}





LogicalOperatorRef Planer::PlanAgg(const SelectStatement& stmt,LogicalOperatorRef child){
    // add those agg to context.

    //select sum(colA),avg(colB) from t group by colC order by avg(colC)  having count(colC) > 1 limit 1;
    // first check if group by contain any agg. group by can't contain agg. 

    std::vector<LogicalExpressionRef> group_bys;
    auto group_cols = std::make_shared<Schema>();
    auto _group_cols = std::make_shared<Schema>();
    
    for(auto& expr : stmt.group_by_){
        if(expr->HasAgg()){
            throw Exception("Group By clause can't contain agg !");
        }
        auto [_1,gpb] = PlanExpression(*expr,{child});
        std::vector<Column> cols;
        group_cols->AddColumn(Column(gpb->toString(),gpb->GetReturnType()));

        gpb->collect_column(cols);
        _group_cols->AddColumns(cols);
        
        context_.agg_group_by_map_[gpb->toString()]
            = std::shared_ptr<ColumnValueExpression>(  new ColumnValueExpression(1010,Column(gpb->toString(),gpb->GetReturnType()),0));

        group_bys.push_back(std::move(gpb));
    }
    // and having clase can't include 
    std::vector<AggregateEntry> all_aggs;
    std::vector<Column> cols;

    for(auto& expr : stmt.select_list_){
        cols.clear();
        AddAggToContext(*expr,all_aggs,std::move(cols),child);
        for(auto& col : cols){
            if(!_group_cols->exist(col)){
                throw Exception(std::format("{} must appear in group by clause !",col.name_));
            }
        }
        
    }


    if(!stmt.having_->isInvalid()){
        cols.clear();
        AddAggToContext(*stmt.having_,all_aggs,std::move(cols),child);
        for(auto& col: cols){
            if(!_group_cols->exist(col)){
                throw Exception(std::format("{} must appear in group by clause !",col.name_));
            }
        }
    }



    //and order by 
    auto& order_bys = stmt.order_bys_;
    for(auto& order : order_bys){
        cols.clear();
        AddAggToContext(*order->expr_,all_aggs,std::move(cols),child);
        for(auto& col : cols){
            if(!_group_cols->exist(col)){
                throw Exception(std::format("{} must appear in group by clause !",col.name_));
            }
        }
    }
    // output should be all aggs.and group bys.
    LogicalOperatorRef plan = nullptr;
    plan = std::shared_ptr<AggregateLogicalOperator>( new AggregateLogicalOperator(std::move(group_bys),std::move(all_aggs),{child}));
    plan->Cast<AggregateLogicalOperator>().group_by_cols_ = std::move(group_cols->columns_);
    
    LogicalExpressionRef having_expr=nullptr;
    if(!stmt.having_->isInvalid()){
        auto [_1,expr] =PlanExpression(*stmt.having_,{child});
        having_expr = expr;
        plan = std::shared_ptr<FilterLogicalOperator>( new FilterLogicalOperator({plan},{having_expr}));
    }

    return plan;
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


    if(!stmt.where_->isInvalid()){
        auto [_1,where_expr] = PlanExpression(*stmt.where_,{plan});
        where_expr->alias_ = std::nullopt;
        plan = std::shared_ptr<FilterLogicalOperator>(
            new FilterLogicalOperator({plan},std::move(where_expr))
        );
    }

    

    bool has_agg = false;
    for(auto& expr : stmt.select_list_){
        if(expr->HasAgg()){
            has_agg = true;
        }
    }
    // plan Agg
    if(!stmt.group_by_.empty() || !stmt.having_->isInvalid() || has_agg){
        plan = PlanAgg(stmt,plan);
    }
    // plan distinct 
    if(stmt.is_distinct_){
        throw NotImplementedException("Not support distinct");
    }
    SchemaRef select_list_column = std::make_shared<Schema>();
    
    auto final_select_list_exprs = std::vector<LogicalExpressionRef>();
    for(auto& expr : stmt.select_list_){
        auto [_1,pr] =  PlanExpression(*expr,{plan});
        final_select_list_exprs.push_back(std::move(pr));
    }
    std::vector<Column> _cols;
    for(auto& expr:final_select_list_exprs){
        std::vector<Column> cols;
        expr->collect_column(cols);
        _cols.insert(_cols.end(),cols.begin(),cols.end());
    }
    select_list_column->AddColumns(_cols);
    select_list_queue_.push_back(std::move(select_list_column));

    // plan order by

    if(!stmt.order_bys_.empty()){
        std::vector<std::pair<OrderByType,LogicalExpressionRef>> exprs;
        for(auto& order_by : stmt.order_bys_){
            auto [_1,expr] = PlanExpression(*order_by->expr_,{plan});
            // copy it . and .erease alias.
            expr = expr->Copy();
            expr->alias_ = std::nullopt;
            exprs.push_back({order_by->order_type_,std::move(expr)});
        }
        plan = std::shared_ptr<SortLogicalOperator>(
            new SortLogicalOperator({plan},std::move(exprs))
        );
    }


    // plan limit 
    if(!stmt.limit_->isInvalid() || !stmt.limit_offset_->isInvalid()){
        auto limit =0;
        auto offset = 0;
        if(!stmt.limit_->isInvalid()){
            auto c = down_cast<BoundConstant*>(stmt.limit_.get());
            if(c->value_.type_== ValueType::TypeString){
                throw Exception("limit num can't be string");
            }   
            limit = c->value_.val_.num_;
        }
        if(!stmt.limit_offset_->isInvalid()){
            auto o = down_cast<BoundConstant*>(stmt.limit_offset_.get());
            if(o->value_.type_ == ValueType::TypeString){
                throw Exception("offset value can't be string");
            }
            offset = o->value_.val_.num_;
        }
        plan = std::shared_ptr<LimitLogicalOperator>(
            new LimitLogicalOperator({plan},limit,offset)
        );
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
        case ExpressionType::AGG_CALL:{
            auto& agg = down_cast<const BoundAgg&>(expr);
            auto it = context_.agg_map_.find(agg.ToString());
            if(it == std::end(context_.agg_map_))
                throw Exception(std::format("Logical Error,{} not found in plan context.",agg.ToString()) );
            return  {UNKNOWNED_NAME,it->second};
        }
        case ExpressionType::CONSTANT:{
            auto& constant_expr =
                 dynamic_cast<const BoundConstant&>(expr);
            auto r = PlanConstant(constant_expr);
            r->alias_ = expr.alias_;
            
            return {UNKNOWNED_NAME,r};
        }
        case ExpressionType::COLUMN_REF:{
            auto& column_expr
             = dynamic_cast<const BoundColumnRef&>(expr);
             auto [name,r]  = PlanColumn(column_expr,children);
             r->alias_ = expr.alias_;
             return {name,r};

        }
        case ExpressionType::BINARY_OP:{
            auto& binary_op = 
                dynamic_cast<const BoundBinaryOp&>(expr);
            auto r = PlanBinaryOp(binary_op,children);
            if(context_.agg_group_by_map_.find(r->toString()) != context_.agg_group_by_map_.end()){
                // this expression is group by result.
                return {UNKNOWNED_NAME,context_.agg_group_by_map_[r->toString()]};
            }
            // here transport alias is for materlialzie node
            r->alias_ = expr.alias_;
            return {UNKNOWNED_NAME,r};
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
    auto op_name = binary_op.operator_name_;
    return PlanBinaryOpInternal(op_name,lexpr,rexpr);
}

LogicalExpressionRef 
Planer::PlanBinaryOpInternal(const std::string& op_name,
LogicalExpressionRef& lexpr,LogicalExpressionRef& rexpr){
    
    if(op_name== "="){
        return  std::shared_ptr<ComparisonExpression>(new ComparisonExpression({lexpr,rexpr},ComparisonType::Equal));
    }else if(op_name == ">="){
        return  std::shared_ptr<ComparisonExpression>(new ComparisonExpression({lexpr,rexpr},ComparisonType::GreaterEqualThan));
    }else if(op_name =="<="){
        return  std::shared_ptr<ComparisonExpression>(new ComparisonExpression({lexpr,rexpr},ComparisonType::LesserEquanThan));
    }else if(op_name == ">"){
        return  std::shared_ptr<ComparisonExpression>(new ComparisonExpression({lexpr,rexpr},ComparisonType::GreaterThan));
    }else if(op_name == "<"){
        return  std::shared_ptr<ComparisonExpression>(new ComparisonExpression({lexpr,rexpr},ComparisonType::LesserThan));
    }else if(op_name == "+"){
        return  std::shared_ptr<ArithmeticExpression>(new ArithmeticExpression({lexpr,rexpr},ArithmeticType::Add));
    }else if(op_name == "-"){
        return  std::shared_ptr<ArithmeticExpression>(new ArithmeticExpression({lexpr,rexpr},ArithmeticType::Minus));
    }else if(op_name =="and"){
        return  std::shared_ptr<AndOrExpression>(new AndOrExpression({lexpr,rexpr},LogicType::And));
    }else if(op_name == "or"){
        return  std::shared_ptr<AndOrExpression>(new AndOrExpression({lexpr,rexpr},LogicType::Or));
    }
    NOT_IMP;
}


std::tuple<std::string,LogicalExpressionRef>
Planer::PlanColumn(const BoundColumnRef& col_ref,
const std::vector<LogicalOperatorRef>& children){
 
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

LogicalOperatorRef 
Planer::PlanJoinTable(const BoundJoinTable& join_table){
    auto l_node = PlanTableRef(*join_table.l_table_);
    auto r_node = PlanTableRef(*join_table.r_table_);
    auto [_1,condition] = PlanExpression(*join_table.condition_,{l_node,r_node});
    auto hash_join = 
        std::shared_ptr<HashJoinLogicalOperator>(
            new HashJoinLogicalOperator({l_node,r_node},
                condition,join_table.j_type_)
        );
    
    if(condition->GetType()!= LogicalExpressionType::ComparsionExpr){
        throw Exception("Join Expression must be ComparsionExpr");
    }


    return hash_join;
}
    

LogicalOperatorRef
Planer::PlanSubSelcet(const BoundSubQueryTable& table){
    auto& sub = down_cast<const BoundSubQueryTable&>(table);
    auto node = PlanSelect(*sub.sub_query_statement_);
    CHEKC_THORW(node->GetType() == MaterilizeOperatorNode);

    auto& ma_node = down_cast<MaterilizeLogicaloperator&>(*node);
    auto sub_query_materialize = std::shared_ptr<SubqueryMaterializeLogicalOperator>( new SubqueryMaterializeLogicalOperator(std::move(ma_node.children_),sub.select_list_));
    sub_query_materialize->final_select_list_expr_ = std::move(ma_node.final_select_list_expr_);
    context_.planings_[sub.alias_] = std::make_unique<TablePlan>(table.alias_,std::nullopt,sub.select_list_,std::make_shared<Schema>());
    return sub_query_materialize;
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
            auto& bound_join = dynamic_cast<const BoundJoinTable&>(expr);
            return PlanJoinTable(bound_join);
            break;
        }
        case TableReferenceType::EXPRESSION_LIST:{
            auto& expr_list = 
                dynamic_cast<const BoundExpressionList&>(expr);
            return PlanExpressionList(expr_list);
            break;
        }
        case TableReferenceType::SUBQUERY:{
            auto& sub_query = down_cast<const BoundSubQueryTable&>(expr);
            return PlanSubSelcet(sub_query);
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
        new SeqScanLogicalOperator(base_table.table_name_,base_table.alias_name_,
        base_table.table_id_,{})
    );
    

    table->SetInputSchema(std::make_shared<Schema>());
    context_.planings_[base_table.getTableNameRef()] =
        std::make_unique<TablePlan>(
            base_table.table_name_,base_table.alias_name_,cata_log_->GetTable(base_table.table_name_)->GetSchemaRef(),table->input_schema_);

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
        "Type mismatch in {} row {} col ",row+1,y +1
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
    
    context_.planings_["__values#0"] = std::make_unique<TablePlan>("__values#0",std::nullopt,schema,std::make_shared<Schema>());
    return values;

}