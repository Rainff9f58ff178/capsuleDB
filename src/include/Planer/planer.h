#pragma  once


#include "Binder/BoundStatement.h"
#include "Binder/Expression/BoundColumnRef.h"
#include "Binder/Expression/BoundConstant.h"
#include "Binder/Statement/CreateStatement.h"
#include "Binder/Statement/InsertStatement.h"
#include "Binder/Statement/SelectStatement.h"
#include "Binder/TableRef/BoundExpressionList.h"
#include "CataLog/CataLog.h"
#include "CataLog/TableCataLog.h"
#include "Expressions/LogicalExpression.h"
#include "Planer/LogicalOperator.h"
#include "Expressions/AndOrExpression.h"
#include "Expressions/ArithmeticExpression.h"
#include "Expressions/ColumnValueExpression.h"
#include "Expressions/ComparisonExpression.h"
#include "Expressions/ConstantValueExpression.h"
#include "Planer/MaterilizeLogicaloperator.h"
#include "Planer/ValuesLogicalOperator.h"
#include "Planer/InsertLogicalOperator.h"
#include "Planer/LogicalOperator.h"
#include "common/Exception.h"
#include "Binder/Expression/BoundBinaryOp.h"
#include "Binder/Expression/BoundUnaryOp.h"
#include "Planer/PlanContext.h"
#include <iostream>
#include <memory>

class AggregateEntry;
class BoundJoinTable;
class BoundAgg;
class BoundSubQueryTable;
class Planer{
public:
    
    Planer(CataLog* cata_log);

    void 
    CreatePlan(std::unique_ptr<BoundStatement> stmt);

    LogicalOperatorRef
    PlanSelect(const SelectStatement& stmt);

    LogicalOperatorRef
    PlanSubSelcet(const BoundSubQueryTable& table);
    LogicalOperatorRef
    PlanInsert(const InsertStatement& stmt);

    std::tuple<std::string,LogicalExpressionRef>
    PlanExpression(const BoundExpression& expr,
        std::vector<LogicalOperatorRef> children);

    LogicalExpressionRef
    PlanConstant(const BoundConstant& constant_expr);

    std::tuple<std::string,LogicalExpressionRef>
    PlanColumn(const BoundColumnRef& col_ref,
        const std::vector<LogicalOperatorRef>& children);
    

    LogicalOperatorRef
    PlanTableRef(const BoundTabRef& expr);


    LogicalOperatorRef
    PlanBaseTable(const BoundBaseTableRef& base_table);

    LogicalOperatorRef 
    PlanJoinTable(const BoundJoinTable& join_table);
    

    LogicalOperatorRef
    PlanExpressionList(const BoundExpressionList& expr_list);

    LogicalOperatorRef 
    PlanAgg(const SelectStatement& stmt,LogicalOperatorRef child);
    void AddAggToContext(const BoundExpression& expr,
    std::vector<AggregateEntry>& agg_ety,
    std::vector<Column>&& cols,LogicalOperatorRef child);

    AggregateEntry GenerateAgg(const BoundAgg& a,LogicalOperatorRef child);
    
    bool CheckHashJoinCondition(LogicalExpressionRef condition){
        auto right_cols = std::vector<Column>();
        condition->children_[1]->collect_column(right_cols);
        CHEKC_THORW(!right_cols.empty());
        std::set<std::string> right_table;

        // if build port have more than one table return false;
        for(auto& col:right_cols){
            auto n = getTableNameFromColName(col.name_);
            right_table.insert(n);
        }
        if(right_table.size() > 1)
            return false;
        return true;
    }





    LogicalExpressionRef 
    PlanBinaryOp(const BoundBinaryOp& binary_op,std::vector<LogicalOperatorRef> child);
    LogicalExpressionRef 
    PlanBinaryOpInternal(const std::string& op_name,
    LogicalExpressionRef& left_child,LogicalExpressionRef& right_child);


    void SetInputOutputSchema(LogicalOperatorRef plan_);
    void SetInputOutputSchemaInternal(LogicalOperatorRef op);


    void AddColumnFromExpression(BoundExpression& expr,SchemaRef& schema);
    LogicalOperatorRef plan_{nullptr};

    std::string ShowPlanTree(LogicalOperatorRef plan){
        std::stringstream ss;
        uint32_t depth = 0;
        PreOrderTraverse(plan,depth,ss);
        return ss.str();
    }
    void PreOrderTraverse(LogicalOperatorRef plan,int depth,std::stringstream& ss);
        
    // check if op output this column,if not ,load 
    enum class LoadResult:uint8_t{LoadSucc,LoadFailed};
    LoadResult LoadColumn(const Column& col,LogicalOperatorRef op);
    
    SchemaRef eraseSurplusColumn(SchemaRef select_schema,LogicalOperatorRef op){
        auto output_schema = op->GetInputSchema()->Copy();
        for(auto& col:op->GetInputSchema()->columns_){
            if(!select_schema->exist(col)){
                output_schema->erase(col);
            }
        }
        return output_schema;
    }
private:
    inline void PrintSpace(uint32_t times,std::stringstream& ss){
        for(uint32_t i=0;i<times;++i)
            ss<<" ";
    }
    inline std::string PrintOperatorName(LogicalOperator* o){
        std::stringstream ss;
        auto type = o->GetType();
        switch (type) {
            case LogicalOperatorNode:{
                throw Exception("Impositable print this node");
                break;
            }
            case MaterilizeOperatorNode:{
                ss<<  "Materilizeoperator";
                break;
            }
            case SubqueryMaterializeOperatorNode:{
                ss<<"SubqueryMaterializeNode";
                break;
            }
            case HashJoinOperatorNode:{
                ss<<"HashJoinOperator";
                break;
            }
            case FilterOperatorNode:{
                ss<<"FilterOperator";
                break;
            }
            case InsertOperatorNode:{
                ss<<"InsertOperator";
                break;
            }
            case ValuesOperatorNode:{
                ss<<"ValueOperator";
                break;
            }
            
            case AggOperatorNode:{
                ss<<"AggOperator";
                break;  
            }
            case LimitOperatorNode:{
                ss<<"LimitOperator";
                break;
            }
            case SortOperatorNode:{
                ss<<"SortOperator";
                break;
            }
            case SeqScanOperatorNode:{
                ss<<"SeqScanOperator";
                break;
            }
            default:
                break;
        }
        ss<< o->PrintDebug();
        return ss.str();
    }


    std::list<SchemaRef> select_list_queue_;
    CataLog* cata_log_;
    PlanContext context_;
    void GetAllColNameFromTableRef(const BoundTabRef& table_ref,std::map<std::string,Schema>& map);

    SchemaRef scope_select_col_names_;
};