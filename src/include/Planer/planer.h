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
class Planer{
public:
    
    Planer(CataLog* cata_log);

    void 
    CreatePlan(std::unique_ptr<BoundStatement> stmt);

    LogicalOperatorRef
    PlanSelect(const SelectStatement& stmt);

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
    PlanExpressionList(const BoundExpressionList& expr_list);


    void 
    CreatePlanAndShowPlanTree(
        std::unique_ptr<BoundStatement> stmt);





    LogicalExpressionRef 
    PlanBinaryOp(const BoundBinaryOp& binary_op,std::vector<LogicalOperatorRef> child);

    void SetInputOutputSchema(LogicalOperatorRef plan_);
    void SetInputOutputSchemaInternal(LogicalOperatorRef op);


    void AddColumnFromExpression(BoundExpression& expr,SchemaRef& schema);
    LogicalOperatorRef plan_{nullptr};

    void PreOrderTraverse(LogicalOperatorRef plan,int depth);
        

    

private:
    inline void PrintSpace(uint32_t times){
        for(uint32_t i=0;i<times;++i)
            std::cout<<" ";
    }
    inline void PrintOperatorName(LogicalOperator* o){
        auto type = o->GetType();
        switch (type) {
            case LogicalOperatorNode:{
                throw Exception("Impositable print this node");
                break;
            }
            case MaterilizeOperatorNode:{
                std::cout<<"Materilizeoperator";
                break;
            }
            case HashJoinOperatorNode:{
                std::cout<<"HashJoinOperator";
                break;
            }
            case FilterOperatorNode:{
                std::cout<<"FilterOperator";
                break;
            }
            case InsertOperatorNode:{
                std::cout<<"InsertOperator";
                break;
            }
            case ValuesOperatorNode:{
                std::cout<<"ValueOperator";
                break;
            }
            
            case AggOperatorNode:{
                std::cout<<"AggOperator";
                break;  
            }
            case LimitOperatorNode:{
                std::cout<<"LimitOperator";
                break;
            }
            case SortOperatorNode:{
                std::cout<<"SortOperator";
                break;
            }
            case SeqScanOperatorNode:{
                std::cout<<"SeqScanOperator ";
                break;
            }
            default:
                break;
        }
        o->PrintDebug();
    }



    CataLog* cata_log_;
    PlanContext context_;
    void GetAllColNameFromTableRef(const BoundTabRef& table_ref,std::map<std::string,Schema>& map);

    SchemaRef
    GetSelectListSchema(const std::vector<std::unique_ptr<BoundExpression>>& stmt);

    std::vector<Column>
    GetSelectListSchemaInternal(const std::unique_ptr<BoundExpression>& expr);
    SchemaRef scope_select_col_names_;
};