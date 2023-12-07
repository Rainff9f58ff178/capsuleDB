

#include "Optimizer/Optimizer.h"
#include "Planer/InsertLogicalOperator.h"
#include "Planer/SeqScanLogicalOperator.h"
#include "Planer/MaterilizeLogicaloperator.h"
#include "Planer/FilterLogicalOperator.h"
/*
    the root node should be insert_node
*/
LogicalOperatorRef
Optimizer::OptimizerInsert(LogicalOperatorRef plan){
    assert(plan->GetType() == OperatorType::InsertOperatorNode);
    auto& insert_node =
         dynamic_cast<const InsertLogicalOperator&>(*plan);
    if(insert_node.insert_type_==INSERT_SELECT)
        return plan;
    
    return OptimizerInsertInternal(std::move(plan));
}

LogicalOperatorRef
Optimizer::OptimizerInsertInternal(LogicalOperatorRef plan){
    std::vector<LogicalOperatorRef> children;

    for(auto& child:plan->children_){
        children.push_back(OptimizerInsertInternal(child));
    }

    if(plan->GetType()==OperatorType::MaterilizeOperatorNode){
        return plan->children_[0];
    }
    auto optimizer_plan = 
        plan->CopyWithChildren(std::move(children));
    return optimizer_plan;
}


LogicalOperatorRef 
Optimizer::FilterPushDown(
LogicalOperatorRef plan){
    // push filter to SeqScan node or hash join node.

    std::vector<LogicalOperatorRef> children;
    for(auto& child:plan->children_){
        children.push_back(FilterPushDown(child));
    }

    if(plan->GetType()== OperatorType::FilterOperatorNode 
    && plan->children_[0]->GetType()==
    OperatorType::SeqScanOperatorNode){
        auto& fp = plan->Cast<FilterLogicalOperator>();
        auto& sp = plan->children_[0]->Cast<SeqScanLogicalOperator>();
        auto p = std::move(fp.pridicator_);
        sp.pridicator_ = p;
        sp.SetOutputSchema(fp.GetOutPutSchema());
        return plan->children_[0];
    }
    //todo push to hash join
    auto op_p = plan->CopyWithChildren(std::move(children));
    return op_p;
}

