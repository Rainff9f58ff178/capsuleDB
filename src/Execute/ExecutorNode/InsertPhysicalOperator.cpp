
#include "Execute/ExecutorNode/InsertPhysicalOperator.h"
#include "Planer/ValuesLogicalOperator.h"
#include "Planer/InsertLogicalOperator.h"
#include "Execute/ExecuteContext.h"
#include "CataLog/TableCataLog.h"
InsertPhysicalOperator::InsertPhysicalOperator(
    LogicalOperatorRef plan,
    ExecuteContext* context,
    PhysicalOperatorRef child
):PhysicalOperator(std::move(plan),context,{std::move(child)}){
    
}


/*
    Get ConstVlauesExpress arr from child node
*/
SinkResult
InsertPhysicalOperator::Sink(ChunkRef& chunk){
    auto& plan = dynamic_cast<InsertLogicalOperator&>(*GetPlan());
    DASSERT(chunk->columns() == plan.GetInputSchema()->columns_.size());
    auto* t_c = context_->cata_log_->GetTable(plan.inserted_table_);

    for(uint32_t i=0;i<chunk->columns();++i){
        chunk->columns_[i]->insertToTable(t_c,i);
    }
    num_insert_ += chunk->rows();
    return SinkResult::NEED_MORE;
}



SourceResult
InsertPhysicalOperator::Source(ChunkRef& chunk){
    if(sourced_)
        return  SourceResult::FINISHED;
    auto& col_name = GetPlan()->Cast<InsertLogicalOperator&>().GetOutPutSchema()->columns_[0].name_;
    auto col = std::make_shared<ColumnVector<uint32_t>>(col_name);
    col->data_.push_back(num_insert_);
    chunk->appendColumn(std::move(col));
    sourced_ = true;
    return SourceResult::HAVE_MORE;
}
OperatorResult
InsertPhysicalOperator::Execute(ChunkRef& bits){
    THIS_NODE_SHOULDNT_BE("Operator");
}