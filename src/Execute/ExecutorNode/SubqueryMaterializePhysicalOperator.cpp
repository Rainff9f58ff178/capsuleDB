#include "Execute/ExecutorNode/SubqueryMaterializePhysicalOperator.h"
#include "Planer/SubqueryMaterializeLogicalOperator.h"





SourceResult SubqueryMaterializePhysicalOperator::Source(ChunkRef& chunk){
    UNREACHABLE;
}



SinkResult SubqueryMaterializePhysicalOperator::Sink(ChunkRef& chunk){
    UNREACHABLE;
}

OperatorResult SubqueryMaterializePhysicalOperator::Execute(ChunkRef& chunk){
    if(chunk->rows() == 0)
        return OperatorResult::HAVE_MORE;
    auto r = MaterializePhysicalOperator::Execute(chunk);
    //change its name .
    auto& plan = GetPlan()->Cast<SubqueryMaterializeLogicalOperator>();

    CHEKC_THORW(chunk->columns() == plan.GetOutPutSchema()->columns_.size());
    for(uint32_t i=0;i<plan.GetOutPutSchema()->columns_.size();++i){
        chunk->columns_[i]->name_ = plan.GetOutPutSchema()->columns_[i].name_;
    }
    return r;    
}