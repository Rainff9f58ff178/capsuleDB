#include "Execute/ExecutorNode/SubqueryMaterializePhysicalOperator.h"
#include "Planer/SubqueryMaterializeLogicalOperator.h"




SubqueryMaterializePhysicalOperator:: SubqueryMaterializePhysicalOperator(LogicalOperatorRef plan,
    ExecuteContext* context,
    std::vector<PhysicalOperatorRef> children):
        MaterializePhysicalOperator(std::move(plan),context,std::move(children)){
}

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
    chunk->map_.clear();
    for(uint32_t i=0;i<plan.GetOutPutSchema()->columns_.size();++i){
        chunk->columns_[i]->name_ = plan.GetOutPutSchema()->columns_[i].name_;
        chunk->map_[chunk->columns_[i]->name_] = chunk->columns_[i];
    }
    return r;    
}