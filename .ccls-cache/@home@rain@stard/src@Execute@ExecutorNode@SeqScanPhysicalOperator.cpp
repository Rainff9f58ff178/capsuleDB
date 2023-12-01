#include "Execute/ExecutorNode/SeqScanPhysicalOperator.h"
#include "Planer/SeqScanLogicalOperator.h"
#include "Execute/ExecuteContext.h"

SourceResult
SeqScanPhysicaloperator::Source(ChunkRef& chunk){
    if(first_)
        LoadData();
    return SourceResult::HAVE_MORE;
}
void 
SeqScanPhysicaloperator::LoadData(){
    auto& plan = dynamic_cast<SeqScanLogicalOperator&>(*GetPlan());
    auto* table_c = context_->cata_log_->GetTable(plan.table_oid_);
    /*
        select colA,coLB from a ; 
        push colA and colB data to chunk;
    */
    auto& schema =  plan.ouput_schema_; 

    auto* tc = context_->cata_log_->GetTable(plan.table_oid_);
    
    auto total_num = tc->GetTotalObj();

    bitmap_.resize(total_num,true);
    
    if(plan.filter_pridicater_)
        plan.filter_pridicater_->Evalute(bitmap_,table_c);

}
SinkResult
SeqScanPhysicaloperator::Sink(ChunkRef& chunk){
    throw NotImplementedException("reach Impossiable");
}
OperatorResult
SeqScanPhysicaloperator::Execute(ChunkRef& chunk){
    throw NotImplementedException("reach Impossiable");
}
