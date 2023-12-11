#include "Execute/ExecutorNode/MaterializePhysicalOperator.h"
#include "Planer/MaterilizeLogicaloperator.h"
#include "Execute/ExecutorNode/ExprExecutor.h"

SinkResult
MaterializePhysicalOperator::Sink(ChunkRef& chunk){
  
  return SinkResult::NEED_MORE;

}

OperatorResult
MaterializePhysicalOperator::Execute(ChunkRef& chunk){
    CHEKC_THORW(chunk);
    auto& plan = GetPlan()->Cast<MaterilizeLogicaloperator>();
    auto& exprs = plan.final_select_list_expr_;
    ChunkRef new_chunk = std::make_shared<Chunk>();
    for(auto& expr : exprs){
        ExprExecutor executor(expr,chunk);
        auto new_col = executor.execute(expr->toString());
        new_chunk->appendColumn(std::move(new_col));
    }
    chunk = std::move(new_chunk);
    
    return OperatorResult::NEED_MORE;
}
SourceResult
MaterializePhysicalOperator::Source(ChunkRef& chunk){
    UNREACHABLE
}