#include "Execute/ExecutorNode/MaterializePhysicalOperator.h"
#include "Planer/MaterilizeLogicaloperator.h"
#include "Execute/ExecutorNode/ExprExecutor.h"
#include "Execute/ExecuteContext.h"


MaterializePhysicalOperator::MaterializePhysicalOperator(LogicalOperatorRef plan,ExecuteContext* context,
std::vector<PhysicalOperatorRef> child):PhysicalOperator(std::move(plan),
context,std::move(child)){
    profile_ = context->profile_->create_child(std::format("{}",getOperatorName(GetType())));
    expr_execute_timer_ = profile_->add_counter("expr execute time");
}


SinkResult
MaterializePhysicalOperator::Sink(ChunkRef& chunk){
  
  return SinkResult::NEED_MORE;

}

OperatorResult
MaterializePhysicalOperator::Execute(ChunkRef& chunk){
    CHEKC_THORW(chunk);
    SCOPED_TIMER(expr_execute_timer_);
    auto& plan = GetPlan()->Cast<MaterilizeLogicaloperator>();
    CHEKC_THORW (chunk->columns() == plan.GetInputSchema()->columns_.size())
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