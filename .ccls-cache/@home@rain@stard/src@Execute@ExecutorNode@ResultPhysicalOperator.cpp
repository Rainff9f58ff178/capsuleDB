#include "Execute/ExecutorNode/ResultPhysicalOperator.h"






ResultPhysicalOperator::ResultPhysicalOperator(LogicalOperatorRef plan,
ExecuteContext* context,
std::vector<PhysicalOperatorRef> children):PhysicalOperator(plan,context,std::move(children)){

}



SourceResult ResultPhysicalOperator::Source(ChunkRef& chunk){
    if(offset_ >= result.size())
        return SourceResult::FINISHED;
    chunk = std::move(result[offset_++]);
    return SourceResult::HAVE_MORE;
}
SinkResult ResultPhysicalOperator::Sink(ChunkRef& chunk){
    result.push_back(std::move(chunk));
    return SinkResult::NEED_MORE;
}
OperatorResult ResultPhysicalOperator::Execute(ChunkRef& chunk){
    UNREACHABLE
}