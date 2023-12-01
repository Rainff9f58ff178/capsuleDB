#include "Execute/ExecutorNode/MaterializePhysicalOperator.h"
#include "Planer/MaterilizeLogicaloperator.h"


SinkResult
MaterializePhysicalOperator::Sink(ChunkRef& chunk){
  
  return SinkResult::NEED_MORE;

}

OperatorResult
MaterializePhysicalOperator::Execute(ChunkRef& chunk){
    return OperatorResult::NEED_MORE;
}
SourceResult
MaterializePhysicalOperator::Source(ChunkRef& chunk){
    UNREACHABLE
}