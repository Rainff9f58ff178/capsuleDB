#include "Execute/ExecutorNode/PhysicalOperator.h"
#include "Execute/ExecuteContext.h"
#include "common/commonfunc.h"
PhysicalOperator::PhysicalOperator(LogicalOperatorRef plan,
    ExecuteContext* context,
    std::vector<PhysicalOperatorRef> children)
:plan_(std::move(plan)),context_(context),
    children_(std::move(children)){
}

SourceResult
PhysicalOperator::Source(ChunkRef& bits){
    throw Exception("Unreachable PhyscialoOperators' source");
}

SinkResult
PhysicalOperator::Sink(ChunkRef& bits){
    throw Exception("Unreachable PhyscialoOperators' sink");

}
OperatorResult
PhysicalOperator::Execute(ChunkRef& bits){
    throw Exception("Unreachable PhyscialoOperators' Execute");

}

void 
PhysicalOperator::BuildPipeline(PipelineRef current,
ExecuteContext* context){  
    
    if(IsSink()){
        assert(children_.size()==1);
        current->source_= this;
        context->Build(children_[0],current);
    }else{
        //is a operator node or source node.
        if(children_.empty()){
            current->source_=this;
        }else{
            //has child_,it must has only one child  for now 
            assert(children_.size()==1);
            current->operators_.insert(current->operators_.begin(),this);
            children_[0]->BuildPipeline(current,context);
        }

    }
}

