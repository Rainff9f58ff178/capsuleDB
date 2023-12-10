

#pragma once
#include "Execute/ExecutorNode/PhysicalOperator.h"
#include "Planer/LogicalOperator.h"
#include "Execute/ExecuteContext.h"
#include "Table/Tuple.h"
#include "Execute/BitMap.h"
class ExecuteEngine{
public:

    ExecuteEngine();
    void Execute(LogicalOperatorRef plan,
    
    std::shared_ptr<ExecuteContext> context);
    void ExecuteExplain(LogicalOperatorRef plan,std::shared_ptr<ExecuteContext> context);
  

private:
    PhysicalOperatorRef
    CreatePhysicalOperatorTree(
        LogicalOperatorRef plan,
        ExecuteContext* context);

    std::vector<PipelineRef>
    GetAllNoChildPipelines(
        std::shared_ptr<ExecuteContext> context);

    void ExecuteInteranl(
        std::shared_ptr<ExecuteContext> context,
        std::vector<PipelineRef> leaf_pipelines);
    void ExecutePipeline(PipelineRef pipeline);
    OperatorResult ExecutePush(PipelineRef& pipeline,ChunkRef& chunk);


    
    SourceResult
    FetchFromSource(PhysicalOperator* source_node,
    ChunkRef& chunk);

};