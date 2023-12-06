#include "Execute/ExecuteEngine.h"
#include "Execute/ExecuteContext.h"
#include "Execute/ExecutorNode/InsertPhysicalOperator.h"
#include "Execute/ExecutorNode/ValuesPhysicalOperator.h"
#include "Execute/ExecutorNode/MaterializePhysicalOperator.h"
#include "Execute/ExecutorNode/SeqScanPhysicalOperator.h"
#include "Execute/ExecutorNode/ResultPhysicalOperator.h"
#include "common/Exception.h"
ExecuteEngine::ExecuteEngine(){

}



void 
ExecuteEngine::Execute(LogicalOperatorRef plan,
std::shared_ptr<ExecuteContext> context){
    
    context->physical_plan_ = 
        CreatePhysicalOperatorTree(plan,context.get());

    context->physical_plan_ = std::shared_ptr<ResultPhysicalOperator>(
        new ResultPhysicalOperator(nullptr,context.get(),{context->physical_plan_})
    );


    context->Build(context->physical_plan_,nullptr);
    if(show_info) context->ShowPipelines();

    auto leaf_pipelines = GetAllNoChildPipelines(context);

    ExecuteInteranl(context,leaf_pipelines);

    // execute finished.
}


void 
ExecuteEngine::ExecuteInteranl(
std::shared_ptr<ExecuteContext> context,
std::vector<PipelineRef> leaf_pipelines){
    for(auto pipe: leaf_pipelines)
        ExecutePipeline(pipe);
}

OperatorResult ExecuteEngine::ExecutePush(PipelineRef& pipeline,ChunkRef& chunk){
    for(auto* o:pipeline->operators_){
        auto operator_result = o->Execute(chunk);
        if(operator_result == OperatorResult::FINISHED){
            return OperatorResult::FINISHED;
        }
    }
    SinkResult sink_result = pipeline->sink_->Sink(chunk);
    if(sink_result== SinkResult::FINISHED){
        return OperatorResult::FINISHED;
    }
    if(sink_result == SinkResult::NEED_MORE){
            return OperatorResult::NEED_MORE;
    }
    UNREACHABLE
}
void 
ExecuteEngine::ExecutePipeline(PipelineRef pipeline){
    if(pipeline->is_root_pipe_line_)
        return;
    
    DASSERT(!pipeline->runed_);

    // first execute init function.
    pipeline->source_->source_init();
    for(auto* o:pipeline->operators_){
        o->exceute_init();
    }
    if( pipeline->sink_ ) pipeline->sink_ ->sink_init();

    SourceResult source_result;
    bool exhausted_source = false;
    while(1){
        if(exhausted_source){
            break;
        }
        auto chunk =  std::make_shared<Chunk>();
        source_result =  FetchFromSource(pipeline->source_,chunk);
        if(source_result == SourceResult::FINISHED){
            exhausted_source = true;
            continue;
        }
        auto result = ExecutePush(pipeline,chunk);
        if(result==OperatorResult::FINISHED)
            break;
    }

    // execute uninit function .

    pipeline->source_->source_uninit();
    for(auto* o:pipeline->operators_){
        o->execute_uninit();
    }
    if ( pipeline->sink_)  pipeline->sink_ ->sink_uninit();



    auto parent = pipeline->Complete();
    if(parent!=nullptr)
        ExecutePipeline(parent);
    return;
}

SourceResult
ExecuteEngine::FetchFromSource(PhysicalOperator* source_node,
ChunkRef& chunk){
    return source_node->Source(chunk);
}

std::vector<PipelineRef>
ExecuteEngine::GetAllNoChildPipelines(   
std::shared_ptr<ExecuteContext> context){
   /*
        this function get the left pipelines ,execute until finish.
   */
   std::vector<PipelineRef> pipelines;
   for(auto& pipeline:context->pipelines_){
    if(pipeline->children_.empty())
        pipelines.push_back(pipeline);
   }
   return pipelines;
}

PhysicalOperatorRef
ExecuteEngine::CreatePhysicalOperatorTree(
LogicalOperatorRef plan,ExecuteContext* context){
    switch (plan->GetType()) {
        case LogicalOperatorNode:{
            throw Exception("Impossiable come here");
            break;
        }
        case PhysicalOperatorNode:{
            throw Exception("Impossiable come here");
            break;
        }
        case MaterilizeOperatorNode:{
            auto child= 
                CreatePhysicalOperatorTree(plan->children_[0],context);
            
            return  std::shared_ptr<MaterializePhysicalOperator>(
                new MaterializePhysicalOperator(std::move(plan),context,{child})
            );
        }
        case HashJoinOperatorNode:{
            break;
        }
        case FilterOperatorNode:{
            break;
        }
        case InsertOperatorNode:{
            //it just has one child.
            auto child =
                 CreatePhysicalOperatorTree(plan->children_[0],context);
            return std::shared_ptr<InsertPhysicalOperator>(
                new InsertPhysicalOperator(plan,context,std::move(child)));
            break;
        }
        case ValuesOperatorNode:{
            // it hasn't child 
            return std::shared_ptr<ValuesPhysicalOperator>(
                new ValuesPhysicalOperator(plan,context));
            break;
        }
        case SeqScanOperatorNode:{
            return  std::shared_ptr<SeqScanPhysicaloperator>(
                new SeqScanPhysicaloperator(std::move(plan),context,{})
            );
        }
        default:
            break;
    }    

    throw Exception("Not Implement for now");
}

