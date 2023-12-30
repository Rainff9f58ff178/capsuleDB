#include "Execute/ExecuteEngine.h"
#include "Execute/ExecuteContext.h"
#include "Execute/ExecutorNode/InsertPhysicalOperator.h"
#include "Execute/ExecutorNode/ValuesPhysicalOperator.h"
#include "Execute/ExecutorNode/MaterializePhysicalOperator.h"
#include "Execute/ExecutorNode/SeqScanPhysicalOperator.h"
#include "Execute/ExecutorNode/ResultPhysicalOperator.h"
#include "Execute/ExecutorNode/HashJoinPhysicalOperator.h"
#include "Execute/ExecutorNode/LimitPhysicalOperator.h"
#include "Execute/ExecutorNode/SortPhyscialOperator.h"
#include "Execute/ExecutorNode/AggregatePhysicalOperator.h"
#include "Execute/ExecutorNode/FilterPhysicalOperator.h"
#include "Execute/ExecutorNode/SubqueryMaterializePhysicalOperator.h"
#include "common/Exception.h"
#include "static/ScopeTimer.h"
#include<stack>
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

    for(auto& p : context->pipelines_){
        p->profile_ = context->profile_->create_child(std::format("pipeline {}",p->identify_));
        p->pipeline_execute_time_ = p->profile_->add_counter("pipeline_execute_time");
    }

    auto leaf_pipelines = GetAllNoChildPipelines(context);

    ExecuteInteranl(context,leaf_pipelines);

    // execute finished.print profile 
    DEBUG("\n"+context->profile_->toString(0));
}

void ExecuteEngine::ExecuteExplain(LogicalOperatorRef plan,
std::shared_ptr<ExecuteContext> context,std::stringstream& ss){
    context->physical_plan_ = 
        CreatePhysicalOperatorTree(plan,context.get());

    context->physical_plan_ = std::shared_ptr<ResultPhysicalOperator>(
        new ResultPhysicalOperator(nullptr,context.get(),{context->physical_plan_})
    );
    context->Build(context->physical_plan_,nullptr);

    for(auto& p : context->pipelines_){
        p->profile_ = context->profile_->create_child(std::format("pipeline {}",p->identify_));
        p->pipeline_execute_time_ = p->profile_->add_counter("pipeline_execute_time");
    }
    
    ss<<context->ShowPipelines();
    // not execute it.
}

void 
ExecuteEngine::ExecuteInteranl(
std::shared_ptr<ExecuteContext> context,
std::vector<PipelineRef> leaf_pipelines){
    for(auto pipe: leaf_pipelines)
        ExecutePipeline(pipe);
}

OperatorResult ExecuteEngine::ExecutePush(PipelineRef& pipeline,ChunkRef& chunk){

    uint32_t current_op_idx=0;
    auto& operators = pipeline->operators_;
    ChunkRef current_chunk = chunk;
    std::stack<uint32_t> stack;
    while(1){
        for(uint32_t i= current_op_idx ;i<operators.size();++i){
            auto* o = operators[i];
            auto result = o->Execute(current_chunk);
            o->CleanUpSurplusColumn(current_chunk);
            if(result==OperatorResult::FINISHED){
                return OperatorResult::FINISHED;
            }
            if(result == OperatorResult::HAVE_MORE){
                stack.push(i);
            }
        }

        SinkResult sink_result = pipeline->sink_->Sink(current_chunk);
        if(sink_result== SinkResult::FINISHED){
            return OperatorResult::FINISHED;
        }
        if(sink_result == SinkResult::NEED_MORE && stack.size() == 0){
            return OperatorResult::NEED_MORE;
        }
        current_op_idx = stack.top();
        stack.pop();
        current_chunk = nullptr;
    }
    UNREACHABLE;
}
void 
ExecuteEngine::ExecutePipeline(PipelineRef pipeline){
    if(pipeline->is_root_pipe_line_)
        return;
    
    DASSERT(!pipeline->runed_);
    {
    SCOPED_TIMER(pipeline->pipeline_execute_time_);
    
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
    }



    auto parent = pipeline->Complete();
    
    if(parent!=nullptr)
        ExecutePipeline(parent);
    return;
}

SourceResult
ExecuteEngine::FetchFromSource(PhysicalOperator* source_node,
ChunkRef& chunk){
    while(1){
        auto result =  source_node->Source(chunk);
        if(result == SourceResult::HAVE_MORE && chunk->rows()==0)
            continue;
        source_node->CleanUpSurplusColumn(chunk);
        return result;
    }
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
        case SubqueryMaterializeOperatorNode:{
            auto child = CreatePhysicalOperatorTree(plan->children_[0],context);
            
            return std::shared_ptr<SubqueryMaterializePhysicalOperator>(
                new SubqueryMaterializePhysicalOperator(std::move(plan),context,{child})
            );
        }
        case AggOperatorNode:{
            auto child =  CreatePhysicalOperatorTree(plan->children_[0],context);
            return std::shared_ptr<AggregatePhysicalOperator>(
                new AggregatePhysicalOperator(std::move(plan),context,{child})
            );
        }
        case SortOperatorNode:{
            auto child = CreatePhysicalOperatorTree(plan->children_[0],context);
            return std::shared_ptr<SortPhysicalOperator>(
                new SortPhysicalOperator(std::move(plan),context,{child})
            );
        }
        case LimitOperatorNode:{
            auto child = CreatePhysicalOperatorTree(plan->children_[0],context);
            return std::shared_ptr<LimitPhysicalOperator>(
                new LimitPhysicalOperator(std::move(plan),context,{child})
            );
        }
        case HashJoinOperatorNode:{
            auto l_child = CreatePhysicalOperatorTree(plan->children_[0],context);
            auto r_child = CreatePhysicalOperatorTree(plan->children_[1],context);
            return  std::shared_ptr<HashJoinPhysicalOperator>(
                new HashJoinPhysicalOperator(plan,context,{l_child,r_child})
            );
            break;
        }
        case FilterOperatorNode:{
            auto child = CreatePhysicalOperatorTree(plan->children_[0],context);
            return std::shared_ptr<FilterPhysicalOperator>(
                new FilterPhysicalOperator(plan,context,{child})
            );
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

