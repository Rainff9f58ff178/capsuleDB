#include "Execute/ExecutorNode/HashJoinPhysicalOperator.h"
#include "Planer/HashJoinLogicalOperator.h"
#include "Execute/ExecuteContext.h"
#include "Execute/ExecutorNode/ExprExecutor.h"
#include "Env.h"




SourceResult HashJoinPhysicalOperator::Source(ChunkRef& chunk){
    NOT_IMP
}   

SinkResult HashJoinPhysicalOperator::Sink(ChunkRef& chunk){
    // Build port.
    CHEKC_THORW(hash_table_.size() > 0);

    auto& plan = GetPlan()->Cast<HashJoinLogicalOperator>();
    
    build_chunks_.push_back(chunk);
    auto chunk_idx = build_chunks_.size() -1;
    ExprExecutor executor(plan.condition_->children_[0],chunk);
    auto ref = executor.execute(plan.condition_->children_[0]->toString());
    for(uint32_t i=0;i<ref->rows();++i){
        auto bucket = ref->HashAt(i) % hash_table_size_;
        hash_table_[bucket].push_back({chunk_idx,i});
    }
    return SinkResult::NEED_MORE;
}

OperatorResult HashJoinPhysicalOperator::Execute(ChunkRef& chunk){
    // probe port.
    auto& plan = GetPlan()->Cast<HashJoinLogicalOperator>();
    
    ExprExecutor executor(plan.condition_->children_[1],chunk);
    auto r_ref = executor.execute(plan.condition_->children_[1]->toString());

    auto left_chunk = build_chunks_[0]->cloneEmpty();
    auto right_chunk = chunk->cloneEmpty();

    for(uint32_t i=0;i<r_ref->rows();++i){
        auto bucket_id = r_ref->HashAt(i) % hash_table_size_;
        auto bucket = hash_table_[bucket_id];
        for(auto& pair : bucket ){
            auto result = plan.condition_->EvaluteJoin(&build_chunks_[pair.first],&chunk,pair.second,i);
            if(static_cast<bool>(result.num_)){
                left_chunk->insertFrom( build_chunks_[pair.first].get(),pair.second);
                right_chunk->insertFrom( chunk.get(),i);
            }
        }
    }
    left_chunk->appendColumns(std::move(right_chunk->columns_));
    
    chunk = std::move(left_chunk);
    return OperatorResult::NEED_MORE;
}
void HashJoinPhysicalOperator::sink_init(){
    // init hash table.
    hash_table_size_ = JOIN_HASH_TABLE_SIZE;
    hash_table_.resize(hash_table_size_);
   
}

void HashJoinPhysicalOperator::
BuildPipeline(PipelineRef current,ExecuteContext* context){
    // this node as current node opreator,and same pipeline with right node.
    // create a new child pipeline for left child , this node is its sink.
    
    current->operators_.insert(current->operators_.begin(),this);
    auto new_pipeline = std::make_shared<Pipeline>(context,context->id_++);   
    current->children_.push_back(new_pipeline);
    current->total_children_++;
    new_pipeline->parent_ = std::move(std::weak_ptr<Pipeline>(current));
    new_pipeline->sink_  = this;
    context->pipelines_.push_back(new_pipeline);
    children_[0]->BuildPipeline(new_pipeline,context);
    children_[1]->BuildPipeline(current,context);
}
