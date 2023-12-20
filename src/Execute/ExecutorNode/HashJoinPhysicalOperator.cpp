#include "Execute/ExecutorNode/HashJoinPhysicalOperator.h"
#include "Planer/HashJoinLogicalOperator.h"
#include "Execute/ExecuteContext.h"
#include "Execute/ExecutorNode/ExprExecutor.h"
#include "Env.h"
#include "static/ScopeTimer.h"




HashJoinPhysicalOperator::HashJoinPhysicalOperator(LogicalOperatorRef plan,ExecuteContext* context,
std::vector<PhysicalOperatorRef> children):
    PhysicalOperator(std::move(plan),context,std::move(children)){
    profile_ = context->profile_->create_child(std::format("{}",getOperatorName(GetType())));
    build_timer_ = profile_->add_counter("build time");
    probe_timer =  profile_->add_counter("probe time");
}

SourceResult HashJoinPhysicalOperator::Source(ChunkRef& chunk){
    NOT_IMP
}   

SinkResult HashJoinPhysicalOperator::Sink(ChunkRef& chunk){
    SCOPED_TIMER(build_timer_);
    if(chunk->rows() == 0)
        return SinkResult::NEED_MORE;
    // Build port.
    CHEKC_THORW(hash_table_.size() > 0);

    auto& plan = GetPlan()->Cast<HashJoinLogicalOperator>();
    
    build_chunks_.push_back(chunk);
    auto chunk_idx = build_chunks_.size() -1;
    ExprExecutor executor(plan.condition_->children_[1],chunk);
    auto ref = executor.execute(plan.condition_->children_[1]->toString());
    for(uint32_t i=0;i<ref->rows();++i){
        auto bucket = ref->HashAt(i) % hash_table_size_;
        hash_table_[bucket].push_back({chunk_idx,i});
    }
    return SinkResult::NEED_MORE;
}

Generator<ChunkRef>
HashJoinPhysicalOperator::ExecuteInteranl(ChunkRef chunk){
    SCOPED_TIMER(probe_timer);
    auto& plan = GetPlan()->Cast<HashJoinLogicalOperator>();
    ExprExecutor executor(plan.condition_->children_[0],chunk);
    auto r_ref = executor.execute(plan.condition_->children_[0]->toString());

    auto left_chunk = build_chunks_[0]->cloneEmpty();
    auto right_chunk = chunk->cloneEmpty();
    for(uint32_t i=0;i<r_ref->rows();++i){
        auto bucket_id = r_ref->HashAt(i) % hash_table_size_;
        auto bucket = hash_table_[bucket_id];
        for(auto& pair : bucket ){
            auto result = plan.condition_->EvaluteJoin(&build_chunks_[pair.first],&chunk,pair.second,i);
            if(static_cast<bool>(result.val_.num_)){
                left_chunk->insertFrom( build_chunks_[pair.first].get(),pair.second);
                right_chunk->insertFrom( chunk.get(),i);
            }
            if(left_chunk->rows() == 4096){
                left_chunk->appendColumns(std::move(right_chunk->columns_));
                co_yield std::move(left_chunk);
                left_chunk = build_chunks_[0]->cloneEmpty();
                right_chunk = chunk->cloneEmpty();
            }
        }
    }
    left_chunk->appendColumns(std::move(right_chunk->columns_));
    co_return std::move(left_chunk);
}
OperatorResult HashJoinPhysicalOperator::Execute(ChunkRef& chunk){
    // probe port.
    if(!current_generator_){
        current_generator_ = ExecuteInteranl(chunk);
    }
    auto new_chunk = current_generator_();
    CHEKC_THORW(new_chunk);
    auto& plan = GetPlan()->Cast<HashJoinLogicalOperator>();
    if(new_chunk->rows() ==0){
        chunk=std::move( new_chunk);
    }
    else if(!plan.pridicators_.empty()){
        if(plan.pridicators_.size() >1 ){
            throw Exception("Not Support pridicator > 1");
        }
        std::vector<ValueUnion> new_value;
        auto& p = plan.pridicators_[0];
        for(uint32_t i=0;i<new_chunk->rows();++i){
            new_value.push_back(p->Evalute(&new_chunk,i).clone());
        }
        if(new_value[0].type_== ValueType::TypeString){
            // if expr result is string /
            new_value.clear();
            for(uint32_t i=0;i<new_chunk->rows();++i){
                new_value.emplace_back(1);
            }
        }

        auto filter = std::dynamic_pointer_cast<ColumnVector<int32_t>>(ColumnFactory::CreateColumn(new_value));
        auto& data = filter->data_;
        CHEKC_THORW(data.size() == new_chunk->rows());
        auto filterd_chunk = new_chunk->cloneEmpty();
        for(uint32_t i=0;i<data.size();++i){
            if(static_cast<bool>(data[i])){
                filterd_chunk->insertFrom(new_chunk.get(),i);
            }
        }
        chunk = std::move(filterd_chunk);
    }else 
        chunk = std::move(new_chunk);


    if(current_generator_.finish()){
        return OperatorResult::NEED_MORE;
    }
    return OperatorResult::HAVE_MORE;

    // if(current_generator_.finish()){
    //     return  OperatorResult::NEED_MORE;
    // }
    // return  OperatorResult::HAVE_MORE;
    // auto& plan = GetPlan()->Cast<HashJoinLogicalOperator>();
    
    // ExprExecutor executor(plan.condition_->children_[1],chunk);
    // auto r_ref = executor.execute(plan.condition_->children_[1]->toString());

    // auto left_chunk = build_chunks_[0]->cloneEmpty();
    // auto right_chunk = chunk->cloneEmpty();

    // for(uint32_t i=0;i<r_ref->rows();++i){
    //     auto bucket_id = r_ref->HashAt(i) % hash_table_size_;
    //     auto bucket = hash_table_[bucket_id];
    //     for(auto& pair : bucket ){
    //         auto result = plan.condition_->EvaluteJoin(&build_chunks_[pair.first],&chunk,pair.second,i);
    //         if(static_cast<bool>(result.num_)){
    //             left_chunk->insertFrom( build_chunks_[pair.first].get(),pair.second);
    //             right_chunk->insertFrom( chunk.get(),i);
    //         }
    //     }
    // }
    // left_chunk->appendColumns(std::move(right_chunk->columns_));
    
    // chunk = std::move(left_chunk);
    // return OperatorResult::NEED_MORE;
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
    children_[1]->BuildPipeline(new_pipeline,context);
    children_[0]->BuildPipeline(current,context);
}
