#include "Execute/ExecutorNode/LimitPhysicalOperator.h"
#include "Planer/LimitLogicalOperator.h"


OperatorResult LimitPhysicalOperator::Execute(ChunkRef& chunk){
    UNREACHABLE;
}
SourceResult LimitPhysicalOperator::Source(ChunkRef& chunk){
    if(offset_ < chunks_.size()){
        chunk = std::move(chunks_[offset_++]);
        return  SourceResult::HAVE_MORE;
    }
    return  SourceResult::FINISHED;
}

void LimitPhysicalOperator::source_init(){
    auto plan = GetPlan()->Cast<LimitLogicalOperator>();
#ifndef NDEBUG
    uint32_t rows = 0;
    for(auto& chunk : chunks_){
        rows += chunk->rows();
    }
#endif
    // take it easier.
    ChunkRef final_b;
    for(uint32_t i=1;i<chunks_.size();++i){
        chunks_[0]->MergeBlock(chunks_[i].get());
    }
    final_b = std::move(chunks_[0]);
    chunks_.clear();

    auto dst_b = final_b->cloneEmpty();
    // no limit offset 100;
    if(plan.limit_ == 0 && plan.offset_ >0){
        for(uint32_t i=plan.offset_;i<final_b->rows();++i){
            dst_b->insertFrom(final_b.get(),i);
            if(dst_b->rows()==4096){
                chunks_.push_back(std::move(dst_b));
                dst_b = final_b->cloneEmpty();
            }
        }
        if(dst_b && dst_b->rows() > 0){
            chunks_.push_back(std::move(dst_b));
        }

        return;
    }

    // limit 1 offset 1
    for(uint32_t i=plan.offset_ ; i<plan.offset_+ plan.limit_;++i){
        dst_b->insertFrom(final_b.get(),i);
        if(dst_b->rows() == 4096){
            chunks_.push_back(std::move(dst_b));
            dst_b = final_b->cloneEmpty();
        }
    }
    if(dst_b && dst_b->rows() > 0){
        chunks_.push_back(std::move(dst_b));
    }
}
SinkResult LimitPhysicalOperator::Sink(ChunkRef& chunk){
    if(chunk->rows() == 0)
        return SinkResult::NEED_MORE;
    accumulate_ += chunk->rows();
    chunks_.push_back(std::move(chunk));
    auto plan = GetPlan()->Cast<LimitLogicalOperator>();
    if(plan.limit_ ==0 && plan.offset_>0)
        return  SinkResult::NEED_MORE;

    if( accumulate_ >= plan.limit_+ plan.offset_){
        return  SinkResult::FINISHED;   
    }
    return SinkResult::NEED_MORE;
}