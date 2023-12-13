#include "Execute/ExecutorNode/SortPhyscialOperator.h"
#include "Execute/ExecutorNode/ExprExecutor.h"
#include "Planer/SortLogicalOperator.h"



OperatorResult SortPhysicalOperator::Execute(ChunkRef& chunk){
    UNREACHABLE;
}




SinkResult SortPhysicalOperator::Sink(ChunkRef& chunk) {
    if(!cache_chunk_)   {
        cache_chunk_ = std::move(chunk);
    }else {
        if(chunk->rows() == 0)
            return  SinkResult::NEED_MORE;

        cache_chunk_->MergeBlock(chunk.get());
    }
    return SinkResult::NEED_MORE;
}

SourceResult SortPhysicalOperator::Source(ChunkRef& chunk){
    if(offset_ < sorted_chunks_.size()){
        chunk = std::move(sorted_chunks_[offset_++]);
        return SourceResult::HAVE_MORE;
    }
    return  SourceResult::FINISHED;
}


void SortPhysicalOperator::source_init(){
    // sort this chunk.
    std::vector<SortEntry> sorts;
    auto plan = GetPlan()->Cast<SortLogicalOperator>();
    for(auto& order_by : plan.order_bys_){
        ExprExecutor executor(order_by.second,cache_chunk_);
        auto col = executor.execute(order_by.second->toString());
        sorts.emplace_back(order_by.first,order_by.second->toString(),col);
        cache_chunk_->appendColumn(std::move(col));
    }
    SortBlock(cache_chunk_,sorts);
    // split it .

    auto small_chunk = cache_chunk_->cloneEmpty();

    for(uint32_t i=0;i<cache_chunk_->rows();++i){
        small_chunk->insertFrom(cache_chunk_.get(),i);
        if(small_chunk->rows() == 4096){
            sorted_chunks_.push_back(std::move(small_chunk));
            small_chunk = cache_chunk_->cloneEmpty();
        }
    }
    CHEKC_THORW(small_chunk->rows() < 4096);
    sorted_chunks_.push_back(std::move(small_chunk));
}

void SortPhysicalOperator::
SortBlock(ChunkRef& chunk,std::vector<SortEntry> sorts){
    RowNumbers row_number;
    row_number.resize(chunk->rows());
    for(uint32_t i=0;i<chunk->rows();++i){
        row_number[i] = i;
    }
    
    auto cmp = [&,this](uint32_t l_idx, uint32_t r_idx){
        for(auto& sort : sorts){
            int r = sort.sort_column_->compare_at(sort.sort_column_.get(),l_idx,r_idx);
            if( r == 0)
                continue;
            int d = sort.type_== OrderByType::ASC ? -1 : 1;
            r = r*d;
            if(r>0)
                return true;
            return false;
        }
        // equal 
        return  l_idx>r_idx;
    };

    std::sort(row_number.begin(),row_number.end(),cmp);

    ChunkRef sorted_chunk = std::make_shared<Chunk>();

    for(uint32_t i=0;i<chunk->columns_.size();++i){
        auto new_col = chunk->columns_[i]->GetByRowNumbers(row_number);
        sorted_chunk->appendColumn(std::move(new_col));
    }
    chunk = std::move(sorted_chunk);
}