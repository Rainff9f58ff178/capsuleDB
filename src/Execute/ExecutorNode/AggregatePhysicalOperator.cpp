#include "Execute/ExecutorNode/AggregatePhysicalOperator.h"
#include "Planer/AggregateLogicalOperator.h"

#include "Execute/ExecutorNode/ExprExecutor.h"





void AggregatePhysicalOperator::source_init(){
    // calculate for all chunk in agg_map_;
    auto& plan = GetPlan()->Cast<AggregateLogicalOperator>();
    // Craete Result Block.
    ChunkRef final_chunk  = std::make_shared<Chunk>();
    for(auto& agg : plan.aggs_){
        auto col = ColumnFactory::CreateColumn(ColumnType::INT,agg.agg_result_name);
        final_chunk->appendColumn(std::move(col));
    }
    for(auto& gp : plan.group_bys_){
        auto col = ColumnFactory::CreateColumn(gp->GetReturnType(),gp->toString());
        final_chunk->appendColumn(std::move(col));
    }

    auto curernt_chunk = final_chunk->cloneEmpty();
    for(auto it : agg_map_){
        auto aggs = __CalculateBucket(it.second);
        // in the last,add group by expression result.
        if(!plan.group_by_cols_.empty()){
            aggs.insert(aggs.end(),it.first.group_by_val_.begin(),it.first.group_by_val_.end());
        }
        curernt_chunk->insertFrom(ValueUnionView(aggs));
        if(curernt_chunk->rows() == 4096){
            chunks_.push_back(std::move(curernt_chunk));
            curernt_chunk = final_chunk->cloneEmpty();
        }
    }
    if(curernt_chunk->rows() > 0 ){
        chunks_.push_back(std::move(curernt_chunk));
    }
}

std::vector<ValueUnion> 
AggregatePhysicalOperator::__CalculateBucket(ChunkRef& chunk){
    auto& plan = GetPlan()->Cast<AggregateLogicalOperator>();
    std::vector<ValueUnion> result;
    for(auto& agg : plan.aggs_){
        if(agg.args_expr_.size() > 1)
            throw Exception("Not suppor agg parametets > 1 ");

        switch(agg.agg_type_){
            case  AggregationType::CountStarAggregate:{
                ExprExecutor exec(agg.args_expr_[0],chunk);
                auto col = exec.execute(agg.args_expr_[0]->toString());
                result.push_back( col->agg_count());
                break;
            }
            case AggregationType::CountAggregate:{
                ExprExecutor exec(agg.args_expr_[0],chunk);
                auto col = exec.execute(agg.args_expr_[0]->toString());
                result.push_back( col->agg_count());
                break;;
            }
            case AggregationType::SumAggregate:{
                ExprExecutor exec(agg.args_expr_[0],chunk);
                auto col = exec.execute(agg.args_expr_[0]->toString());
                result.push_back( col->agg_sum ()); 
                break;;
            }
            case AggregationType::MinAggregate:{
                ExprExecutor exec(agg.args_expr_[0],chunk);
                auto col = exec.execute(agg.args_expr_[0]->toString());
                result.push_back(col->agg_min());
                break;;
            }
            case AggregationType::MaxAggregate:{
                ExprExecutor exec(agg.args_expr_[0],chunk);
                auto col = exec.execute(agg.args_expr_[0]->toString());
                result.push_back(col->agg_max());
                break;;
            }
            case AggregationType::AvgAggregate:{
                ExprExecutor exec(agg.args_expr_[0],chunk);
                auto col = exec.execute(agg.args_expr_[0]->toString());
                result.push_back(col->agg_avg());
                break;;
            }
            default:
                NOT_IMP;
        }
    }
    return  result;
}

SourceResult AggregatePhysicalOperator::Source(ChunkRef& chunk) {
    if(offset_ < chunks_.size()){
        chunk = std::move(chunks_[offset_++]);
        return  SourceResult::HAVE_MORE;
    }
    return SourceResult::FINISHED;
}


SinkResult AggregatePhysicalOperator::Sink(ChunkRef& chunk){
    // hash it by 'group by expression'
    if(chunk->rows() == 0)
        return  SinkResult::NEED_MORE;

    auto& plan = GetPlan()->Cast<AggregateLogicalOperator>();
    for(uint32_t i=0;i<chunk->rows();++i){
        std::vector<ValueUnion> group_bys;
        for(auto& expr : plan.group_bys_){
            auto val = expr->Evalute(&chunk,i);
            group_bys.push_back(val.clone());
        }
        AggregateKey key(std::move(group_bys));

        if(agg_map_.count(key) ==0){
            agg_map_[key]  = chunk->cloneEmpty();
        }
        agg_map_[key]->insertFrom(chunk.get(),i);
    }
    return  SinkResult::NEED_MORE;
}

OperatorResult AggregatePhysicalOperator::Execute(ChunkRef& chunk){
    UNREACHABLE;
}