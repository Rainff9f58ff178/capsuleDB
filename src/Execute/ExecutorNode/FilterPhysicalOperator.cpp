#include "Execute/ExecutorNode/FilterPhysicalOperator.h"
#include "Planer/FilterLogicalOperator.h"





SourceResult FilterPhysicalOperator::Source(ChunkRef& chunk){
    UNREACHABLE;
}

SinkResult FilterPhysicalOperator::Sink(ChunkRef& chunk){
    UNREACHABLE;
}


OperatorResult FilterPhysicalOperator::Execute(ChunkRef& chunk){

    if(chunk->rows() == 0)
        return OperatorResult::NEED_MORE;
    
    auto& plan = GetPlan()->Cast<FilterLogicalOperator>();

    CHEKC_THORW(!plan.pridicator_.empty());

    if(plan.pridicator_.size() > 1){
        throw Exception("Not Support pricator > 1");
    }


    std::vector<ValueUnion> new_value;
    auto& p = plan.pridicator_[0];
    for(uint32_t i=0;i<chunk->rows();++i){
        new_value.push_back(p->Evalute(&chunk,i).clone());
    }
    if(new_value[0].type_== ValueType::TypeString){
        // if expr result is string /
        new_value.clear();
        for(uint32_t i=0;i<chunk->rows();++i){
            new_value.emplace_back(1);
        }
    }

    auto filter = std::dynamic_pointer_cast<ColumnVector<int32_t>>(ColumnFactory::CreateColumn(new_value));
    auto& data = filter->data_;
    CHEKC_THORW(data.size() == chunk->rows());
    auto filterd_chunk = chunk->cloneEmpty();
    for(uint32_t i=0;i<data.size();++i){
        if(static_cast<bool>(data[i])){
            filterd_chunk->insertFrom(chunk.get(),i);
        }
    }
    chunk = std::move(filterd_chunk);
    return OperatorResult::NEED_MORE;
}