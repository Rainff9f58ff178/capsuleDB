

#include "Execute/ExecutorNode/ValuesPhysicalOperator.h"
#include "Execute/core/ColumnFactory.h"

ValuesPhysicalOperator::ValuesPhysicalOperator(
    LogicalOperatorRef plan,
    ExecuteContext* context
):PhysicalOperator(std::move(plan),context,{}){

}

OperatorType 
ValuesPhysicalOperator::GetType(){
    return type_;
}

/*
    this function should awayls return bitMap that all true.
*/
SourceResult 
ValuesPhysicalOperator::Source(ChunkRef& chunk){
    auto& plan = reinterpret_cast<ValuesLogicalOperator&>(*GetPlan());
    if(plan.GetOutPutSchema()->columns_.size() != plan.all_values_[0].size())
        throw Exception("Values operator values doest not equal output schema");
    auto total_values = plan.all_values_.size();
    if(num_ >= total_values)
        return SourceResult::FINISHED;
    
        
    uint32_t i =0;

    ColumnRefs columns;

    for(uint32_t i=0;i<plan.all_values_[0].size();++i){
        std::vector<ValueUnion> values;
        auto value = plan.all_values_[0][i]->Evalute(nullptr,0);
        if(value.type_==ValueType::TypeInt){
            columns.push_back(ColumnFactory::CreateColumnVector<int32_t>(plan.GetOutPutSchema()->columns_[i].name_));
        }else if(value.type_ == ValueType::TypeString){
            columns.push_back(ColumnFactory::CreateColumnString(plan.GetOutPutSchema()->columns_[i].name_));
        }else{
            DASSERT(0);
        }
        for(uint32_t j=0;j<ONE_DATA_CHUNK_SIZE && j+num_<total_values;++j){
            values.push_back(plan.all_values_[j+num_][i]->Evalute(nullptr,0));
        }
        columns.back()->insertFrom(ValueUnionView(values));
    }
#ifndef  NDEBUG

    for(auto i=1;i<columns.size();++i){
        DASSERT(columns[i-1]->rows() == columns[i]->rows());
    }
#endif
    num_ += columns[0]->rows();
    chunk->appendColumns(std::move(columns));
    return SourceResult::HAVE_MORE;
}

SinkResult
ValuesPhysicalOperator::Sink(ChunkRef& bits){
    throw Exception("Impossitable call VlauesOperaors's Sink,this node couldn't be sink node");
}

OperatorResult
ValuesPhysicalOperator::Execute(ChunkRef& bits){
    throw Exception("ValuesNode cant be OperaorNode");
}