#pragma once



#include "Execute/ExecutorNode/PhysicalOperator.h"


class FilterPhysicalOperator:public PhysicalOperator{
    static constexpr const OperatorType type_ = 
        OperatorType::FilterOperatorNode;
public:
    FilterPhysicalOperator(LogicalOperatorRef plan,
    ExecuteContext* context,std::vector<PhysicalOperatorRef> child);


    bool IsSink() override{
        return false;
    }
    OperatorType GetType() override{
        return type_;
    }


    SourceResult Source(ChunkRef &chunk) override;
    SinkResult Sink(ChunkRef &chunk) override;
    OperatorResult Execute(ChunkRef &chunk) override;


    RuntimeProfile::Counter* filter_timer_ = nullptr;
};