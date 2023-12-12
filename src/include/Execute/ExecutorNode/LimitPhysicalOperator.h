#pragma once
#include "Execute/ExecutorNode/PhysicalOperator.h"



class LimitPhysicalOperator:public PhysicalOperator{
    static constexpr const OperatorType type_ 
        = OperatorType::LimitOperatorNode;
public:
    LimitPhysicalOperator(LogicalOperatorRef plan,ExecuteContext* context,
    std::vector<PhysicalOperatorRef> children):
        PhysicalOperator(std::move(plan),context,std::move(children)){

    }

    bool IsSink() override{
        return true;
    }
    OperatorType GetType() override{
        return  type_;
    }

    void source_init() override;
    SourceResult Source(ChunkRef& chunk) override;
    SinkResult Sink(ChunkRef& chunk) override;
    OperatorResult Execute(ChunkRef& chnk) override;
    
    
    std::vector<ChunkRef> chunks_;
    uint32_t offset_ = 0;
    uint32_t accumulate_=0;

};