#pragma once
#include "Execute/ExecutorNode/PhysicalOperator.h"



class MaterializePhysicalOperator: public PhysicalOperator{
    static constexpr const OperatorType type_ =
         OperatorType::MaterilizeOperatorNode;
public:
    MaterializePhysicalOperator(LogicalOperatorRef plan,ExecuteContext* context,
    std::vector<PhysicalOperatorRef> child):PhysicalOperator(std::move(plan),
    context,std::move(child)){}


    virtual SinkResult Sink(ChunkRef& bits) override;

    virtual SourceResult Source(ChunkRef& bits) override;
    
    virtual OperatorResult Execute(ChunkRef& bits) override;
    virtual bool IsSink() override{
        return false;
    }
    virtual OperatorType GetType() override{
        return type_;
    }
};