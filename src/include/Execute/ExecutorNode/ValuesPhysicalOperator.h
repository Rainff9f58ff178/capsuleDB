#pragma once


#include "Execute/ExecutorNode/PhysicalOperator.h"
#include "Planer/ValuesLogicalOperator.h"

class ValuesPhysicalOperator:public PhysicalOperator{
    static constexpr const OperatorType type_ = 
        OperatorType::ValuesOperatorNode;
public:
    
    ValuesPhysicalOperator(LogicalOperatorRef plan,
        ExecuteContext* context);

    virtual SourceResult Source(ChunkRef& chunk) override;
    virtual SinkResult Sink(ChunkRef& chunk) override;
    virtual OperatorResult Execute(ChunkRef& chunk) override;

    
    virtual OperatorType GetType() override;
    virtual bool IsSink() override{
        return false;
    }
    
    // this member record current return bit map row_idx;
    uint num_{0};
    // this node shouldnt has chidl node.
};
