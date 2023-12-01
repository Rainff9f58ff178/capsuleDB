#pragma once


#include "Execute/ExecutorNode/PhysicalOperator.h"





class ResultPhysicalOperator:public PhysicalOperator{

    static constexpr const OperatorType type_ = 
        OperatorType::ResultOperatorNode;
public:

    explicit ResultPhysicalOperator(LogicalOperatorRef plan,
        ExecuteContext* context,
        std::vector<PhysicalOperatorRef> children);
    
    bool IsSink() override{
        return true;
    }
    SourceResult Source(ChunkRef& chunk) override;
    SinkResult Sink(ChunkRef& chunk) override;
    OperatorResult Execute(ChunkRef& chunk) override;



    OperatorType GetType() override{
        return type_;   
    }


private:
    std::vector<ChunkRef> result;
    uint32_t offset_{0}; // use in source
};