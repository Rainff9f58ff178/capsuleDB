#pragma once
#include "Execute/ExecutorNode/PhysicalOperator.h"


class SeqScanPhysicaloperator:public PhysicalOperator{
    static constexpr const OperatorType type_ =
         OperatorType::SeqScanOperatorNode;

    uint32_t offset_{0};
    std::vector<std::vector<Value>> cache_;
public:
    SeqScanPhysicaloperator(LogicalOperatorRef plan,
    ExecuteContext* context,
    std::vector<PhysicalOperatorRef> children):
        PhysicalOperator(std::move(plan),context,
        std::move(children)){}

    virtual SourceResult Source(ChunkRef& bits) override;
    virtual SinkResult Sink(ChunkRef& bits) override;
    virtual OperatorResult Execute(ChunkRef& bits) override;

    virtual OperatorType GetType() override{
        return type_;
    }    
    virtual bool IsSink() override{
        return false;
    }
private:
    void LoadData();
    std::vector<bool> bitmap_;
    bool first_{true};
};