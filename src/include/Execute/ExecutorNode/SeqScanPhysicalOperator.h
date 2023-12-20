#pragma once
#include "Execute/ExecutorNode/PhysicalOperator.h"
#include "CataLog/PageIterator/PageIterator.h"
class SeqScanPhysicaloperator:public PhysicalOperator{
    static constexpr const OperatorType type_ =
         OperatorType::SeqScanOperatorNode;

    uint32_t offset_{0};
    std::vector<std::vector<Value>> cache_;
public:
    SeqScanPhysicaloperator(LogicalOperatorRef plan,
    ExecuteContext* context,
    std::vector<PhysicalOperatorRef> children);

    ~SeqScanPhysicaloperator();

    virtual void source_init() override;
    virtual void source_uninit() override;
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
    RuntimeProfile::Counter* load_timer_ = nullptr;
    RuntimeProfile::Counter* filter_timer = nullptr;

    std::vector<std::unique_ptr<ColumnIterator>> column_iterators_; 
};

