#pragma once


#include "Execute/ExecutorNode/PhysicalOperator.h"
#include "Utils/Generator.h"

class HashJoinPhysicalOperator : public PhysicalOperator{
    static constexpr const OperatorType type_ = 
        OperatorType::HashJoinOperatorNode;
public:
    HashJoinPhysicalOperator(LogicalOperatorRef plan,ExecuteContext* context,
    std::vector<PhysicalOperatorRef> children):
        PhysicalOperator(std::move(plan),context,std::move(children)){

    }

    ~HashJoinPhysicalOperator(){}
    void sink_init() override;

    SourceResult Source(ChunkRef& chunk) override;
    SinkResult Sink(ChunkRef& chunk) override;
    OperatorResult Execute(ChunkRef& chunk) override;

    Generator<ChunkRef> ExecuteInteranl(ChunkRef chunk);


    
    OperatorType GetType() override{
        return type_;
    }
    bool IsSink() override{
        return true;
    }
    void BuildPipeline(PipelineRef current,ExecuteContext* context) override;

    // {chunk idx,line idx}
    using BuildEntry = std::pair<uint32_t,uint32_t>;
    
    Generator<ChunkRef> current_generator_;
    std::vector<std::vector<BuildEntry>> hash_table_;
    std::vector<ChunkRef> build_chunks_;
    uint32_t hash_table_size_=0;
};