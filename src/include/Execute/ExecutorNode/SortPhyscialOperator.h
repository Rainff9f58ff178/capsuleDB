#include "Execute/ExecutorNode/PhysicalOperator.h"


struct SortEntry{
    OrderByType type_;
    std::string col_name_;
    ColumnRef sort_column_;
};


class SortPhysicalOperator:public PhysicalOperator{
    static constexpr const OperatorType type_ =     
        SortOperatorNode;
    
public:
    
    SortPhysicalOperator(LogicalOperatorRef plan,ExecuteContext* context,
    std::vector<PhysicalOperatorRef> children);


    bool IsSink() override{
        return true;
    }

    OperatorType GetType() override{
        return  type_;
    }

    SourceResult Source(ChunkRef& chunk) override;
    SinkResult Sink(ChunkRef& chunk) override;
    
    void source_init() override;

    ChunkRef cache_chunk_{nullptr};
    OperatorResult Execute(ChunkRef& chunk) override;

private:
    std::vector<ChunkRef> sorted_chunks_;
    uint32_t offset_=0;
    void SortBlock(ChunkRef& chunk,std::vector<SortEntry> sorts);


    RuntimeProfile::Counter* sort_timer_ = nullptr;
};

