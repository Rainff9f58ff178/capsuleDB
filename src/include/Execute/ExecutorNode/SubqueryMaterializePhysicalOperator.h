#pragma once
#include "Execute/ExecutorNode/MaterializePhysicalOperator.h"



// this node execute expression in select_list and change it's name.
class SubqueryMaterializePhysicalOperator:public MaterializePhysicalOperator{
    static constexpr const OperatorType type_ = 
        OperatorType::SubqueryMaterializeOperatorNode;
public:
    SubqueryMaterializePhysicalOperator(LogicalOperatorRef plan,
        ExecuteContext* context,
        std::vector<PhysicalOperatorRef> children):
            MaterializePhysicalOperator(std::move(plan),context,std::move(children)){

    }

    bool IsSink() override{
        return false;
    }
    OperatorType GetType() override{
        return type_;
    }


    SourceResult Source(ChunkRef &chunk) override;
    SinkResult Sink(ChunkRef &chunk) override;
    OperatorResult Execute(ChunkRef &chunk) override;
};






