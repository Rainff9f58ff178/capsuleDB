#pragma once 


#include "Execute/ExecutorNode/PhysicalOperator.h"
class InsertPhysicalOperator:public PhysicalOperator{

    static constexpr const OperatorType type_ =
        OperatorType::InsertOperatorNode;
    
    
    int32_t insert_nums_{0};
public:



    InsertPhysicalOperator(LogicalOperatorRef plan,
        ExecuteContext* context,PhysicalOperatorRef child);


    virtual bool IsSink() override{
        return true;
    }

    virtual SinkResult Sink(ChunkRef& bits) override;
    virtual SourceResult Source(ChunkRef& bits) override;
    virtual OperatorResult Execute(ChunkRef& bits) override;


    virtual OperatorType GetType() override{
        return type_;
    }
    /*
        this node should has only one child. insert values has 
        ValuesPhysicaloperator child ,insert select has Materilize
        Node as child.
    */
private:
    uint32_t num_insert_{0};
    bool sourced_{0}; // just push once block
};