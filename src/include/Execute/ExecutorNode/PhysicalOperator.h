#pragma once
#include "Planer/LogicalOperator.h"
#include "Execute/Pipeline/Pipeline.h"
#include "Execute/core/Chunk.h"
#include "Execute/BitMap.h"
#include "Table/Tuple.h"
#include "Execute/core/ColumnFactory.h"
#include "Execute/core/ColumnVector.h"
#include "Execute/core/ColumnString.h"
#include "common/commonfunc.h"
class ExecuteContext;
class PhysicalOperator;
using PhysicalOperatorRef = std::shared_ptr<PhysicalOperator>;
class Pipeline;
using PipelineRef = Pipeline::PipelineRef;
static constexpr const uint32_t ONE_DATA_CHUNK_SIZE= 8;

#define THIS_NODE_SHOULDNT_BE(nodename)                 \
    std::string prefix= " this node shouldn't be ";     \
    std::string last = nodename;                        \
    throw Exception(prefix+last);


class PhysicalOperator{

private:
    // his plan node
    LogicalOperatorRef plan_;
    
    //i think needn't create another Physical Node version OperatorType
    static constexpr const OperatorType type_ 
        = OperatorType::PhysicalOperatorNode;
    
public:
    PhysicalOperator(LogicalOperatorRef plan,
    ExecuteContext* context,
    std::vector<PhysicalOperatorRef> children);
    
    void CleanUpSurplusColumn(ChunkRef chunk){
       auto output = plan_->GetOutPutSchema(); 
       std::vector<std::string> d_cols;
        for(auto& col : chunk->columns_){
            if(!output->exist(Column(col->name_,ColumnType::UNKOWN))){
                d_cols.push_back(col->name_);
            }
        }
        for(auto& n : d_cols){
            DEBUG(std::format("{} erase column {}",getOperatorName(GetType()),n));
            chunk->eraseColumn(n);
        }
    }

    virtual ~PhysicalOperator(){}
    virtual bool IsSink()=0;
    
    virtual void 
    BuildPipeline(PipelineRef current,ExecuteContext* context);

    virtual OperatorType GetType() = 0;

    virtual void sink_init(){

    }
    virtual void sink_uninit(){

    }
    virtual void exceute_init(){

    }
    virtual void execute_uninit(){

    }
    virtual void source_init(){
        // do nothing 
    }
    virtual void source_uninit(){
        //do nothing 
    }
    // this function called by source node
    virtual SourceResult Source(ChunkRef& chunk);
    //this function called by sink node.
    virtual SinkResult Sink(ChunkRef& chunk);
    //this function called by operaotrs
    virtual OperatorResult Execute(ChunkRef& chunk);


    virtual inline LogicalOperatorRef
    GetPlan(){
        return plan_;
    }

    ExecuteContext* context_;
    RuntimeProfile* profile_ = nullptr;
    std::vector<PhysicalOperatorRef> children_;
};