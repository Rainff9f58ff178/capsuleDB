#pragma once
#include <memory>
#include "CataLog/Schema.h"
#include "common/Exception.h"
class LogicalOperator;
using LogicalOperatorRef = std::shared_ptr<LogicalOperator>;
enum OperatorType{
    LogicalOperatorNode=0,
    PhysicalOperatorNode,
    MaterilizeOperatorNode,
    HashJoinOperatorNode,
    FilterOperatorNode,
    InsertOperatorNode,
    ValuesOperatorNode,
    AggOperatorNode,
    LimitOperatorNode,
    SortOperatorNode,
    SeqScanOperatorNode,
    ResultOperatorNode
};
#define COPY_PLAN_WITH_CHILDREN(cname)                      \
virtual LogicalOperatorRef                                  \
CopyWithChildren(std::vector<LogicalOperatorRef> children)  \
override{                                                   \
    auto plan = cname(*this);                               \
    plan.children_=children;                                \
    return std::make_shared<cname>(std::move(plan));        \
}              


class LogicalOperator{
    static constexpr const OperatorType type_ = OperatorType::LogicalOperatorNode;
public:
    LogicalOperator(std::vector<LogicalOperatorRef> children);
    virtual ~LogicalOperator()=default;

    inline LogicalOperatorRef GetChild(uint32_t idx){
        if(idx>= children_.size())
            throw Exception("LogicalOperatorRef GetChld Out Of bound");
        return children_[idx];
    }
    template<class TARGET>
    TARGET& Cast(){
        return dynamic_cast<TARGET&>(*this);
    }

    //this function return the select_lsit schema/
    inline SchemaRef  GetOutPutSchema(){
        return ouput_schema_;
    }
    inline SchemaRef GetInputSchema(){
        return input_schema_;
    }
    virtual OperatorType GetType(){
        throw Exception("Unreachable");
    }
    virtual LogicalOperatorRef 
    CopyWithChildren(std::vector<LogicalOperatorRef> children)=0;
    
    virtual void PrintDebug(){
        if(!show_info)
            return;;

        if(ouput_schema_){
            std::cout<<" | output_schema: ";
            for(auto& col :ouput_schema_->columns_){
                std::cout<<" "<<col.name_<<" ";
            }
        }
        std::cout<<" | intput_schema: ";
        for(auto& col:input_schema_->columns_){
            std::cout<<" "<<col.name_<<" ";
        }
    }

    virtual void SetOutputSchema(SchemaRef schema){
        ouput_schema_ = std::move(schema);
    }
    virtual void SetInputSchema(SchemaRef schema){
        input_schema_ = std::move(schema);
    }
    SchemaRef ouput_schema_;
    //every node has table_schame .
    SchemaRef input_schema_{nullptr};
    std::vector<LogicalOperatorRef> children_;

};
